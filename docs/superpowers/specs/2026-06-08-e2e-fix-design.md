# Fix Design: P0 Tickfile Missing Minutes + P1 Order Late Cap

> **Date**: 2026-06-08 (v5 — 五轮审阅修订，Final)
> **Status**: ✅ **IMPLEMENTED** (2026-06-09, Phase 19, 375 tests passed, 3-agent review all passed)
> **Scope**: P0 (Tickfile 缺失 24 分钟) + P1 (Order 丢失 13.4%)
> **Strategy**: P0 定向修补（5 处）+ P1 调参（2 处）+ 完整性校验（3 处）
> **Parent Bug Report**: `docs/superpowers/specs/2026-06-08-e2e-live-bugs.md`
> **Review History**: v1 R1(5C+7M) → v2 R2 → v3 R3(3C+7M) → v4 R4(1C+4M) → v5 本版本修复
> **Implementation**: Subagent-Driven Development, 10 tasks, 9 Implementation Notes 全验证通过

---

## P0: Tickfile 缺失 24 分钟 — 竞态修补

### 根因定位

`_tickfile_pending` 字典有 3 条 pop 路径，来自 2 条不同线程：

| 路径 | 位置 | 线程 | 说明 |
|------|------|------|------|
| ① | `flusher.py:541` `_try_generate_tickfile()` | tickfile-writer | 正常生成后 pop |
| ② | `flusher.py:711` `_reroute_buffer_to_late_queue()` | clock-thread | **re-route 时 pop，但 tickfile 从未生成** |
| ③ | `flusher.py:253` `_step1_cross_day_check()` | clock-thread | 跨日清空（正常） |

**路径②是主因**: 当 snapshot re-routing race window 触发时，`_reroute_buffer_to_late_queue` 将已 flush 分钟的 buffer 数据 re-route 到 late queue。在此过程中，它**无条件 pop 了 `_tickfile_pending[k]`**（line 711），导致：

1. snapshot 分钟 N 被 flush → `_tickfile_pending["N"]` 被设置
2. race window 检测到分钟 N 又有新数据到达 → `_reroute_buffer_to_late_queue(["N"])`
3. `_tickfile_pending.pop("N")` → tickfile 数据被清除
4. order thread watermark 推进到 N → `_drain_tickfile_triggers` 尝试 enqueue N
5. tickfile writer dequeue N → `_try_generate_tickfile("N")` → `pending is None` → **静默跳过**

**加剧因素**: `_try_generate_tickfile` 在 `pending=None` 时无任何日志（line 542-543），导致问题极难追踪。

**`_tickfile_pending` 完整生命周期图**:

```
写入路径:
  flusher.py:421  _flush_minutes_internal()    → 设置 pending[k] = TickfilePendingData
  flusher.py:582  _try_generate_tickfile()     → IO error 时 re-insert pending[k]

消费路径:
  flusher.py:541  _try_generate_tickfile()     → pop(k) 正常消费

错误 pop（Fix-A 修复目标）:
  flusher.py:711  _reroute_buffer_to_late_queue()  → pop(k) 删除但未生成 ← BUG

清空路径:
  flusher.py:253  _step1_cross_day_check()     → .clear() 跨日清空（先生成再清空）

读取路径:
  flusher.py:114,219,474,485  keys() / 成员检查
  engine.py:671,799           membership / iteration
```

**所有访问均在 `self._state.lock` 保护下**（RLock），无未加锁访问。

### 修复方案

#### Fix-A: re-route 不再丢弃 tickfile pending（核心修复）

**文件**: `src/minute_bar/flusher.py:709-716`

**改动**: `_reroute_buffer_to_late_queue` 中，保留 `_tickfile_pending` 不 pop。Re-route 只应清理 ohlcv/raw buffer，tickfile 的 snapshot_copy 数据不受 re-route 影响。

```
Before:
  if self._enable_tickfile:
      order_buf = self._state.raw_order_buffers.pop(k, None)
      pending_tick = self._state._tickfile_pending.pop(k, None)  ← 删除 tickfile 数据
      if pending_tick:
          logger.debug("Reroute: cleared tickfile pending for minute=%s", k)

After:
  if self._enable_tickfile:
      order_buf = self._state.raw_order_buffers.pop(k, None)
      # DO NOT pop _tickfile_pending — tickfile generation must proceed
      # even when snapshot data is re-routed to late queue.
      # See: INV-TF1 — only _try_generate_tickfile and _step1_cross_day_check
      # may remove entries from _tickfile_pending.
```

**需同步更新的现有测试**（2 个）:

| 测试文件 | 旧类名 → 新类名 | 修改内容 |
|----------|-----------------|----------|
| `tests/test_tickfile_sync.py:426` | `TestReroutePopsRawOrderBuffersAndPending` → `TestReroutePreservesTickfilePending` | 断言改为 `assert mk in state._tickfile_pending` |
| `tests/test_tickfile_bg_writer.py:778` | `TestReroutePopsTickfilePending` → `TestReroutePreservesTickfilePending` | 断言改为 `assert "202606020900" in state._tickfile_pending` |

#### Fix-A2: overflow direct-IO 路径（无需代码修改）

**背景**: 时钟线程的 overflow 路径（flusher.py:131-153）在 `_tickfile_queue` 满且 writer 线程已死亡时，绕过 queue 直接调用 `_try_generate_tickfile(mk)`（line 150）。该方法内含 `pop()` + `None` guard，天然安全。

**额外安全考虑（v4 新增）**: overflow 路径对 `force_keys[0]` 既 enqueue（line 133）又 direct-IO（line 150）。如果 direct-IO 成功，queue 中的 entry 后续 drain 时得到 `pending=None` 安全跳过。如果 direct-IO 失败并 re-insert pending，queue entry + re-inserted pending 共存，drain 处理 queue entry 时会成功生成，然后 `flush_all_remaining` 处理 re-inserted entry 时……**Fix-F 的 `_generated_tickfile_minutes` set 会阻止重复写入**（见 Fix-F）。

**结论**: 无需对 overflow 路径本身做代码修改。Fix-F 提供了兜底保护。

#### Fix-B: `_try_generate_tickfile` 添加 per-minute-key 日志（v4 修订，解决 C3）

**文件**: `src/minute_bar/flusher.py:541-543`

**改动**: 使用 **per-minute-key** 频率限制，而非全局计数器。每个 minute_key 首次 skip 输出 WARNING，后续降级为 DEBUG。使用 `_skip_warned_keys: set` 追踪已警告的 key。

```python
# flusher class 新增:
_tickfile_skip_warned_keys: set = set()  # per-minute-key WARNING 跟踪

# line 541-543 替换:
pending = self._state._tickfile_pending.pop(minute_key, None)
if pending is None:
    # Diagnostic counter: approximate, GIL-dependent. Acceptable for monitoring.
    self._engine_ref._tickfile_queue_skip_count += 1
    if minute_key not in self._tickfile_skip_warned_keys:
        self._tickfile_skip_warned_keys.add(minute_key)
        logger.warning(
            "Tickfile skipped: no pending data for minute=%s [thread=%s, queue_depth=%d]",
            minute_key,
            threading.current_thread().name,
            self._tickfile_queue.qsize() if self._tickfile_queue else 0,
        )
    else:
        logger.debug(
            "Tickfile skipped (already warned): minute=%s",
            minute_key,
        )
    return
```

**变更理由（v4 vs v3）**:
- v3 使用全局 `_tickfile_queue_skip_count <= 5`，前 5 次 WARNING 跨所有 minute_key 消耗
- v4 改为 per-minute-key `_skip_warned_keys` set，**每个缺失分钟都至少得到一次 WARNING**
- 全局 skip 计数器 `_tickfile_queue_skip_count` 保留用于 shutdown summary

#### Fix-C: engine shutdown tickfile 完整性校验（v4 增强，解决 C2+M1）

**文件**: `src/minute_bar/engine.py` — `stop()` 方法

**改动**: 三层校验 + `_generated_tickfile_minutes` 内存比对（Layer 3 替代方案）。

**执行顺序**: writer join → `_tickfile_writer_drain()` → 快照 pending 数量 → `flush_all_remaining()` → Layer 1/2/3 检查

```python
# In Engine.stop(), 执行顺序:
# 1. Join writer thread (line ~362)
# 2. _tickfile_writer_drain() (line ~368)
# 3. Snapshot pending count for later comparison
# 4. flush_all_remaining() (line ~372)
# 5. Fix-C checks below

if self._config.output.enable_tickfile:
    # Note: pre_flush_pending_count is informational context only.
    # flush_all_remaining may ADD new pending entries via _flush_minutes_internal
    # for remaining ohlcv buffers. The count is not an exact "how many consumed" measure.
    # After flush_all_remaining, pending should be empty unless IO errors caused re-inserts.
    with self._state.lock:
        pre_flush_pending_count = len(self._state._tickfile_pending)

    # --- flush_all_remaining already called above ---

    # Layer 1: pending in-memory check (after flush_all_remaining)
    # Detects: flush_all_remaining failures (IO errors during EOF fallback)
    # Note: flush_all_remaining(skip_tickfile=True) path at flusher.py:485 reads
    # _tickfile_pending without lock (zombie-writer scenario only). This is an
    # acceptable diagnostic inaccuracy — the critical flush_all_remaining(normal)
    # path runs after writer join so no concurrent access occurs.
    with self._state.lock:
        pending = set(self._state._tickfile_pending.keys())
    if pending:
        logger.warning(
            "Shutdown CHECK 1 FAIL: %d tickfile minutes still pending after flush_all_remaining "
            "(info: %d before flush): %s",
            len(pending), pre_flush_pending_count, sorted(pending)[:20],
        )

    # Layer 2: Queue residual check (non-consuming, just report)
    qdepth = self._tickfile_queue.qsize() if self._tickfile_queue else 0
    if qdepth > 0:
        logger.warning(
            "Shutdown CHECK 2 FAIL: %d minute keys still in tickfile queue after drain",
            qdepth,
        )

    # Layer 3: In-memory generated set vs flushed snapshot minutes
    # Uses _generated_tickfile_minutes (populated by _try_generate_tickfile)
    # instead of filesystem scan — avoids 400MB+ CSV read on every shutdown.
    # This ALWAYS runs (no conditional), ensuring completeness check is never skipped.
    #
    # IMPORTANT: Only compare minutes that have BOTH snapshot AND order data.
    # Pre/post-market minutes (e.g., 0800, 0830) may have snapshot but no orders,
    # and select_tickfile_records returns empty for them (Implementation Note #6
    # marks them as "generated"). Comparing unconditionally would produce
    # false-positive MISSING warnings on every shutdown.
    missing = []
    with self._state.lock:
        generated = set(self._state._generated_tickfile_minutes)
    flushed_snaps = set(self._state.flushed_snapshot_minutes)
    # flushed_order_minutes MUST exist on SharedState (Implementation Plan step 3 + 4b).
    # Assert rather than silent fallback — ensures implementation steps are completed.
    assert hasattr(self._state, 'flushed_order_minutes'), (
        "SharedState.flushed_order_minutes required for Fix-C Layer 3 "
        "(see Implementation Plan step 3 and step 4b)"
    )
    flushed_orders = set(self._state.flushed_order_minutes)
    comparison_mins = flushed_snaps & flushed_orders  # minutes with BOTH snapshot AND order
    if comparison_mins:
        missing = sorted(comparison_mins - generated)
        extra = sorted(generated - comparison_mins)
        if missing:
            logger.warning(
                "Shutdown CHECK 3 FAIL: %d tickfile minutes MISSING (have snapshot, no tickfile): %s",
                len(missing), missing[:30],
            )
        if extra:
            logger.warning(
                "Shutdown CHECK 3 WARN: %d tickfile minutes EXTRA (no snapshot): %s",
                len(extra), extra[:10],
            )
        if not missing and not extra:
            logger.info(
                "Shutdown CHECK 3 PASS: tickfile complete (%d minutes match snapshot&order intersection)",
                len(comparison_mins),
            )

    # Summary line
    logger.info(
        "Tickfile shutdown summary: enqueue=%d, dequeue=%d, skip=%d, generated=%d, missing=%d",
        getattr(self, '_tickfile_enqueue_count', 0),
        getattr(self, '_tickfile_dequeue_count', 0),
        getattr(self, '_tickfile_queue_skip_count', 0),
        len(generated),
        len(missing),
    )
```

**v4 vs v3 变更**:
- Layer 3 从"条件执行文件系统扫描"改为"**始终执行内存比对**"
- 使用 `_generated_tickfile_minutes` set（由 `_try_generate_tickfile` 在成功生成后填入）替代 400MB CSV 读取
- **始终运行**（无 conditional），确保不会跳过任何场景
- 依赖 `_state.flushed_snapshot_minutes`（需确认此 set 存在；如不存在，回退到文件系统扫描作为备选）

#### Fix-F: 防止 tickfile 重复写入（v4 新增，解决 C1+M2+M4）

**背景**: `write_tickfile_rows` 使用 append 模式（`open(path, "a")`）。如果 `_try_generate_tickfile` 在部分写入后失败，re-insert pending 数据，然后重试成功，**同一分钟的行会被追加两次**。同样，overflow 路径对同一 key 的 enqueue + direct-IO 在错误重试时也可能导致重复。

**文件**: `src/minute_bar/aggregator.py` + `src/minute_bar/flusher.py`

**改动**:

1. `aggregator.py` — `SharedState` 添加 `_generated_tickfile_minutes: set`

```python
# In SharedState (aggregator.py), 添加:
_generated_tickfile_minutes: set  # 已成功生成 tickfile 的分钟集合

# 初始化 (在 __init__ 或 __post_init__):
self._generated_tickfile_minutes = set()
```

2. `flusher.py` — `_try_generate_tickfile` 添加去重 guard

**⚠️ 关键：`.add()` 必须在 write 成功之后，不可在 write 之前。详见实现注意事项 #1。**

```python
# _try_generate_tickfile 中的 Fix-F 改动
# ⚠️ 关键：.add() 必须在 write 成功之后，不可在 write 之前。
# 以下代码与实现注意事项 #1 完全一致，以实现注意事项为准。

# 步骤 1: 去重检查 + pending pop + seqno（同一 lock block）
with self._state.lock:
    if minute_key in self._state._generated_tickfile_minutes:
        logger.debug("Tickfile dedup: minute=%s already generated, skipping", minute_key)
        return
    pending = self._state._tickfile_pending.pop(minute_key, None)
    if pending is None:
        # ... skip logging (Fix-B) ...
        return
    # NOTE: 不在此处 .add()——必须在 write 成功后（步骤 3）
    order_records = self._state.raw_order_buffers.pop(minute_key, [])
    self._state._tickfile_seqno += 1
    current_seqno = self._state._tickfile_seqno

# 步骤 2: select + write（在 lock 外）
selected = select_tickfile_records(...)
if not selected:
    # 实现注意事项 #6: 数据已 pop 但无记录可选，标记为已处理
    with self._state.lock:
        self._state._generated_tickfile_minutes.add(minute_key)
    return

write_tickfile_rows(...)

# 步骤 3: 成功后标记（write 成功，不可省略）
with self._state.lock:
    self._state._generated_tickfile_minutes.add(minute_key)

# 步骤 4: IO error handler（write 失败，key 不在 set 中，允许 re-insert）:
except Exception:
    with self._state.lock:
        # key 不在 _generated_tickfile_minutes（因为步骤 3 未执行），可以安全 re-insert
        self._state._tickfile_pending[minute_key] = pending
        logger.warning(
            "Tickfile IO error for minute=%s, re-inserted for retry",
            minute_key,
        )
```

3. `_step1_cross_day_check` — 跨日清空时同步清理

```python
# In _step1_cross_day_check, after generating all pending:
# Log any minutes that failed to generate
with self._state.lock:
    failed = set(self._state._tickfile_pending.keys())
    if failed:
        logger.critical(
            "Cross-day: %d tickfile minutes FAILED to generate, data lost: %s",
            len(failed), sorted(failed)[:20],
        )
    self._state._tickfile_pending.clear()
    self._state._generated_tickfile_minutes.clear()  # 跨日重置
```

**风险评估**:
- `_generated_tickfile_minutes` 在 `SharedState` 中，受 `_state.lock` 保护
- 跨日清空时同步清理，避免 set 无限增长
- 内存开销极小：一天 ~330 分钟 × 每条 ~20 bytes string ≈ 7KB

#### Fix-G: cross-day force-generation 失败日志增强（v4 新增，解决 M3）

**文件**: `src/minute_bar/flusher.py` — `_step1_cross_day_check`

**改动**: 在 force-generation 循环后、`.clear()` 前，检查并记录失败条目。

```python
# After the force-generation loop (flusher.py ~line 230):
with self._state.lock:
    remaining = set(self._state._tickfile_pending.keys())
if remaining:
    logger.critical(
        "Cross-day CHECK: %d tickfile minutes FAILED to generate (data will be lost on clear): %s",
        len(remaining), sorted(remaining)[:20],
    )
# Then proceed with existing clear()
```

---

## P1: Order Late Cap 截断 — 调参

### 根因

`MAX_LATE_ORDER_RECORDS_PER_MINUTE = 50000`（`engine.py:42`，硬编码）。

源 order 数据按 rcvtime 排序但 engine 用 time 列做分钟归属，导致大量交错。交易时段每分钟有 100K-750K 条 late record，远超 50000 上限。

### 架构根因分析（P1 长期问题）

**短期修复**: 提高默认 cap 至 1,000,000（Fix-D），确保 100x E2E 测试通过。

**长期根因**: Order 线程基于文件顺序推进分钟（顺序读取），而记录按 `time` 列归属分钟。这种架构不匹配导致交错记录始终被标记为 "late"。**正确的长期修复**是让 order 线程使用 watermark-based flush（与 snapshot 线程一致），从根本上消除 late record 概念。

**分期计划**:
- **本轮 (Phase 19)**: Fix-D 调参 + Fix-E 监控，确保 E2E 测试数据完整
- **下轮 (Phase 20+)**: Order 线程 watermark-based flush 架构改造

### 修复方案

#### Fix-D: `MAX_LATE_ORDER_RECORDS_PER_MINUTE` 改为可配置 + 提高默认值

**文件**: `src/minute_bar/config.py` + `src/minute_bar/engine.py` + config ini（共 7 个: `production.ini`, `test-tickfile-live.ini`, `test-watermark.ini`, `test-order-live.ini`, `test-order-replay.ini`, `test-live.ini`, `config.ini.example`）

**Config 文件更新策略**:
- `production.ini`: **必须添加**（生产默认值）
- `config.ini.example`: **必须添加**（文档参考）
- 其余 5 个测试 config: 可选（使用代码默认值 1,000,000），但在 `[recovery]` section 添加注释说明此配置项存在

**改动**:

```
# config.py — RecoveryConfig 添加:
# Maximum late order records per minute before discarding.
# Applies to ALL modes (live, replay), not just tickfile mode.
# WARNING: Each record uses ~300-400 bytes of Python memory (frozen dataclass overhead).
# At 1,000,000 records, peak memory per minute is ~400 MB.
# If multiple minutes accumulate late records simultaneously, total could reach 2-3 GB.
# Production real-time: late records are minimal (<1000/min); this cap is a safety valve.
# 100x speed test: busiest minute (0900) has ~750K records; 1M gives 33% headroom.
max_late_order_records_per_minute: int = 1000000

# config.py — load_config() parser 添加:
# In the [recovery] section parser:
cfg.recovery.max_late_order_records_per_minute = s.getint(
    "max_late_order_records_per_minute",
    cfg.recovery.max_late_order_records_per_minute
)

# engine.py — 删除:
MAX_LATE_ORDER_RECORDS_PER_MINUTE = 50000  # line 42

# engine.py — _order_loop 使用:
self._max_late_order_records = config.recovery.max_late_order_records_per_minute

# engine.py — line 623 替换:
if count >= self._max_late_order_records:
```

**config ini 注释**（添加到 `[recovery]` section，适用于 `production.ini` 和 `config.ini.example`）:

```ini
[recovery]
# Maximum late order records per minute before discarding.
# Applies to ALL modes (live, replay, tickfile enabled or disabled).
# Default: 1000000 (1M). Each record uses ~300-400 bytes of Python memory.
# At 1M records this is ~400 MB per minute. Multiple minutes accumulating
# simultaneously could reach 2-3 GB total. Monitor memory if adjusting.
# Production: rarely exceeds 1000 late records per minute.
# 100x test: busiest minute has ~750K records.
max_late_order_records_per_minute = 1000000
```

#### Fix-E: order late discard 添加统计日志

**文件**: `src/minute_bar/engine.py:621-625`

**改动**: 在 cap 触发时累计丢弃计数。统计在 drain_count >= 100 时和 **loop 退出时** 均输出。增加 engine shutdown 汇总。

```python
# _order_loop 添加计数器:
late_dropped_per_minute: Dict[str, int] = {}
_total_late_dropped: int = 0  # 累计丢弃总数

# line 623 替换:
if count >= self._max_late_order_records:
    late_dropped_per_minute[minute_key] = late_dropped_per_minute.get(minute_key, 0) + 1
    _total_late_dropped += 1
    continue

# 在 drain_count >= 100 时输出统计:
if late_dropped_per_minute:
    total_dropped = sum(late_dropped_per_minute.values())
    if total_dropped > 0:
        logger.warning(
            "Order late cap: dropped %d records across %d minutes (cap=%d)",
            total_dropped, len(late_dropped_per_minute), self._max_late_order_records,
        )
    late_dropped_per_minute.clear()

# === loop 退出后最终统计 ===
# 在 _order_loop 的 while loop 退出后:
if late_dropped_per_minute:
    total_dropped = sum(late_dropped_per_minute.values())
    if total_dropped > 0:
        logger.warning(
            "Order late cap FINAL: dropped %d records across %d minutes in final batch (cap=%d)",
            total_dropped, len(late_dropped_per_minute), self._max_late_order_records,
        )
    late_dropped_per_minute.clear()

# Store cumulative total for shutdown summary:
# Thread safety: safe because stop() joins order_thread before reading this field
self._total_late_order_dropped = _total_late_dropped
```

**Engine shutdown 汇总**（添加到 `stop()` 方法）:

```python
if hasattr(self, '_total_late_order_dropped') and self._total_late_order_dropped > 0:
    logger.warning(
        "Order late cap summary: %d total records dropped due to late cap (cap=%d)",
        self._total_late_order_dropped,
        self._max_late_order_records,
    )
```

---

## 修复影响矩阵

| Fix | 文件 | 改动行数 | 风险 | 测试方式 |
|-----|------|----------|------|----------|
| Fix-A | flusher.py | ~6 行 | 低（删除 pop） | 更新 2 个现有测试 + 新增 1 个回归测试 |
| Fix-A2 | flusher.py | 0 行 | 无 | 由 Fix-F 覆盖 |
| Fix-B | flusher.py | ~15 行 | 极低（日志） | 观察日志 + 单元测试 |
| Fix-C | engine.py | ~50 行 | 低（shutdown 校验） | 观察 shutdown 日志 |
| Fix-D | config.py + engine.py + ini | ~20 行 | 低（调参） | E2E 验证 order 完整性 |
| Fix-E | engine.py | ~20 行 | 极低（日志） | 观察日志 |
| Fix-F | aggregator.py + flusher.py | ~20 行 | 低（去重 guard） | 单元测试覆盖 |
| Fix-G | flusher.py | ~8 行 | 极低（日志） | 观察 cross-day 日志 |

**总计**: ~139 行改动，4 个源文件 + 7 个 config ini

---

## `_tickfile_pending` 生命周期管理规范

### Invariant

> **INV-TF1**: `_tickfile_pending[k]` 的唯一合法移除路径:
> 1. `_try_generate_tickfile()` → `pop(k)` — 正常消费（含 overflow direct-IO 路径）
> 2. `_step1_cross_day_check()` → `.clear()` — 跨日清空（先 force-generate 所有 pending）
>
> **禁止路径**: `_reroute_buffer_to_late_queue()` 不得 pop `_tickfile_pending`

> **INV-TF2**: `_generated_tickfile_minutes` 去重保护:
> - `_try_generate_tickfile` 在写文件前检查 `minute_key not in _generated_tickfile_minutes`
> - 成功写入后添加到 `_generated_tickfile_minutes`
> - IO 错误 re-insert 时，如果已在 `_generated_tickfile_minutes` 中，跳过 re-insert（防止部分写入后重复）
> - 跨日时 `_generated_tickfile_minutes.clear()`

> **INV-TF3**: `_tickfile_pending` 内存上界:
> - 一天最多 ~330 个交易分钟，每个 entry 持有 snapshot_copy（~4505 symbols）+ raw_records
> - 最大 pending 数受 `MAX_TICKFILE_PENDING_MINUTES = 10` 溢出保护限制
> - 跨日 cleanup 是最终释放机制
> - `aggregator.py:91` 的 `_tickfile_pending` 声明处需注释说明此上界

### Double-Enqueue 说明

以下场景会导致同一 minute_key 被 enqueue 两次（**这是正常的、安全的**）:

1. Clock thread `_flush_minutes_internal` → `_enqueue_tickfile(N)`
2. Order thread `_drain_tickfile_triggers` → `_enqueue_tickfile(N)`（catch-up）

第二次 dequeue 时 `pop()` 返回 None，`_try_generate_tickfile` 记录 skip 并返回。Fix-F 的 `_generated_tickfile_minutes` guard 提供额外保护。

---

## 测试计划

### 需更新的现有测试（2 个）

| 文件 | 旧类名 → 新类名 | 修改内容 |
|------|-----------------|----------|
| `tests/test_tickfile_sync.py:426` | → `TestReroutePreservesTickfilePending` | 断言改为 `assert mk in state._tickfile_pending` |
| `tests/test_tickfile_bg_writer.py:778` | → `TestReroutePreservesTickfilePending` | 断言改为 `assert "202606020900" in state._tickfile_pending` |

### 新增单元测试

1. **`TestReroutePreservesTickfilePendingThenGenerates`**（核心回归测试）
   - **Fixture**: 设置 `_tickfile_pending[k]` 含至少一个有效 symbol 的 snapshot data + raw_records（参考 `test_pending_cleanup_after_generation` 的 fixture 设置，test_tickfile_sync.py:224-238）
   - 调用 `_reroute_buffer_to_late_queue([k])`
   - 断言 `k in state._tickfile_pending`
   - 调用 `_try_generate_tickfile(k)`
   - 断言 tickfile 文件成功生成

2. **`TestGeneratedTickfileMinutesPreventsDuplicateWrite`**（Fix-F 覆盖）
   - 设置 `_tickfile_pending[k]` 含有效 snapshot data
   - 调用 `_try_generate_tickfile(k)` → 成功生成
   - 再次调用 `_try_generate_tickfile(k)` → 应被 `_generated_tickfile_minutes` guard 拦截
   - 断言 tickfile CSV 行数未增加

3. **`TestOverflowDirectIOThenDrainSafe`**（Fix-A2 + Fix-F 覆盖）
   - Mock `_tickfile_writer_alive=False` + writer thread not alive
   - 设置 `_tickfile_pending[k]`
   - 模拟 overflow 路径: 先 enqueue k，再直接调用 `_try_generate_tickfile(k)`
   - 断言 tickfile 成功生成
   - 模拟 drain: 再次调用 `_try_generate_tickfile(k)` → 被 guard 拦截

4. **`TestLateOrderCapDropLoggingFinalBatch`**（Fix-E 覆盖）
   - 模拟 95 条 late record（< 100 threshold）
   - 断言 loop 退出时仍输出 drop 统计

5. **`TestLateOrderCapConfigReadFromIni`**（Fix-D 覆盖）
   - 从 config ini 读取 `max_late_order_records_per_minute`
   - 断言 `_order_loop` 使用配置值

6. **`TestRerouteDoesNotMutatePendingData`**（Fix-A 数据完整性）
   - 设置 `_tickfile_pending[k]` 含完整 snapshot_copy（有效 symbol data）
   - 调用 `_reroute_buffer_to_late_queue([k])`
   - 生成 tickfile
   - 断言 tickfile 内容与 reroute 前一致
   - **额外断言**: 文档化 tickfile snapshot != 最终 snapshot 文件（当 late records 存在时）

7. **`TestCrossDayForceGenerationLogsFailures`**（Fix-G 覆盖）
   - 设置多个 pending entries，mock `_try_generate_tickfile` 对其中一个失败
   - 调用 `_step1_cross_day_check`
   - 断言 CRITICAL 日志输出失败分钟

8. **`TestGoldenPathTickfileUnchangedByFix`**（回归测试，解决 M4/M7）
   - 设置正常分钟（snapshot + order data）
   - 正常 flush（不触发 reroute）
   - 生成 tickfile
   - 捕获 tickfile 行数和列数作为 baseline

9. **`TestShutdownTickfileCompletenessCheck`**（Fix-C 覆盖）
   - 使用 mock engine object（参考 test_tickfile_bg_writer.py 的 fixture 模式）
   - 设置部分 pending 未生成
   - 验证 shutdown 检查输出正确日志

### E2E 自动化验证

将验证脚本固化为 `tests/test_e2e_tickfile_completeness.py`:

```python
"""E2E 自动化验证: tickfile 完整性 + order 完整性"""
import os, csv, pytest
from pathlib import Path

@pytest.mark.e2e
def test_tickfile_minutes_match_order_and_snapshot():
    """P0 验证: tickfile 分钟数 == 有 order + snapshot 的分钟数

    只比较有 order AND snapshot 数据的分钟（排除盘前/盘后仅有 snapshot 的分钟）。
    Bug report: 329 minutes with order + snapshot data.
    """
    output_dir = Path(os.environ.get("E2E_OUTPUT_DIR", "test/tickfile_live_output"))
    date_str = os.environ.get("E2E_DATE", "20260528")
    year = date_str[:4]

    snap_dir = output_dir / "snapshot" / year / date_str
    tf_path = output_dir / "tickfile" / year / date_str / f"tickfile_{date_str}.csv"
    order_dir = output_dir / "order" / year / date_str

    assert snap_dir.exists(), f"Snapshot directory not found: {snap_dir}"
    assert tf_path.exists(), f"Tickfile not found: {tf_path}"

    snap_mins = {f.stem.split('_')[-1] for f in snap_dir.glob("*.csv")}

    # Only count order minutes that exist (intersection with snapshot)
    order_mins = set()
    if order_dir.exists():
        order_mins = {f.stem.split('_')[-1] for f in order_dir.glob("*.csv")}
    comparison_mins = snap_mins & order_mins  # minutes with BOTH order and snapshot

    tf_mins = set()
    with open(tf_path, encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        time_idx = header.index("UpdateTime")
        for row in reader:
            if len(row) > time_idx:
                tf_mins.add(row[time_idx].split(' ')[1].replace(':', '')[:4])

    missing = sorted(comparison_mins - tf_mins)
    assert not missing, (
        f"Missing {len(missing)} tickfile minutes "
        f"(snapshot={len(snap_mins)}, order={len(order_mins)}, "
        f"comparison={len(comparison_mins)}, tickfile={len(tf_mins)}): {missing}"
    )

@pytest.mark.e2e
def test_order_output_count_matches_source():
    """P1 验证: order 输出记录数 == 源记录数"""
    output_dir = Path(os.environ.get("E2E_OUTPUT_DIR", "test/tickfile_live_output"))
    source_count = int(os.environ.get("E2E_SOURCE_ORDER_COUNT", "87521294"))

    total_lines = 0
    for f in sorted((output_dir / "order").rglob("*.csv")):
        with open(f, encoding='utf-8') as fh:
            # Skip header, count data lines
            first_line = True
            for line in fh:
                if first_line:
                    first_line = False
                else:
                    total_lines += 1

    delta = abs(total_lines - source_count)
    assert delta == 0, f"Order count mismatch: output={total_lines}, source={source_count}, delta={delta}"
```

---

## 验证计划

修复后重新运行 E2E live test，验证：

1. **P0 验证**: tickfile 分钟数 == 有 order + snapshot 的分钟数（329/329）
2. **P1 验证**: order 输出记录数 == 源记录数（87.5M/87.5M）
3. **回归验证**: 现有 pytest suite 全部通过（含更新的 2 个测试）
4. **新增测试**: 9 个新单元测试全部通过
5. **E2E 自动化**: `test_tickfile_minutes_match_order_and_snapshot` + `test_order_output_count_matches_source` 通过
6. **Fix-F 验证**: `_generated_tickfile_minutes` 去重保护正常工作

```bash
# E2E 测试命令
cd D:/FIU
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini
PYTHONPATH=src python -m data_simulator --source-dir input --output-dir test/output --speed 100 --file-types order,snapshot,code --date 20260528

# 自动化 E2E 验证
E2E_OUTPUT_DIR=test/tickfile_live_output E2E_DATE=20260528 E2E_SOURCE_ORDER_COUNT=87521294 \
  python -m pytest tests/test_e2e_tickfile_completeness.py -v

# 单元测试回归
python -m pytest tests/ -v
```

---

## 本轮修复 vs 下轮计划

| 问题 | 本轮 (Phase 19) | 下轮 (Phase 20+) |
|------|-----------------|-------------------|
| P0 tickfile 缺失 24 分钟 | ✅ Fix-A + Fix-F + Fix-G 完整修复 | — |
| tickfile 重复行防护 | ✅ Fix-F `_generated_tickfile_minutes` | — |
| P1 order 丢失 13.4% | ✅ Fix-D 调参 + Fix-E 监控 | 🔧 Order 线程 watermark-based flush |
| tickfile snapshot 一致性 | ✅ 文档确立一致性边界 | 🔧 考虑 late records 触发 tickfile 更新 |
| P2 snapshot late queue 溢出 | ⏳ 本轮不修复 | 🔧 Phase 20 处理 |
| Replay vs Live 一致性 | ⏳ Replay 独立路径（本轮 fix 不影响） | 🔧 统一 tickfile 生成路径 |
| Runtime tickfile 缺失检测 | ✅ Fix-C shutdown 3-layer check | 🔧 Phase 20: runtime stale detection |

### 一致性边界声明

**Tickfile 内容 vs Snapshot 文件**:
- tickfile 中的 snapshot 数据来自原始 flush 时刻（不含 late records 的 snapshot 更新）
- snapshot 文件包含完整数据（原始 + late appended）
- 这是可接受的 trade-off: tickfile 的核心用途是 OHLCV + 成交明细，snapshot 部分略滞后不影响分析
- 如果下游需要完全一致的 snapshot，应直接读取 snapshot 文件

**Reroute 分钟的 tickfile order 数据缺失（已知限制）**:
- `_reroute_buffer_to_late_queue` 在 Fix-A 后保留 `_tickfile_pending`，但 **仍 pop `raw_order_buffers`**（flusher.py:710）
- 这意味着 reroute 分钟的 tickfile 将有完整的 snapshot 数据，但 **零 order records**
- Order 输出文件不受影响（order thread 的写入路径独立于 reroute）
- 这是可接受的 trade-off：不完整的 tickfile（有 snapshot 无 order）远优于完全缺失的 tickfile（P0 bug 修复前的状态）
- Fix-A 的核心目标是保证 tickfile **存在**，而非保证 tickfile 内容 100% 完整
- 此限制可通过在 `_reroute_buffer_to_late_queue` 中也保留 `raw_order_buffers` 来改善，但会增加内存占用（order records 保留两份），留作 Phase 20+ 考虑

**Order late cap drop**:
- 本轮（Phase 19）: cap 提高到 1M，E2E 测试不应触发 drop
- 如果 drop 触发，视为数据完整性事件，输出 WARNING + shutdown 汇总
- 下轮（Phase 20+）: watermark-based flush 从根本上消除 late record 概念

**生产默认值 vs 测试默认值**:
- `max_late_order_records_per_minute`: 统一默认 1,000,000
- 此配置影响所有模式（live, replay, tickfile enabled/disabled），不仅限于 tickfile
- 生产环境 late records 极少（< 1000/分钟），1M cap 远超需求
- 100x 测试 late records 可达 750K/分钟，1M 留 33% 余量

### Rollback 策略

- **Fix-A**: 恢复 pop 代码 + 恢复 2 个测试断言。Blast radius: tickfile minutes 再次丢失
- **Fix-D**: ini 中设置 `max_late_order_records_per_minute = 50000` 回退。Blast radius: order data loss 恢复
- **Fix-F**: 可独立保留（去重保护对任何模式都有益），或随 Fix-A 一起回退
- **Fix-B/C/E/G**: 均为日志/诊断，无数据影响，回退无风险
- 所有改动向后兼容（无 schema/file format 变更），回滚 = revert commit

---

## 实现注意事项（R2 审阅 3-Agent 确认）

> 以下 8 条由第二轮 3 个审阅 Agent 独立发现并一致确认，均为 implementation-level 细节。
> 设计意图正确，但 implementation plan 和编码时必须严格遵循。

### 1. Fix-F `_generated_tickfile_minutes.add()` 必须在 write 成功之后

**来源**: R2 Agent 1 C1 + R2 Agent 2 C1

**问题**: 如果 `.add()` 放在 `write_tickfile_rows()` 之前，部分写入后 IO 失败时：
- key 已在 `_generated_tickfile_minutes` 中 → re-insert 被跳过
- 部分数据在磁盘上 → 永久不完整
- `_tickfile_pending` 无此条目 → `flush_all_remaining` 无法重试

**正确做法**: guard check（`if key in set: return`）仍在 write 前，但 `.add(key)` 移到 write 成功后。

```python
# _try_generate_tickfile 正确顺序:
with self._state.lock:
    # ① 去重检查（write 前）
    if minute_key in self._state._generated_tickfile_minutes:
        logger.debug("Tickfile dedup: minute=%s already generated", minute_key)
        return
    # ② Pop pending（同一 lock block）
    pending = self._state._tickfile_pending.pop(minute_key, None)
    if pending is None:
        # ... skip logging ...
        return
    # ③ Pop order records
    order_records = self._state.raw_order_buffers.pop(minute_key, [])
    # ④ Increment seqno（guard 通过后）
    self._state._tickfile_seqno += 1
    current_seqno = self._state._tickfile_seqno

# ⑤ Select（lock 外）
selected = select_tickfile_records(...)
if not selected:
    logger.warning("Tickfile: no records selected for minute=%s", minute_key)
    with self._state.lock:
        self._state._generated_tickfile_minutes.add(minute_key)  # 数据已 pop，不可重试
    return

# ⑥ Write（lock 外）
write_tickfile_rows(...)

# ⑦ 成功后标记（lock 内）
with self._state.lock:
    self._state._generated_tickfile_minutes.add(minute_key)

# ⑧ IO error handler:
except Exception:
    with self._state.lock:
        if minute_key not in self._state._generated_tickfile_minutes:
            self._state._tickfile_pending[minute_key] = pending  # 可重试
        else:
            logger.warning(
                "Tickfile IO error: partial write for minute=%s, skipping re-insert "
                "(partial data may exist on disk)",
                minute_key,
            )
```

### 2. Fix-F guard 必须与 pending pop 在同一 lock block，在 seqno increment 之前

**来源**: R2 Agent 2 C1

**理由**: 如果 guard 是独立的 lock acquisition，被 rejected 的 duplicate attempt 仍会执行到 `_tickfile_seqno += 1`（浪费 seqno）。将 guard check、pending pop、seqno increment 放在同一个 `with self._state.lock:` block 内可避免此问题。

### 3. `_tickfile_skip_warned_keys` 必须在 `__init__` 中初始化，不可用 class-level mutable default

**来源**: R2 Agent 1 C2

**问题**: Python class-level `_tickfile_skip_warned_keys: set = set()` 会在所有实例间共享。测试中多个 flusher 实例会互相污染。

**正确做法**:
```python
def __init__(self, ...):
    ...
    self._tickfile_skip_warned_keys: set = set()  # 实例级，非 class 级
```

**线程安全注释**: `_tickfile_skip_warned_keys` 从 tickfile-writer、clock-thread（overflow）、main-thread（drain）访问。Python set 的 `in` 和 `add()` 在 CPython GIL 下原子执行。作为诊断字段，最坏情况是重复 WARNING，可接受。应添加注释：`# Diagnostic set: GIL-dependent, approximate. Acceptable for WARNING dedup.`

### 4. `_tickfile_skip_warned_keys` 需在 cross-day 时 `.clear()`

**来源**: R2 Agent 2 M2

**位置**: `_step1_cross_day_check`（flusher.py ~line 232-259），与其他 clear 操作一起。

```python
# In _step1_cross_day_check, alongside existing clears:
self._tickfile_skip_warned_keys.clear()
```

### 5. `_generated_tickfile_minutes.clear()` 顺序约束

**来源**: R2 Agent 2 M3

**顺序**: force-generate loop → log failures → **clear `_generated_tickfile_minutes`** → clear `_tickfile_pending`

**理由**: force-generate loop 中 `_try_generate_tickfile` 需要 `_generated_tickfile_minutes` 未被 clear（去重 guard 在 force-generate 期间仍需工作）。Clear 必须在 force-generate 完成后。

### 6. `not selected` early return 也应添加到 `_generated_tickfile_minutes`

**来源**: R2 Agent 1 M1 + R2 Agent 3 Major 1

**理由**: `not selected` 时 pending 已被 pop（数据已消费），不可重试。标记为"已处理"防止 double-enqueue 第二次尝试。

```python
if not selected:
    logger.warning("Tickfile: no records selected for minute=%s", minute_key)
    with self._state.lock:
        self._state._generated_tickfile_minutes.add(minute_key)
    return
```

### 7. Fix-E 局部变量命名：`total_late_dropped`（无下划线前缀）

**来源**: R2 Agent 3 Minor 3

**理由**: 与现有 `_order_loop` 中 `late_order_per_minute`（无下划线）命名保持一致。下划线前缀在 Python 中暗示私有属性，但此变量是局部变量。

```python
# 使用:
total_late_dropped: int = 0  # 无下划线前缀
```

### 8. Fix-C `hasattr` guard 可移除

**来源**: R2 Agent 3 Suggestion 3

**理由**: `flushed_snapshot_minutes` 在 `aggregator.py:77` 始终初始化为 `set()`，`hasattr` guard 永远为 True。可移除或改为 assertion（早期错误检测）。

### 9. Fix-C Layer 3 需 `flushed_order_minutes` 集合避免 false positive

**来源**: R5 Agent 1 M2 + R5 Agent 3 C1

**问题**: Layer 3 比对 `flushed_snapshot_minutes - generated` 会将盘前/盘后仅有 snapshot 无 order 的分钟标记为 MISSING。这些分钟的 `select_tickfile_records` 返回空，被 Implementation Note #6 标记为"已生成"，但实际 tickfile CSV 中无对应行。

**解决方案**: 在 `SharedState` 添加 `flushed_order_minutes: set`，在 order 线程 flush 时添加分钟 key。Layer 3 改为 `comparison_mins = flushed_snaps & flushed_orders`，只比较有 order+snapshot 的分钟。

**fallback**: 如果 `flushed_order_minutes` 不可用（`hasattr` 返回 False），回退到 `flushed_snaps` 并在日志中标注 "(unfiltered, may include snapshot-only minutes)"。

---

## Implementation Plan 依赖顺序

> 供 writing-plans skill 参考的推荐实施顺序

| 步骤 | 文件 | 修改点 | 依赖 |
|------|------|--------|------|
| 1 | `config.py` | Fix-D: RecoveryConfig 添加字段 + load_config parser `s.getint(...)` | 无 |
| 2 | `engine.py:42` | Fix-D: 删除 `MAX_LATE_ORDER_RECORDS_PER_MINUTE` 硬编码常量 | 步骤 1 |
| 3 | `aggregator.py` | Fix-F: `SharedState` 添加 `_generated_tickfile_minutes: set` + `flushed_order_minutes: set` | 无 |
| 4 | `flusher.py __init__` | Fix-B: `self._tickfile_skip_warned_keys = set()` 初始化 | 无 |
| 4b | `engine.py` order flush 路径 | Fix-C: order 分钟 flush 时 `state.flushed_order_minutes.add(minute_key)` | 步骤 3 |
| 5 | `flusher.py:540-547` | Fix-F: 去重 guard + pending pop + seqno 同一 lock block（见注意事项 #1, #2） | 步骤 3 |
| 6 | `flusher.py:560-562` | Fix-F: `not selected` early return 也 add to generated set（见注意事项 #6） | 步骤 3 |
| 7 | `flusher.py:582` | Fix-F: IO error re-insert 检查 generated set（见注意事项 #1） | 步骤 3 |
| 8 | `flusher.py:711` | Fix-A: 删除 `_tickfile_pending.pop(k, None)` | 无 |
| 9 | `flusher.py:541-543` | Fix-B: per-minute-key WARNING 使用 `_skip_warned_keys` set | 步骤 4 |
| 10 | `flusher.py:231-265` | Fix-G: cross-day CRITICAL log + clear generated set + clear skip_warned（见注意事项 #4, #5） | 步骤 3+4 |
| 11 | `engine.py:621-625` | Fix-E: drop 计数 + final batch + shutdown 汇总（见注意事项 #7） | 步骤 2 |
| 12 | `engine.py stop()` | Fix-C: 3-layer shutdown check，Layer 3 用 `snaps & orders` 交集（见注意事项 #8, #9） | 步骤 3+4b |
| 13 | 7 个 config ini 文件 | Fix-D: production.ini + config.ini.example 必须添加；其余 5 个添加注释 | 步骤 1 |
| 14 | 2 个现有测试 | 更新断言 + 重命名类名 | 步骤 8 |
| 15 | 9 个新单元测试 | 按测试计划实现（见测试计划章节） | 步骤 5-12 |
| 16 | E2E 自动化测试 | `tests/test_e2e_tickfile_completeness.py` | 无 |

## 上线前 Checklist

- [ ] Fix-A 应用后 `_tickfile_pending` 仅被 `_try_generate_tickfile` pop
- [ ] Fix-F `_generated_tickfile_minutes` 去重 guard 正常工作（`.add()` 在 write 后）
- [ ] 2 个更新测试 + 9 个新测试通过
- [ ] E2E: tickfile 329/329（`snap_mins & order_mins` 交集，排除盘前/盘后）
- [ ] E2E: order 87.5M/87.5M
- [ ] shutdown 日志无 CHECK 3 FAIL
- [ ] `_tickfile_queue_skip_count` 合理（double-enqueue 为正常现象）
- [ ] 无 `Order late cap` WARNING（1M cap 足够覆盖 100x 测试）
- [ ] cross-day 无 CRITICAL 日志（所有 pending 成功生成）
- [ ] `production.ini` 添加 `max_late_order_records_per_minute` 并标注内存风险
