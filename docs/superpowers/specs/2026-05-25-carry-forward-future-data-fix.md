# Carry-Forward 未来数据问题 — 设计文档

**日期**：2026-05-25
**状态**：方案评审完成，待实施
**关联**：Phase 12 (Data-Driven Watermark Flush) 后续修复

---

## 1. 问题描述

### 1.1 现象

Snapshot 分钟文件中，carry-forward 行（`update_flag=N`）出现未来时间戳：

- `0830.csv` 包含 `time=0900` 的记录
- `1516.csv` 包含 `time=1517` 的记录
- `1524.csv` 包含 `time=1530` 的记录

### 1.2 问题升级：从测试环境问题到生产 Bug

初始评估认为这只是 speed=100 加速测试的产物。经深入分析确认：**1x 生产环境同样存在**。

**1x 生产环境重现路径**：

```
T=08:25  Symbol X 最后交易 → latest_snapshot[X] = {time: 0825...}
T=08:26~08:30  Symbol X 无交易
T=08:31:00.001  Symbol X 再次交易 → latest_snapshot[X] = {time: 0831...}  ← 覆盖
T=08:31:00.050  Clock thread tick → watermark=0831 → flush 0830
                  snapshot_copy[X] = {time: 0831...}  ← 未来数据！
                  0830.csv carry-forward: time=0831 → 数据逻辑错误
```

### 1.3 数据逻辑错误

Carry-forward 行语义是"截至该分钟，此 symbol 的最后已知状态"。0830 文件中的 carry-forward 不应包含 0831 的时间戳和价格——这违反了时序逻辑。

---

## 2. 根因分析

### 2.1 时序问题

`latest_snapshot` 是单槽存储，始终保留每个 symbol 的**最新**记录。`_flush_minutes_internal` 在 flush 时复制 `snapshot_copy = dict(self._state.latest_snapshot)`（flusher.py:218），用于 carry-forward。

```
aggregator.py:133    self.latest_snapshot[symbol] = record  ← N+1 记录已写入
aggregator.py:134    self.current_minute = N+1              ← watermark 推进
...
flusher.py:218       snapshot_copy = dict(self._state.latest_snapshot)  ← 包含 N+1 数据
```

Watermark 推进和 snapshot 污染在同一个 `process_snapshot` 调用内完成（同一把锁），flusher 获取锁时污染已经发生。

### 2.2 影响范围

| 组件 | 受影响 | 说明 |
|------|--------|------|
| `write_snapshot_file` else 分支 | 是 | carry-forward 使用 `snapshot_copy[symbol]`，可能包含未来数据 |
| `write_snapshot_file` elif 分支 | 间接 | `snapshot_copy[symbol]` 用于非 OHLCV 字段，理论上 raw_records 优先级更高 |
| `write_kline_file` else 分支 | 是 | carry-forward 的 OHLC 使用 `rec.lastprice`，可能来自未来 |
| `replay.py _flush_snapshot_minute` | 是 | 同样使用 `dict(state.latest_snapshot)` |

---

## 3. 修复方案：分钟级快照（Per-Minute Snapshot）

### 3.1 核心思路

在 `current_minute` 从 N 推进到 N+1 时，**在更新 `latest_snapshot` 之前**快照当前状态，存为"minute N 结束时的全市场状态"。Flush 时使用该分钟对应的历史快照。

### 3.2 时序正确性保证

```
N+1 记录到达 (minute_key=N+1 > current_minute=N):

  1. 检测 minute advance: N+1 > N → True
  2. 快照 dict(self.latest_snapshot)              ← 此时只有 ≤N 的数据
  3. current_minute = N+1
  4. latest_snapshot[symbol] = N+1 record          ← N+1 数据此时才写入

快照严格在 N+1 记录写入前捕获，不存在任何污染。
```

### 3.3 修改文件清单

#### 3.3.1 aggregator.py — 快照捕获（~10 LOC）

新增字段：

```python
# SharedState.__init__
self._snapshot_at_minute_end: Dict[str, Dict[str, SnapshotRecord]] = {}
```

`process_snapshot` 中的代码重排：

```python
# 原代码（line 133-135）:
self.latest_snapshot[symbol] = record
if not self.current_minute or minute_key > self.current_minute:
    self.current_minute = minute_key

# 修改为:
if not self.current_minute or minute_key > self.current_minute:
    if self.current_minute:
        self._snapshot_at_minute_end[self.current_minute] = dict(self.latest_snapshot)
    self.current_minute = minute_key
self.latest_snapshot[symbol] = record
```

#### 3.3.2 flusher.py — 使用分钟级快照（~20 LOC）

`_flush_minutes_internal` 修改：

```python
with self._state.lock:
    # ... 现有 pop ohlcv/raw/order 逻辑 ...

    # 弹出分钟级快照
    snapshots = {}
    for k in minute_keys:
        snap = self._state._snapshot_at_minute_end.pop(k, None)
        if snap is not None:
            snapshots[k] = snap
    # 回退：无快照的分钟用当前 latest_snapshot
    snapshot_copy = dict(self._state.latest_snapshot)

# 在 flush 循环中：
for minute_key in minute_keys:
    ...
    minute_snapshot = snapshots.get(minute_key, snapshot_copy)
    self._write_minute_files(minute_key, minute_snapshot, data, raw, orders)
```

`_step1_cross_day_check` 修改：
- 跨日分钟 flush 使用分钟级快照
- 跨日清理时 `_snapshot_at_minute_end.clear()`

#### 3.3.3 replay.py — 同模式（~10 LOC）

`_flush_snapshot_minute` 修改：

```python
with state.lock:
    ohlcv_data = dict(state.ohlcv_buffers.get(minute_key, {}))
    raw_records = dict(state.raw_snapshot_buffers.get(minute_key, {}))
    snapshot_copy = state._snapshot_at_minute_end.pop(minute_key, None)
    if snapshot_copy is None:
        logger.debug("No per-minute snapshot for %s, using current latest_snapshot", minute_key)
        snapshot_copy = dict(state.latest_snapshot)
```

#### 3.3.4 writer.py — 时间过滤兜底（~25 LOC）

添加辅助函数：

```python
def _minute_end_threshold(minute_key: str) -> int:
    """Return next minute start as 17-digit time integer."""
    yyyymmdd = int(minute_key[:8])
    hh = int(minute_key[8:10])
    mm = int(minute_key[10:12])
    mm += 1
    if mm >= 60:
        mm -= 60
        hh += 1
        if hh >= 24:
            hh = 0
            yyyymmdd += 1
    return int(f"{yyyymmdd:08d}{hh:02d}{mm:02d}00000")
```

`write_snapshot_file` carry-forward 行过滤：

```python
minute_end = _minute_end_threshold(minute_key)

# else 分支（carry-forward）:
rec = snapshot_copy[symbol]
if rec.time >= minute_end:
    continue  # 跳过带未来时间戳的 carry-forward
lines.append(_format_snapshot_row(rec, name, "N"))
```

`write_kline_file` carry-forward 同理。

### 3.4 边界情况处理

| 场景 | 处理方式 | 正确性 |
|------|---------|--------|
| 第一分钟数据 | `current_minute` 为空不快照 → fallback 到 `latest_snapshot` | 安全：只有本分钟数据，无未来数据 |
| 最后分钟 1530 | 无后续分钟触发快照 → fallback + writer 过滤 | 安全：writer 过滤是 MANDATORY |
| 跨日 flush | 快照在跨日前已捕获，flush 后清理 | 安全：弹出在清理前 |
| 批量 flush 多分钟 | 每分钟独立快照，无快照的用 fallback | 安全：writer 过滤兜底 |
| 延迟 flush（DELAYED_FLUSH_ROUNDS=3） | 快照在 minute advance 时捕获，与 flush 延迟无关 | 安全：快照是时间点快照 |
| 迟到记录 | 走 late path，不经过 `process_snapshot` | 安全：独立路径，不影响快照 |
| 乱序数据 | 已捕获快照是独立 dict 副本 | 安全：后续更新不影响 |
| 重启恢复 | `_snapshot_at_minute_end` 是内存结构，重启丢失 → fallback | 已知限制：fallback + writer 过滤 |

### 3.5 内存分析

- 每个快照：~4500 symbol × ~170 bytes ≈ 765KB
- `data_flush_delay_minutes=1`：最多 1-2 个快照 < 2MB
- Stall 极端情况（360 分钟）：~275MB（可接受，stall 本身就触发告警）
- 快照在 flush 时弹出（pop），不累积

### 3.6 日志要求

| 事件 | 日志级别 | 格式 |
|------|---------|------|
| 无 per-minute snapshot，使用 fallback | DEBUG | `No per-minute snapshot for {minute_key}, using current latest_snapshot` |
| Writer 过滤跳过 carry-forward | WARNING | `Skipping carry-forward for symbol {symbol} in minute {minute_key}: time {rec.time} >= minute_end {minute_end}` |

---

## 4. 三 Agent 评审结果

### 4.1 Agent 1：并发安全

| 检查项 | 结果 | 说明 |
|--------|------|------|
| 快照捕获锁安全 | PASS | `process_snapshot` 全程持有 `self.lock`（RLock） |
| 快照弹出锁安全 | PASS | `_flush_minutes_internal` 在 `self._state.lock` 内弹出 |
| `dict()` 浅拷贝安全 | PASS | `SnapshotRecord` 是 `frozen=True`，对象不可变 |
| 乱序数据不污染快照 | PASS | 已捕获快照是独立 dict 副本 |
| 迟到记录路径隔离 | PASS | 迟到记录走 `_late_snapshot_records`，不经过快照逻辑 |
| Writer 时间过滤安全 | PASS | `>= minute_end` 不会误过滤合法记录 |
| **无快照 fallback** | **CRITICAL** | 最后活跃分钟无快照 → **Writer 时间过滤是 MANDATORY** |
| 内存边界 | PASS | 正常 <2MB，极端 ~275MB |

### 4.2 Agent 2：边界情况与回归

| 检查项 | 结果 |
|--------|------|
| 第一分钟 / 最后分钟 / 跨日 / 批量 flush / 延迟 flush / 乱序 / 迟到 | 全 PASS |
| 跨日 flush 顺序 | **CRITICAL**：必须在清理 `_snapshot_at_minute_end` 前弹出快照 |
| Writer 过滤日志 | **Important**：跳过时应输出 WARNING |
| 现有 108 测试 | **零回归预期** |

### 4.3 Agent 3：设计一致性

| 评估维度 | 结论 |
|----------|------|
| 设计一致性 | 强一致：per-minute snapshot 是 data-driven watermark 哲学的自然延伸 |
| 与 `_flush_minutes_internal` 复用 | 完全兼容 |
| 与迟到记录处理 | 正交 |
| 设计文档更新 | 需更新 watermark 设计文档 Section 5.4/5.5/7/8 |
| 实现复杂度 | ~145-185 LOC（含测试），Low-Medium |
| 总体风险 | LOW（内存、并发、回归均 LOW） |

**综合结论：APPROVED（三 Agent 一致）**

---

## 5. 测试计划

### 5.1 新增单元测试

| 测试 | 文件 | 说明 |
|------|------|------|
| 快照捕获时机 | `test_aggregator.py` | 处理 N 分钟数据 → 验证 `_snapshot_at_minute_end[N]` 为空；处理第一条 N+1 → 验证快照已捕获且不含 N+1 数据 |
| 快照弹出 | `test_aggregator.py` | 验证 pop 移除条目并返回 dict；不存在的 key 返回 None |
| Flusher 使用分钟级快照 | `test_watermark_flusher.py` | 设置快照 → flush → 验证使用快照而非当前 `latest_snapshot` → 验证快照已清理 |
| Writer 时间过滤 | `test_writer.py` | `snapshot_copy` 中包含 `time >= minute_end` 记录 → 验证 carry-forward 被跳过；验证 active 记录不受影响 |
| Replay 分钟级快照 | `test_replay.py` | 验证 `_flush_snapshot_minute` 弹出快照；无快照时 fallback |
| 跨日清理 | `test_flusher.py` | 验证跨日后 `_snapshot_at_minute_end` 被清除 |

### 5.2 集成测试

- Speed=100 carry-forward 正确性：验证每分钟 carry-forward 的 time 在该分钟范围内
- 回归：108 个现有测试全部通过

---

## 6. 4 个问题的最终评估

| Issue | 原评级 | 修正评级 | 修复方案 |
|-------|--------|----------|---------|
| #1 Snapshot 正常生成 | OK | OK | 无需修改 |
| #2 Carry-forward 未来数据 | P2 | **P1** | 分钟级快照 + writer 时间过滤（MANDATORY）✅ 已修复 |
| #3 1530 未生成 | P2 | **P1** | Stall-triggered flush（见 Section 7） |
| #4 name 缺失 | P2 | P2 | 测试配置 `code_refresh_sec=1` ✅ 已验证 |

---

## 7. Issue #3 修复方案：Stall-Triggered Flush

### 7.1 问题

`is_data_driven_expired` 要求 `watermark >= minute_key + delay_minutes`。1530 是最后一分钟，之后没有新数据推进 watermark → 1530 永远不满足过期条件。

现有兜底机制 `flush_all_remaining()` 在 `stop()` 中调用，但进程被 SIGKILL 时 `stop()` 可能未执行。

### 7.2 方案：Watermark Stall 触发 Flush

Watermark 停滞超过 `stall_flush_sec`（默认 300s，可配置）意味着数据线程已停止提供新数据。此时直接 flush 所有残余 buffer。

### 7.3 安全性

1. 数据线程已停止推进 watermark（stall_flush_sec 无新数据）→ buffer 不会再增长
2. `_flush_minutes_internal` 在 clock 线程内执行，与 `_step3` 同一线程，无并发风险
3. buffer 在 lock 内 pop，即使数据线程恢复也只能创建新的 buffer entry
4. Order 线程有独立 stall 检测（见 7.5）

### 7.4 实现方案

#### 7.4.1 配置化 `stall_flush_sec`

**config.py** — `RecoveryConfig` 新增字段：

```python
stall_flush_sec: int = 300  # Watermark stall threshold for triggering flush
```

INI 配置（`[recovery]` section）：

```ini
stall_flush_sec = 300  ; 生产环境
stall_flush_sec = 30   ; 测试环境（config-watermark-test.ini）
```

#### 7.4.2 Flusher stall-triggered flush

**flusher.py** `_step3_minute_output`，stall 检测后增加 flush：

```python
# 构造函数新增参数
self._stall_flush_sec = stall_flush_sec

# _step3_minute_output 中，stall 检测后：
if not self._stall_warned and stalled_sec >= self._stall_flush_sec:
    logger.error("Watermark stalled at %s for %.0f seconds ...", data_watermark, stalled_sec)
    self._stall_warned = True

    # Flush all remaining ohlcv buffers — data source has stopped
    with self._state.lock:
        stall_keys = sorted(self._state.ohlcv_buffers.keys())
    if stall_keys:
        logger.info("Stall-triggered flush: %d remaining minutes", len(stall_keys))
        self._flush_minutes_internal(stall_keys, is_final=False)
```

#### 7.4.3 Order 线程 stall 检测

**engine.py** `_order_loop` 新增 stall 检测（与 flusher 同模式）：

```python
# _order_loop 新增变量
prev_order_minute: Optional[str] = None
last_order_advance_ts = time.monotonic()
order_stall_warned = False

# while 循环末尾（_enforce_max_pending 之后）：
if current_minute != prev_order_minute:
    last_order_advance_ts = time.monotonic()
    order_stall_warned = False
    prev_order_minute = current_minute
else:
    stalled_sec = time.monotonic() - last_order_advance_ts
    if not order_stall_warned and stalled_sec >= stall_flush_sec:
        logger.error("Order watermark stalled at %s for %.0f seconds", current_minute, stalled_sec)
        order_stall_warned = True
        if buffers:
            logger.info("Stall-triggered order flush: %d remaining minutes", len(buffers))
            self._flush_all_order_buffers(buffers, output_dir)
```

### 7.5 三 Agent 评审结论（2026-05-25）

| 维度 | Agent 1 (并发) | Agent 2 (边界) | Agent 3 (设计) |
|------|---------------|---------------|---------------|
| 线程安全 | 全 PASS | — | — |
| `is_final=False` | 正确 | 正确 | 正确，保持不变 |
| `STALL_WARN_SECONDS` 硬编码 | — | **CRITICAL：300s 对测试环境太长** | 建议 configurable |
| SIGKILL 前未达阈值 | — | 已知限制，需文档化 | 需文档化 |
| Order 线程同类 bug | **发现 order 线程有相同问题** | — | — |
| Replay 引擎 | — | — | 不受影响（自有 EOF flush） |

**综合结论：APPROVED（三 Agent 一致），附带调整**：

1. `stall_flush_sec` 改为可配置（三 Agent 一致建议） — **已采纳**
2. Order 线程独立 stall 检测（Agent 1 发现） — **已采纳**
3. SIGKILL 在阈值前 → 最后分钟丢失为已知限制（生产用 SIGTERM）

### 7.6 与 `flush_all_remaining` 的关系

| 特性 | Stall-triggered flush | `flush_all_remaining` |
|------|----------------------|----------------------|
| 触发时机 | watermark 停滞 ≥ `stall_flush_sec` | `stop()` 所有线程 join 后 |
| 调用线程 | clock 线程 / order 线程 | 主线程 |
| `is_final` | False（IO 失败 → SystemExit） | True（IO 失败 → continue） |
| 覆盖场景 | 数据停止但进程存活 | 正常 shutdown |
| Order 覆盖 | order 线程独立检测 | 主线程 flush |

两者互补：正常 shutdown 用 `flush_all_remaining`，数据停止用 stall-triggered flush。

### 7.7 修改文件清单

| 文件 | 变更 | LOC |
|------|------|-----|
| `config.py` | `RecoveryConfig` 新增 `stall_flush_sec`，`load_config` 解析 | ~4 |
| `clock.py` | 移除 `STALL_WARN_SECONDS` 常量 | -1 |
| `flusher.py` | 构造函数新增 `stall_flush_sec`，`_step3` stall 触发 flush | ~10 |
| `engine.py` | 传递 `stall_flush_sec`，`_order_loop` 新增 stall 检测 | ~15 |
| `config/*.ini` | `[recovery]` 新增 `stall_flush_sec` | ~1 each |
| **测试** | `test_flusher.py` 新增 stall flush 测试 | ~60 |

### 7.8 测试计划

| 测试 | 文件 | 说明 |
|------|------|------|
| Stall 触发 ohlcv flush | `test_flusher.py` | 停滞 ≥ `stall_flush_sec` → 验证 ohlcv buffer 被 flush |
| Stall 不到阈值不触发 | `test_flusher.py` | 停滞 < `stall_flush_sec` → 验证 buffer 不被 flush |
| Stall flush 后数据恢复 | `test_flusher.py` | stall flush 后数据恢复 → 验证新 buffer 正常处理 |
| Order stall flush | `test_engine.py` | Order watermark 停滞 → 验证 order buffer 被 flush |

### 7.9 已知限制

- **SIGKILL 在阈值前**：进程被 SIGKILL 且 `stall_flush_sec` 未到 → 最后分钟丢失。生产环境应使用 SIGTERM。
- **Replay 引擎不受影响**：Replay 有自有 EOF final flush 机制，不存在此问题。
- **内存中的 stall 状态**：`_stall_flush_sec` 是进程内状态，重启后重置。重启后依赖 `flush_all_remaining`。

### 7.10 风险

| 风险 | 级别 | 说明 |
|------|------|------|
| 误触发（数据暂时停滞） | LOW | 阈值可配置；即使触发，已 flush 数据正确，新数据走正常路径 |
| 并发竞争 | LOW | clock/order 线程各操作独立 buffer，使用 lock 保护 |
| Order stall 检测延迟 | LOW | Order 线程 sleep 间隔决定检测粒度（通常 ≤1s） |

---

## 8. Live 端到端测试结果（2026-05-25）

### 测试配置
- Simulator: `--speed 100 --file-types order,snapshot,code`
- Minute bar: `config-watermark-test.ini`（`enable_time_fallback=false`, `code_refresh_sec=1`）

### Carry-forward 修复验证（Issue #2）

| 检查项 | 结果 |
|--------|------|
| 328 个 snapshot 文件全扫描 | **0 条未来数据** |
| 0830/0900/0913/1516/1524 重点检查 | 全部 OK |
| WARNING 日志（writer 过滤触发） | 无（说明主机制 per-minute snapshot 有效） |

### Code table name 验证（Issue #4）

| 分钟 | 总行数 | 空 name | 填充率 |
|------|--------|---------|--------|
| 0800 | 4,809 | 3,879 | 19.3% |
| 0830 | 4,524 | 3,912 | 13.5% |
| 0900 | 60,270 | 36,654 | 39.2% |
| 0910 | 41,339 | 16 | **100.0%** |
| 0913+ | — | 16 | ~100% |

`code_refresh_sec=1` 效果显著，0910 起基本全覆盖。

### 1530 缺失复现（Issue #3）

进程被 SIGKILL 终止 → `stop()` 未执行 → `flush_all_remaining()` 未调用 → snapshot 止于 1524，order 止于 1337。需 Stall-triggered flush 修复。
