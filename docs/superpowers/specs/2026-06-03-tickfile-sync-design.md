# Tickfile Synchronous Generation Design Spec

> **Version**: Round 8 Final (2026-06-03) | **Status**: Review Passed
>
> **Depends on**: `2026-06-01-tickfile-generation-design.md` (Phase 16)
>
> **Review history**: R1→R7(0C in last 4 rounds)→R8(3-agent,0C/1 actionable M: re-insert replace vs extend)→fix→R2 verify PASS

## 1. Problem

Live 模式下 tickfile 生成绑定在 snapshot flush 上。Order 数据量是 snapshot 的 ~8 倍（5.6GB vs 698MB），order 线程系统性滞后。Tickfile 写入时 `raw_order_buffers[minute_key]` 为空或仅部分填充，只能用 `latest_order_by_symbol` carry-forward 数据（覆盖率 95.8%，但为前几分钟的旧 bid/ask，非当前分钟真实 order）。

**Live 测试数据佐证**（speed=5, order-speed=100）：
```
Seqno=1   (0800): bid=47.1%  — 盘前，部分 symbol 无 order
Seqno=2   (0830): bid=84.5%  — carry-forward 开始积累
Seqno=4+  (0900+): bid=95.8% — 稳定，但全部是 carry-forward
```

## 2. Solution: Dual-Thread Join Sync

Tickfile 是 snapshot 分钟文件和 order 分钟文件的下游产物。**两者都完成才生成**。

### 2.1 时序图

```
Case A: Snapshot 先完成（典型，order 8x 数据量滞后）
  ────────────────────────────────────────────────────────
  Clock Thread:
    flush snapshot X
      → write snapshot/kline files (tickfile NOT written)
      → _tickfile_pending[X] = {raw_records, snapshot_copy}
      → flushed_snapshot_minutes.add(X)
      → check order_current_minute > X → NO → return

  Order Thread:
    ... processing X records → raw_order_buffers[X] accumulating ...
    (条件: X in _tickfile_pending → 继续写入 raw_order_buffers)
    flush order X (write order file)
      → _flushed_order_minutes.add(X)
      → _tickfile_trigger_pending.append(X)  ← 仅记录，不更新 order_current_minute
    batch write (raw_order_buffers[X] complete)
      → _drain_tickfile_triggers:
           order_current_minute = X  ← batch write 后更新
           catch-up scan: check _tickfile_pending[X] → FOUND
           _try_generate_tickfile(X)
        lock: pop _tickfile_pending[X], pop raw_order_buffers[X] (COMPLETE),
              copy latest_order_by_symbol, _tickfile_seqno += 1
        IO:   select_tickfile_records → write_tickfile_rows
        verify: check file exists and has expected rows

Case B: Order 先完成（罕见，snapshot 瓶颈时）
  ────────────────────────────────────────────────────────
  Order Thread:
    flush order X
      → _flushed_order_minutes.add(X)
      → _tickfile_trigger_pending.append(X)
    batch write (raw_order_buffers[X] complete)
      → order_current_minute = X    ← 在 batch write 之后更新
      → drain: _try_generate_tickfile(X) → _tickfile_pending[X] NOT FOUND → no-op

  Clock Thread:
    flush snapshot X
      → _tickfile_pending[X] = {raw_records, snapshot_copy}
      → flushed_snapshot_minutes.add(X)
      → check order_current_minute > X → YES/NO
        YES (order moved to Y > X): clock thread triggers _try_generate_tickfile(X)
        NO  (order_current_minute == X): order thread catch-up handles it
          → 下一轮 order batch write 时 catch-up 扫描 _tickfile_pending
          → 发现 X <= order_current_minute → 触发 _try_generate_tickfile(X)

Case C: EOF 兜底（order 永远不到该分钟）
  ────────────────────────────────────────────────────────
  Clock Thread:
    flush_all_remaining()
      → for each remaining _tickfile_pending[X]:
          _try_generate_tickfile(X) with carry-forward
          (raw_order_buffers[X] may be empty → carry-forward)

Case D: Pending overflow（order 极端滞后）
  ────────────────────────────────────────────────────────
  Clock Thread (every tick):
    check len(_tickfile_pending) > MAX_TICKFILE_PENDING_MINUTES
      → force-generate oldest pending minutes with carry-forward

Case E: Cross-day 兜底（跨日时 order 未到达的 pending 分钟）
  ────────────────────────────────────────────────────────
  Clock Thread:
    _step1_cross_day_check()
      → _flush_minutes_internal for yesterday's minutes (may create pending)
      → for each remaining _tickfile_pending[X]:
          _try_generate_tickfile(X) with carry-forward  ← NEW: 先生成再清除
      → _tickfile_pending.clear()  ← 清除残留（仅 force-generate 失败的）
      → _tickfile_seqno = 0
      → order_current_minute = ""
```

**关于 daily tickfile 行序**：由于 Case A 和 Case B 允许不同线程并发触发不同分钟的 tickfile 生成，daily tickfile 中的行物理顺序可能与分钟时间顺序不同。但 seqno 保证单调递增（在 SharedState lock 下分配）。下游消费者应按 seqno 排序而非按文件物理位置。

### 2.2 核心数据流改变

```
BEFORE (Phase 16 — 同步 snapshot flush):
  _flush_minutes_internal (clock thread):
    lock:  pop raw_order_buffers[X]          ← 此时可能为空
    lock:  copy latest_order_by_symbol
    IO:    _write_minute_files → tickfile generated
           → select_tickfile_records(raw, snap, orders=[], carry)
           → orders 为空 → 全部走 carry-forward

AFTER (Phase 17 — 双线程 join):
  _flush_minutes_internal (clock thread):
    lock:  DO NOT pop raw_order_buffers[X]   ← 不弹出了
    IO:    _write_minute_files (no tickfile)
    lock:  _tickfile_pending[X] = snapshot data
           flushed_snapshot_minutes.add(X)
           if order_current_minute > X → tickfile_trigger_keys.append(X)
    IO:    for mk in tickfile_trigger_keys: _try_generate_tickfile(mk)

  _flush_order_minute (order thread, inside for line loop):
    IO:    write order file
           _tickfile_trigger_pending.append(minute_key)  ← 延迟记录
    (不在此处更新 order_current_minute — batch write 未发生，raw_order_buffers 可能不完整)

  batch write (order thread, after for line loop):
    lock:  raw_order_buffers.update(所有 accumulated records)
    lock:  order_current_minute = max(all flushed minutes)  ← 在 batch write 之后更新
           catch-up: scan _tickfile_pending for minutes <= order_current_minute
    IO:    for mk in (deferred triggers + catch-up keys):
             _try_generate_tickfile(mk)             ← 此时 raw_order_buffers 完整
```

## 3. SharedState New Fields

`src/minute_bar/aggregator.py` `SharedState.__init__` 新增 3 个字段：

| 字段 | 类型 | 初始值 | 用途 | 访问线程 |
|------|------|--------|------|---------|
| `_tickfile_pending` | `Dict[str, dict]` | `{}` | 已 flush snapshot 但等 order 的 tickfile 数据 | clock + order |
| `_tickfile_seqno` | `int` | `0` | tickfile seqno（从 flusher 迁移） | clock + order |
| `order_current_minute` | `str` | `""` | order 线程处理进度（"" 表示尚未 flush 任何 order，小于任何有效 minute_key） | clock 读 + order 写 |

**`_tickfile_pending` entry 结构**：
```python
_tickfile_pending[minute_key] = {
    'raw_records': Dict[str, List[SnapshotRecord]],  # raw_snapshot_buffers 的数据
    'snapshot_copy': Dict[str, SnapshotRecord],       # carry-forward snapshot
}
```

**INVARIANTS**:
- `_tickfile_pending` 中的数据在 tickfile 生成后立即清除
- `_tickfile_seqno` 递增仅在 `_try_generate_tickfile` 内，在 SharedState lock 下
- `order_current_minute` 在 batch write 完成后更新（在 SharedState lock 下），**不在** `_flush_order_minute` 内更新
- `enable_tickfile` 在构造后不可变（从 config 读取，运行时不改变）
- **Config validation**: `enable_tickfile=True` 要求 `enable_order=True`（构造时 `if not ... raise ValueError`）

## 4. Detailed Changes

### 4.1 `src/minute_bar/aggregator.py`

**位置**: `SharedState.__init__` (line ~87 之后)

新增字段：
```python
# Tickfile sync: snapshot data waiting for order completion
self._tickfile_pending: Dict[str, dict] = {}
# Tickfile seqno (shared between clock and order threads)
self._tickfile_seqno: int = 0
# Order thread processing progress (updated in _drain_tickfile_triggers AFTER batch write)
# "" = no order flushed yet; always < any valid minute_key
self.order_current_minute: str = ""
```

### 4.2 `src/minute_bar/flusher.py`

#### 4.2.1 Remove `_tickfile_seqno` instance variable

**位置**: `__init__` (line 75)

```python
# REMOVE this line:
self._tickfile_seqno: int = 0
```

所有 `self._tickfile_seqno` 引用改为 `self._state._tickfile_seqno`。

**同时 REMOVE** `flusher.py` line 134 的 `self._tickfile_seqno = 0`（跨日 reset）。该 reset 已移至 SharedState lock scope 内（见 4.2.8）。如果保留旧行，order thread 可能在 seqno 递增后被重置为 0，违反 Invariant #4。

#### 4.2.2 Seqno recovery at initialization

**位置**: `__init__` 末尾，或 `tick()` 首次调用时。

`recover_tickfile_seqno_lazy`（从已有文件读取最大 seqno）涉及文件 IO。为避免竞态，**仅在 flusher 初始化时执行一次**，不在 `_try_generate_tickfile` 中执行。

```python
# In __init__ (after self._enable_tickfile = enable_tickfile):
if enable_tickfile:
    # Config validation: tickfile requires order (ValueError, not assert — survives -O)
    if not self._enable_order:
        raise ValueError("enable_tickfile=True requires enable_order=True")
    # Recover seqno from existing tickfile (single-threaded, before engine.start())
    # Note: flusher.__init__ does not receive AppConfig. target_date is derived
    # from jst_now_yyyymmdd() or passed as a new constructor parameter.
    target_date = jst_now_yyyymmdd()
    self._state._tickfile_seqno = recover_tickfile_seqno_lazy(output_dir, target_date)
```

新增辅助函数 `recover_tickfile_seqno_lazy`：
```python
def recover_tickfile_seqno_lazy(output_dir: str, date: str) -> int:
    """Recover seqno from existing tickfile for a specific date. Returns 0 if no file found.
    Called once at startup, before any threads start.

    Reuses existing recover_tickfile_seqno from writer.py which correctly
    reads Seqno at column index 59 (TICKFILE_HEADER: ...,ActionDay,Type,Seqno,...).
    The existing function also has a 200MB safety limit (MAX_RECOVERY_SIZE)
    and handles corrupt/truncated lines.
    """
    from minute_bar.writer import recover_tickfile_seqno
    # Pick any minute key for the target date to locate the daily tickfile
    sample_minute_key = f"{date}0800"  # Only date portion used by get_tickfile_path (daily file); 0800 is arbitrary
    try:
        return recover_tickfile_seqno(output_dir, sample_minute_key)
    except Exception:
        logger.warning("Failed to recover tickfile seqno for date=%s", date)
        return 0
```

**时序保证**: `recover_tickfile_seqno_lazy` 在 `engine.start()` 之前调用（flusher 构造时），此时无其他线程运行。无竞态。

#### 4.2.3 Remove tickfile from `_write_minute_files`

**位置**: `_write_minute_files` (lines 364-373)

删除整个 `if self._enable_tickfile:` block（10 行）。Tickfile 不再在此方法中生成。

#### 4.2.4 Modify `_flush_minutes_internal` — don't pop `raw_order_buffers` for tickfile

**位置**: `_flush_minutes_internal` (lines 255-259)

**改动**: 当 `enable_tickfile=True` 时，不 pop `raw_order_buffers`（留给 tickfile 生成时 pop）。

```python
# AFTER:
order_data = {}
if not self._enable_tickfile:
    for k in minute_keys:
        v = self._state.raw_order_buffers.pop(k, None)
        if v is not None:
            order_data[k] = v
```

**`enable_tickfile=True` 时的行为**：
- `order_data` 保持为空 dict
- `_write_minute_files` 中 `if self._enable_order and order_records:` 条件为 False → 不写 order file
- **⚠️ 实施顺序**：Section 4.2.3（删除 tickfile block）和 Section 4.2.4（skip pop）**必须同时实施**。如果只实施 4.2.4 而遗漏 4.2.3，`_write_minute_files` 仍会用旧逻辑生成 tickfile（empty order_data → carry-forward），`_tickfile_pending` 被旧代码消费，新逻辑找不到 pending 数据。
- Order file 已由 order 线程的 `_flush_order_minute` 独立写入（从 local buffer），不丢数据
- `raw_order_buffers[minute_key]` 保持完整，等 tickfile 生成时由 `_try_generate_tickfile` pop

**前提条件**: `enable_tickfile=True` 要求 `enable_order=True`（见 4.2.2 config validation）。这确保 order file 总是由 order thread 写入。

**内存安全**：见 Section 5.2 内存控制。

#### 4.2.5 Store `_tickfile_pending` + trigger in second lock scope

**位置**: `_flush_minutes_internal` 第二个 lock scope (lines 293-299)

在 `flushed_snapshot_minutes.add(minute_key)` 之后新增。**注意**：不在 lock 内做 IO，仅记录需要触发的分钟。

```python
tickfile_trigger_keys = []
with self._state.lock:
    self._state.output_minutes.add(minute_key)
    self._state.last_output_minute = minute_key
    self._state.flushed_snapshot_minutes.add(minute_key)
    for sym, agg in data.items():
        self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
        self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
    # NEW: Store tickfile pending data
    if self._enable_tickfile:
        self._state._tickfile_pending[minute_key] = {
            'raw_records': raw,
            'snapshot_copy': minute_snapshot,
        }
        # If order already passed this minute, trigger tickfile after lock release
        if self._state.order_current_minute > minute_key:
            tickfile_trigger_keys.append(minute_key)

# After lock scope — IO outside lock
for mk in tickfile_trigger_keys:
    try:
        self._try_generate_tickfile(mk)
    except Exception:
        if is_final:
            logger.exception("Tickfile generation failed for minute=%s", mk)
        else:
            logger.fatal("Tickfile generation failed for minute=%s", mk)
            raise SystemExit(1)
```

#### 4.2.6 New method: `_try_generate_tickfile`

**唯一方法**，所有 tickfile 生成都通过此方法。

```python
def _try_generate_tickfile(self, minute_key: str) -> None:
    """Generate tickfile for a minute. Thread-safe. Callable from any thread.

    Returns silently if no pending data found (already generated by other thread).
    On IO failure, re-inserts ALL popped data for flush_all_remaining retry.
    """
    import threading
    import time

    # Step 1: Pop data under lock (no IO)
    with self._state.lock:
        pending = self._state._tickfile_pending.pop(minute_key, None)
        if pending is None:
            return
        order_records = self._state.raw_order_buffers.pop(minute_key, [])
        latest_order_copy = dict(self._state.latest_order_by_symbol)
        self._state._tickfile_seqno += 1
        current_seqno = self._state._tickfile_seqno

    # Step 2: IO outside lock
    start_ts = time.monotonic()
    try:
        from minute_bar.tickfile import select_tickfile_records
        from minute_bar.writer import write_tickfile_rows

        raw_records = pending['raw_records']
        snapshot_copy = pending['snapshot_copy']
        code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
        selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)

        if not selected:
            logger.warning("Tickfile: no records selected for minute=%s", minute_key)
            return  # seqno gap acceptable (see Invariant #15)

        write_tickfile_rows(self._output_dir, minute_key, selected, current_seqno, code_table_getter=code_getter)

        # Post-write verification (warning only — do NOT re-insert on failure)
        # If write_tickfile_rows succeeded without exception, the write is complete.
        # Re-inserting on os.path.exists=False risks duplicate rows on retry.
        path = get_tickfile_path(self._output_dir, minute_key)
        if not os.path.exists(path):
            logger.error("Tickfile file missing after successful write: %s (possible filesystem issue)",
                         path)

        elapsed_ms = (time.monotonic() - start_ts) * 1000
        logger.info("Tickfile generated minute=%s (%d symbols, %d orders, %.1fms) "
                    "[thread=%s, order_watermark=%s]",
                    minute_key, len(selected), len(order_records), elapsed_ms,
                    threading.current_thread().name,
                    self._state.order_current_minute)
        if elapsed_ms > 200:
            logger.warning("Tickfile generation slow: minute=%s %.1fms", minute_key, elapsed_ms)
    except Exception:
        # Re-insert ALL popped data so flush_all_remaining can retry
        with self._state.lock:
            self._state._tickfile_pending[minute_key] = pending
            if order_records:
                # Replace (not extend): order thread won't re-create entries after pop
                # (write condition mk not in _tickfile_pending → False), so existing
                # list is either empty or absent. Replace is safer than extend.
                self._state.raw_order_buffers[minute_key] = order_records
        logger.warning("Tickfile IO failed for minute=%s, re-inserted for retry [thread=%s]",
                       minute_key, threading.current_thread().name)
        raise
```

**线程安全分析**:
- `_tickfile_pending.pop()` 在 lock 内原子完成 → 无论哪个线程调用，只有一个线程能 pop 到数据
- `raw_order_buffers.pop()` 同理
- `_tickfile_seqno` 递增在 lock 内，`current_seqno` 拷贝到局部变量 → 后续 IO 使用局部变量，无竞态
- `write_tickfile_rows` 使用文件级写锁（`_get_write_lock`）→ 并发安全
- lock 外的 `select_tickfile_records` 是纯函数，操作已 pop 的局部变量 → 无竞态
- **IO 失败时 re-insert pending + raw_order_buffers** → `flush_all_remaining` 可重试，不会永久丢失数据
- **Post-write 验证** → warning only，不 re-insert（避免重复行风险）
- **线程名记录** → 区分 Case A（order thread 触发）vs Case B（clock thread 触发）

**关于 seqno gap**：当 `select_tickfile_records` 返回空时，seqno 已递增但无文件写入。这是 acceptable 的（见 Invariant #15）。当 IO 失败后重试，seqno 会再次递增，产生 gap。这是设计决策：正确性（保证 seqno 单调递增）优于连续性。

**`write_tickfile_rows` 契约要求**：`write_tickfile_rows`（writer.py）在遇到不可恢复错误时**必须抛出异常**，而非静默返回。当前实现有多个 silent return 路径需修复：

1. **writer.py line 334-336（header corrupted, non-empty file）**：`logger.error(...); return` → 改为 `raise IOError(...)`
2. **writer.py line 296（all rows failed to build）**：`if not rows: return` → 改为 `raise IOError("All tickfile rows failed to build")`
3. **writer.py line 273（empty input）**：`if not selected: return` → 此路径由 `_try_generate_tickfile` 的 `if not selected` 提前拦截，不会到达 `write_tickfile_rows`，无需修改
4. **writer.py line 337（empty file rewrite success）**：此 `return` 是正常的早期退出（重写成功），**保留不变**

**实施注意**：修改时仅需改路径 1 和 2。路径 3 已被上游拦截，路径 4 是正常流程。修改后 replay.py 的 `_flush_snapshot_minute`（line 262）也调用 `write_tickfile_rows`，需确保 replay 的调用路径也能正确处理 IOError（replay 当前不处理 tickfile 异常，需添加 try/except）。

#### 4.2.7 Pending overflow protection — `MAX_TICKFILE_PENDING_MINUTES`

**位置**: `tick()` 方法，在 `_step3_minute_output()` 之后

```python
MAX_TICKFILE_PENDING_MINUTES = 10

# In tick(), after _step3_minute_output:
if self._enable_tickfile:
    # Single lock scope for count + keys (avoid TOCTOU)
    with self._state.lock:
        pending_keys = sorted(self._state._tickfile_pending.keys())
        pending_count = len(pending_keys)
        if pending_count > MAX_TICKFILE_PENDING_MINUTES:
            # Force-generate oldest minutes to reduce count to MAX
            force_count = pending_count - MAX_TICKFILE_PENDING_MINUTES + 1
            force_keys = pending_keys[:force_count]
        else:
            force_keys = []

    if force_keys:
        logger.warning(
            "Tickfile pending overflow: %d minutes pending (max=%d), forcing %d oldest",
            pending_count, MAX_TICKFILE_PENDING_MINUTES, len(force_keys),
        )
        for mk in force_keys:
            try:
                self._try_generate_tickfile(mk)
            except Exception:
                logger.exception("Force tickfile generation failed for minute=%s", mk)
```

**设计决策**: 设 `MAX_TICKFILE_PENDING_MINUTES=10`。超过 10 说明 order 线程严重滞后（>10 分钟），此时 carry-forward 是合理的降级策略。Force-generated tickfile 使用 partial order data + carry-forward。

#### 4.2.8 Cross-day: `_step1_cross_day_check`

**位置**: line 134 及后续 cleanup scope

**关键修复**：
1. **REMOVE** line 134 的 `self._tickfile_seqno = 0`（旧 flusher instance variable 的 reset）
2. **新增 force-generate**：在 `_tickfile_pending.clear()` 之前，为所有残留 pending 生成 tickfile（carry-forward）
3. **所有 tickfile 相关清理在同一 lock scope 内完成**

```python
# Step 1: Force-generate remaining pending tickfiles BEFORE clearing
remaining_pending = []
with self._state.lock:
    remaining_pending = sorted(self._state._tickfile_pending.keys())
if remaining_pending:
    logger.warning(
        "Cross-day: generating %d pending tickfiles before cleanup (order lagging)",
        len(remaining_pending),
    )
    for mk in remaining_pending:
        try:
            self._try_generate_tickfile(mk)
        except Exception:
            logger.exception("Cross-day tickfile generation failed for minute=%s", mk)

# Step 2: Atomic cleanup in same lock scope
with self._state.lock:
    cleared_pending = len(self._state._tickfile_pending)
    self._state._tickfile_pending.clear()
    # Clean orphaned raw_order_buffers for yesterday's dates
    # (order thread may have written AFTER force-generate popped)
    orphaned_keys = [k for k in list(self._state.raw_order_buffers)
                     if k[:8] != current_date]
    # 注：这些 orphaned entries 可能由 order thread 在 force-generate IO 期间继续写入。
    # force-generate pop 后的写入由 catch-up scan 或下一轮 overflow 处理。
    # 此 cleanup 是最终兜底，确保跨日后不留昨日数据。
    for k in orphaned_keys:
        self._state.raw_order_buffers.pop(k, None)
    self._state._tickfile_seqno = 0  # ← replaces removed line 134
    self._state.order_current_minute = ""  # ← 显式重置
if cleared_pending > 0:
    logger.warning(
        "Cross-day cleared %d tickfile pending minutes that failed to generate",
        cleared_pending,
    )
```

**注意**: `raw_order_buffers` 的跨日 pop（flusher.py line 107-111）在**不同的 lock scope**（第一个 lock scope，IO 之前）。**当 `enable_tickfile=True` 时，必须跳过此 pop**，将 `raw_order_buffers` 留给 `_try_generate_tickfile` 在 force-generate 中消费。

跨日第一个 lock scope 修改：
```python
# In _step1_cross_day_check, first lock scope (line 107-111):
# BEFORE (Phase 16):
pending_orders = {
    k: self._state.raw_order_buffers.pop(k)
    for k in list(self._state.raw_order_buffers)
    if is_yesterday(k, current_date)
}

# AFTER (Phase 17, when enable_tickfile=True):
if not self._enable_tickfile:
    pending_orders = {
        k: self._state.raw_order_buffers.pop(k)
        for k in list(self._state.raw_order_buffers)
        if is_yesterday(k, current_date)
    }
else:
    # Skip pop — raw_order_buffers will be consumed by _try_generate_tickfile
    # during cross-day force-generate (Section 4.2.8 Step 1)
    pending_orders = {}  # Order data written by engine thread, not flusher
```

**原因**：`_try_generate_tickfile` 在 force-generate 中需要 pop `raw_order_buffers[X]` 获取真实 order 数据。如果第一个 lock scope 已 pop，force-generate 只能拿到空数据（carry-forward）。

#### 4.2.9 `flush_all_remaining` — 处理残余 pending

**位置**: `flush_all_remaining` (lines 313-341)

在 `_step4_handle_late_records()` 之后、`_write_checkpoint()` 之前新增：

```python
# PRECONDITION: All worker threads (data, clock, order) must be joined before calling.

# Generate tickfile for remaining pending minutes (EOF fallback)
tickfile_errors = 0
with self._state.lock:
    remaining_pending = sorted(self._state._tickfile_pending.keys())
for mk in remaining_pending:
    try:
        self._try_generate_tickfile(mk)
    except Exception:
        tickfile_errors += 1
        logger.exception("EOF tickfile generation failed for minute=%s", mk)
if remaining_pending:
    logger.info("EOF tickfile summary: %d generated, %d failed",
                len(remaining_pending) - tickfile_errors, tickfile_errors)

# After tickfile, assert cleanup
with self._state.lock:
    if self._state._tickfile_pending:
        # WARNING only (not FATAL) — if order thread didn't join cleanly,
        # pending may have entries that can never be generated
        logger.warning("Tickfile pending not empty after flush_all_remaining: %s "
                       "(may be caused by order thread join timeout)",
                       list(self._state._tickfile_pending.keys()))
        self._state._tickfile_pending.clear()
```

#### 4.2.10 `_reroute_buffer_to_late_queue` — handle `raw_order_buffers`

**位置**: `_reroute_buffer_to_late_queue` (flusher.py line 437-467)

**新增**: 当 `enable_tickfile=True` 时，reroute 路径也需要 pop `raw_order_buffers`，否则这些数据会无限保留在内存中（reroute 路径不经过 `_flush_minutes_internal` 的 pending 机制）。

```python
# In _reroute_buffer_to_late_queue, inside the existing single lock scope
# that already pops ohlcv/raw_snapshot/_snapshot_at_minute_end:
if self._enable_tickfile:
    # Pop raw_order_buffers + _tickfile_pending for rerouted minutes
    # Must be in same lock scope as ohlcv/raw_snapshot pops for atomicity
    order_buf = self._state.raw_order_buffers.pop(minute_key, None)
    pending_tick = self._state._tickfile_pending.pop(minute_key, None)
    if order_buf:
        logger.debug("Reroute: popped %d order records for minute=%s (no tickfile)",
                     len(order_buf), minute_key)
    if pending_tick:
        logger.debug("Reroute: cleared tickfile pending for minute=%s", minute_key)
```

### 4.3 `src/minute_bar/engine.py`

#### 4.3.1 Deferred tickfile trigger + order_current_minute timing fix

**关键时序修复**（Round 5 Critical fix）：`_flush_order_minute` 在 `for line in lines` 内部循环中被调用（engine.py line 514），此时 `pending_shared_orders` 的 batch write（line 533）尚未发生。如果在 `_flush_order_minute` 内更新 `order_current_minute`，clock thread 可能在 batch write 前看到 `order_current_minute >= Y`（Y 为当前 chunk 中的早期分钟），触发 tickfile 但 `raw_order_buffers[Y]` 不完整。

**修复**：`_flush_order_minute` 仅记录需触发的分钟。`order_current_minute` 更新和 tickfile trigger **都延迟到 batch write 完成后**。同时增加 catch-up 机制：更新 `order_current_minute` 后扫描 `_tickfile_pending`，发现任何 `minute <= order_current_minute` 的 pending 条目也加入触发列表。

**位置**: `_flush_order_minute` (lines 614-637) + `_order_loop` (lines 533-555)

```python
# In _flush_order_minute:
def _flush_order_minute(self, buffers, minute_key, output_dir) -> None:
    buf = buffers.pop(minute_key, None)
    if buf is None or not buf.records:
        return
    try:
        write_order_file(output_dir, minute_key, buf.records)
    except Exception as e:
        logger.fatal(...)
        raise
    with self._checkpoint_lock:
        self._committed_order_offset = buf.line_end_offset
    self._flushed_order_minutes.add(minute_key)
    logger.info(...)

    # NEW: Record minute for deferred tickfile trigger
    if self._config.output.enable_tickfile:
        # DO NOT update order_current_minute here — batch write hasn't happened yet
        # DO NOT trigger tickfile here — raw_order_buffers may not be complete yet
        self._tickfile_trigger_pending.append(minute_key)
```

**Drain helper method**（在 Engine 中提取为方法，避免重复代码）：

```python
def _drain_tickfile_triggers(self) -> None:
    """Drain _tickfile_trigger_pending: update order_current_minute + generate tickfiles.
    Called after batch write and after _flush_expired_order_minutes.
    """
    if not self._tickfile_trigger_pending:
        return
    triggers = list(self._tickfile_trigger_pending)
    self._tickfile_trigger_pending.clear()

    # Update order_current_minute AFTER batch write (all data now in raw_order_buffers)
    if triggers:
        latest = max(triggers)
        with self._state.lock:
            self._state.order_current_minute = latest
            # Catch-up: find pending tickfiles where order data is now complete
            # This handles Case B gap: snapshot flushed after order drain but
            # before order moved to next minute
            for mk in list(self._state._tickfile_pending):
                if mk <= latest and mk not in triggers:
                    triggers.append(mk)

    # Generate tickfiles (deferred triggers + catch-up keys)
    for mk in triggers:
        try:
            self._flusher._try_generate_tickfile(mk)
        except Exception:
            logger.exception("Tickfile generation failed for minute=%s [thread=%s]",
                             mk, threading.current_thread().name)
```

**Drain call sites — 在 `_order_loop` 中三处调用**：

```python
# Drain point 1: After batch write (line 548: pending_shared_orders.clear())
if self._config.output.enable_tickfile:
    self._drain_tickfile_triggers()

# _flush_expired_order_minutes runs next (line 550-552):
self._flush_expired_order_minutes(buffers, output_dir, current_minute)

# Drain point 2: After _flush_expired_order_minutes (inner while loop)
if self._config.output.enable_tickfile:
    self._drain_tickfile_triggers()

# ... inner while loop may exit (lines empty) ...
# Drain point 3: After _flush_expired_order_minutes (post inner-loop, line 565-567)
# This handles the case where _flush_expired_order_minutes is called after the
# inner while loop exits (before checkpoint write). Without this drain, triggers
# accumulated during this call would be delayed until the next outer loop iteration.
self._flush_expired_order_minutes(buffers, output_dir, output_delay_sec, current_minute or "")
if self._config.output.enable_tickfile:
    self._drain_tickfile_triggers()
```

**注**：Drain point 3 的延迟不会导致数据丢失（`_tickfile_trigger_pending` 保留在列表中），但可能导致 tickfile 生成延迟一个外层循环迭代（通常 < 100ms）。添加显式 drain 确保时序一致性。

**Engine `__init__` 新增字段**：
```python
self._tickfile_trigger_pending: list = []  # order-thread only — do not access from other threads
```

**时序保证**：
1. `write_order_file` 成功 → `_flushed_order_minutes.add` → `_tickfile_trigger_pending.append`
2. batch write → `raw_order_buffers` 完整
3. `_drain_tickfile_triggers` → `order_current_minute = max(all flushed)` → catch-up scan → tickfile IO

当 `_drain_tickfile_triggers` 更新 `order_current_minute = X` 时，`raw_order_buffers[X]` 已包含全部记录（batch write 在步骤 2 完成）。Clock thread 看到 `order_current_minute > Y` 时，`raw_order_buffers[Y]` 也已完整（Y < X，Y 的数据在之前的 batch write 中写入）。

**Catch-up 机制**：当 clock thread 在 order drain 后、order 进入下分钟前 flush snapshot X（`order_current_minute == X`），clock thread 的 `> X` 条件为 False 不触发。但下一轮 order batch write 的 catch-up scan 发现 `X <= order_current_minute`，将其加入触发列表。

**Order thread I/O 延迟**: `_try_generate_tickfile` 在 order thread 上执行文件 IO。典型耗时 <100ms（~4500 symbols）。如果输出目录在慢存储上可能更长。`_try_generate_tickfile` 内部有 >200ms 的 WARNING 日志。如果实测影响 order 吞吐量，可改为 enqueue 到 clock thread 的 queue 中。

**⚠️ 已知 I/O 放大问题（pre-existing）**：`write_tickfile_rows` 的 append 路径在每次写入时通过 `f.readlines()` 读取**整个** daily tickfile 来检查截断行。在交易日后期（~1.5M 行 / ~300MB 文件），每次 tickfile 生成都需要全文件读取。这是 Phase 16 遗留问题，非本 spec 引入。本 spec 将 tickfile IO 放到 order thread 上，使得此问题更显著。**建议**：在 Phase 17 实施后尽快优化 writer.py，将 `readlines()` 替换为 `seek(-N, 2)` 尾部检查。此优化不阻塞 Phase 17 上线（功能正确性不受影响，仅性能）。

#### 4.3.2 Modify `raw_order_buffers` write condition

**位置**: line 543

```python
# AFTER:
if mk not in self._state.flushed_snapshot_minutes or mk in self._state._tickfile_pending:
    self._state.raw_order_buffers.setdefault(mk, []).append(rec)
```

**语义**:
- `mk not in flushed_snapshot_minutes` → True：snapshot 未 flush，正常写入（原始行为不变）
- `mk in flushed_snapshot_minutes` AND `mk in _tickfile_pending` → True：snapshot 已 flush 但 tickfile 等待 order，继续写入（新行为）
- `mk in flushed_snapshot_minutes` AND `mk not in _tickfile_pending` → False：snapshot 已 flush 且 tickfile 已生成（或 tickfile 未启用），跳过

**`enable_tickfile=False` 时的简化**: `_tickfile_pending` 始终为空 → 条件简化为 `mk not in flushed_snapshot_minutes`，与 Phase 16 完全一致。

**原子性保证**: `_tickfile_pending` 和 `flushed_snapshot_minutes` 的 add 在同一个 lock scope 内（`_flush_minutes_internal` 第二个 lock scope），因此 order 线程看到 `mk in flushed_snapshot_minutes` 时，`mk in _tickfile_pending` 的状态也已确定。

#### 4.3.3 `_flush_all_order_buffers` — 不需要额外改动

每个 `_flush_order_minute` 调用已包含 tickfile trigger，无需额外改动。

#### 4.3.4 Cross-day: `order_current_minute` + `_flushed_order_minutes` 在 order thread 跨日时重置

**位置**: `_order_loop` 跨日 try-finally (lines 472-483)

在 `current_date = record_date` 赋值后，重置 SharedState：

```python
if current_date is not None and record_date != current_date:
    try:
        self._flush_all_order_buffers(buffers, output_dir)
    except Exception:
        logger.exception("Cross-day order flush failed, resetting date anyway")
    finally:
        old_keys = [k for k in buffers if k[:8] != record_date]
        for k in old_keys:
            buffers.pop(k, None)
    current_date = record_date
    current_minute = None
    # NEW: Reset order progress for cross-day
    if self._config.output.enable_tickfile:
        with self._state.lock:
            self._state.order_current_minute = ""
        # Drain any remaining tickfile triggers from previous day
        self._drain_tickfile_triggers()
    # NEW: Clear _flushed_order_minutes to prevent stale entries
    # Note: outside lock — safe because _flushed_order_minutes is order-thread-only (Invariant 5)
    self._flushed_order_minutes.clear()
```

### 4.4 `src/minute_bar/replay.py` — NO CHANGES（writer.py raise 除外）

Replay 模式不需要同步机制：
- `_stream_orders` 在 `_flush_snapshot_minute` 之前写入 `raw_order_buffers`
- `DELAYED_FLUSH_ROUNDS=3` 确保 order 数据在 snapshot flush 时已到位
- Tickfile 同步问题仅存在于 live 多线程模式

Replay 的 `_flush_snapshot_minute` (lines 250-262) 保持不变，直接 pop `raw_order_buffers` 生成 tickfile。

**writer.py IOError 影响**：Section 4.2.6 将 `write_tickfile_rows` 的两条 silent return 路径改为 `raise IOError`。Replay 的 `_flush_snapshot_minute`（line 262）也调用 `write_tickfile_rows`。在正常 replay 场景下，这两条路径不会被触发（replay 输入为预写好的 clean 文件）。但为防御性编程，建议在 replay 的 tickfile 调用处添加 `try/except IOError` 记录日志后继续。此改动不计入 Section 7 的文件修改列表（防御性改动，非功能需求）。

**Replay vs Live 行为差异**：Replay 直接在 snapshot flush 时生成 tickfile（因为 order 数据已到位）。Live 延迟到 order flush 后生成。两者最终输出相同（相同的 snapshot + order 数据 → 相同的 tickfile 内容），仅生成时机不同。这是有意设计：replay 用作回归 oracle 验证输出一致性，不验证生成时序。Replay 测试套件仍需运行以确认无回归。

## 5. Edge Cases

### 5.1 Order 线程 batch-write 时序

```
Order thread inner while loop:
  for line in lines:
    pending_shared_orders.append((mk, rec))     ← 积累
    if minute boundary detected:
      _flush_order_minute(current_minute)       ← 写 order file + record in _tickfile_trigger_pending
                                                   (NOT updating order_current_minute here)
  batch write to SharedState (line 533)          ← 写入 raw_order_buffers（包含当前 chunk 全部记录）
  pending_shared_orders.clear()
  _drain_tickfile_triggers()                     ← drain 1: 更新 order_current_minute + catch-up scan + tickfile IO
  _flush_expired_order_minutes (line 550)         ← 写 order file + record in _tickfile_trigger_pending
  _drain_tickfile_triggers()                     ← drain 2: 同上
  ... inner while loop may exit (lines empty) ...
  _flush_expired_order_minutes (line 565)         ← 写 order file + record in _tickfile_trigger_pending
  _drain_tickfile_triggers()                     ← drain 3: 同上（post inner-loop）
```

**关键时序保证**：
1. `order_current_minute` 仅在 `_drain_tickfile_triggers` 中更新（batch write 之后）
2. tickfile trigger 在 batch write **之后**执行，`raw_order_buffers[X]` 已包含 X 的全部记录
3. catch-up scan 确保即使 snapshot 在 order drain 后到达，也能在下一轮 drain 中触发
4. Clock thread 的 `>` 条件配合 batch-write-first 更新：`order_current_minute > Y` 时，`raw_order_buffers[Y]` 一定完整

### 5.2 内存控制

`_tickfile_pending` 和对应的 `raw_order_buffers` 在内存中保留，直到 tickfile 生成。

**估算**（基于 CPython 3.11+，实际因 Python 实现和系统分配器而异）：
- 每分钟 ~170K order records × ~280 bytes（frozen dataclass with `__dict__` overhead）= ~47.6 MB/minute
- `_tickfile_pending` 本身：每分钟 ~4500 symbols × ~500 bytes/symbol ≈ ~2.25 MB/minute
- 5 pending minutes = ~250 MB peak
- 10 pending minutes (MAX cap) = ~500 MB peak

**注**：若 `OrderRecord` 使用 `__slots__`，每实例可减少 ~100 bytes，降至 ~180 bytes → 10 pending = ~320 MB。是否优化取决于实测内存压力。

**保护机制**：`MAX_TICKFILE_PENDING_MINUTES = 10`。当 pending 超过上限时，强制用 carry-forward 生成最旧 pending 分钟的 tickfile。检查发生在 `tick()` 方法中（每秒一次）。

**Stall flush 协同**：现有 stall flush（`_stall_flush_sec`）在 watermark 停滞时 flush ohlcv buffers。Stall flush 走 `_flush_minutes_internal`，其中会存储 `_tickfile_pending`。Stall flush 后，pending 仍需等待 order thread 或 EOF。`MAX_TICKFILE_PENDING_MINUTES` 确保不会无限积累。

### 5.3 Cross-day 清理

跨日时 `_step1_cross_day_check` 清理：
1. **Force-generate**：在 clear 之前，为所有残留 pending 调用 `_try_generate_tickfile`（carry-forward）
2. 原子清理（同一 lock scope）：`_tickfile_pending.clear()` + `_tickfile_seqno = 0` + `order_current_minute = ""`
3. Order thread 跨日时也重置 `order_current_minute = ""` + `_flushed_order_minutes.clear()`（双保险）
4. 生成失败（force-generate 抛异常）的 pending 被 clear 丢弃（等同 Phase 16 行为）
5. 日志：clear 前记录 pending 数量，clear 后记录残留数量

### 5.4 Crash Recovery

Crash 后重启：
- `recover_tickfile_seqno_lazy` 从文件恢复 seqno（在 flusher 构造时执行，单线程，无竞态）
- `_tickfile_pending` 是内存结构，crash 后丢失 → 这几个分钟的 tickfile 不会被生成
- 但 snapshot/order 文件已写入 → 可通过 replay 重生成 tickfile
- `raw_order_buffers` 同理，crash 后丢失（设计如此，等同 Phase 16 行为）
- **启动审计**：启动时比较 snapshot 文件数 vs tickfile seqno 数，不一致则 WARNING
- **`.tmp` 文件清理**：启动时检测并删除残留 `.tmp` 文件（上一次写入中途 crash 的产物）
- **Late order 与 tickfile**：Late orders（order thread 的 `_flushed_order_minutes` 已包含该分钟后到达的 order）走 late path，不进入 `raw_order_buffers`。Tickfile 使用 `latest_order_by_symbol` carry-forward 处理 late orders。这是已知限制，等同 Phase 16 行为。

### 5.5 `enable_tickfile=False`

当 tickfile 未启用时：
- `_flush_minutes_internal` 中 `raw_order_buffers` 正常 pop（等同 Phase 16 Bug 2 修复前行为）
- `_tickfile_pending` 始终为空 → 所有新增代码不触发
- `raw_order_buffers` 写条件简化为 `mk not in flushed_snapshot_minutes`（`_tickfile_pending` 为空）
- 零影响
- `enable_tickfile` 从 config 读取，运行时不可变

### 5.6 Daily Tickfile 行序

由于 Case A 和 Case B 允许不同线程并发触发不同分钟的 tickfile 生成，daily tickfile 中的行物理顺序可能与分钟时间顺序不同。

**保证**：
- Seqno 单调递增（SharedState lock 内分配）
- 每行的 seqno 正确对应其分钟

**不保证**：
- 文件中行的物理顺序与 seqno 顺序一致

**下游要求**：消费者应按 seqno 排序而非依赖文件物理位置。这是向后兼容的（Phase 16 的 seqno 也是单调的）。

### 5.7 Config Validation

`enable_tickfile=True` 要求 `enable_order=True`。在 flusher 构造时检查（使用 `ValueError`，不用 `assert`，确保 `-O` 模式下仍生效）：

```python
if not self._enable_order:
    raise ValueError("enable_tickfile=True requires enable_order=True")
```

原因：当 `enable_tickfile=True` 时，flusher 不写 order file（`_write_minute_files` 跳过 order 写入）。Order file 由 engine 的 `_flush_order_minute` 写入。如果 `enable_order=False`，order file 不会被写入，且 `raw_order_buffers` 不会被 engine pop，导致内存泄漏。

### 5.8 Lock Ordering

**INVARIANT**: 所有嵌套锁获取必须遵循以下顺序：
1. `SharedState.lock`（先）
2. `_checkpoint_lock`（后，如果需要）

**绝不反序**。当前代码路径验证：
- `_flush_order_minute`：先 `checkpoint_lock`（release）→ 后 `state.lock` → 无嵌套
- `_try_generate_tickfile`：仅 `state.lock` → 无嵌套
- `_flush_minutes_internal`：`state.lock` → 无 `checkpoint_lock`

## 6. Testing Plan

### 6.1 Unit Tests: `tests/test_tickfile_sync.py` (new file)

| # | 测试名 | 场景 | 验证 |
|---|--------|------|------|
| 1 | `test_snapshot_first_order_triggers` | Snapshot flush → pending → order flush → tickfile | raw_order_buffers 完整，seqno 正确 |
| 2 | `test_order_first_clock_triggers` | Order flush (no pending) → snapshot flush → check order passed → tickfile | clock thread 触发路径 |
| 3 | `test_eof_fallback_carry_forward` | Snapshot flush → pending → EOF (order never arrives) | tickfile 用 carry-forward 生成 |
| 4 | `test_pending_cleanup_after_generation` | 生成后 _tickfile_pending 为空 | 无内存泄漏 |
| 5 | `test_seqno_shared_between_threads` | Clock thread 生成 seqno=1, order thread 生成 seqno=2 | seqno 单调递增 |
| 6 | `test_raw_order_buffers_condition` | flushed + pending → 允许写入; flushed + not pending → 跳过 | 条件正确 |
| 7 | `test_cross_day_clears_pending_and_order_minute` | 跨日 → force-generate pending → clear → _tickfile_pending 清空 + order_current_minute 重置 + orphaned raw_order_buffers 清除 | 清理正确 |
| 8 | `test_no_tickfile_no_change` | enable_tickfile=False → 现有行为不变 | 零回归 |
| 9 | `test_double_trigger_safe` | 两个线程同时检查同一分钟 → 只生成一次（使用 threading.Barrier 同步） | pop 原子性 |
| 10 | `test_order_file_not_double_written` | enable_tickfile=True → flusher 不写 order file | 消除冗余写入 |
| 11 | `test_io_failure_reinserts_all_data` | write_tickfile_rows raises → pending + raw_order_buffers re-inserted → flush_all_remaining retry | 失败恢复 |
| 12 | `test_pending_overflow_forces_oldest` | 15 pending minutes → force oldest 6 → count drops to ≤10 | MAX cap 生效 |
| 13 | `test_first_minute_no_carry_forward` | 日首分钟无历史 carry-forward → tickfile 用 snapshot-only | 边界 |
| 14 | `test_minute_zero_orders` | snapshot 有数据但 order_records=[] → carry-forward 正确 | 空 order |
| 15 | `test_tickfile_failure_no_block_order_flush` | mock write raises → order file 仍正常写入 | 异常隔离 |
| 16 | `test_seqno_recovery_at_init` | 预存 tickfile seqno=50 → 下一个 seqno=51 | 恢复正确 |
| 17 | `test_order_current_minute_updated_after_batch` | flush order X → batch write → drain → order_current_minute == X | 更新时机：batch write 后 |
| 18 | `test_cross_day_force_generates_pending` | 5 pending minutes → cross-day → all 5 tickfiles generated before clear | 跨日不丢数据 |
| 19 | `test_reroute_pops_raw_order_buffers_and_pending` | Snapshot reroute → raw_order_buffers + _tickfile_pending for rerouted minute both popped | 无内存泄漏 |
| 20 | `test_post_write_missing_file_warns_only` | os.path.exists returns False → ERROR log only, no re-insert | 验证不重复 |
| 21 | `test_seqno_gap_on_empty_selection` | select_tickfile_records returns empty → seqno incremented but no file | seqno gap acceptable |
| 22 | `test_stall_flush_creates_pending_then_order_generates` | Watermark stalls → stall flush → pending created → order catches up → tickfile generated | Stall flush 交互 |
| 23 | `test_tickfile_row_ordering_in_daily_file` | Two threads trigger for different minutes → file may have out-of-order rows but seqno is monotonic | 行序容忍 |
| 24 | `test_flushed_order_minutes_cleared_cross_day` | Cross-day in order thread → _flushed_order_minutes cleared | 无内存泄漏 |
| 25 | `test_config_validation_tickfile_requires_order` | enable_tickfile=True + enable_order=False → ValueError (not assert) | 配置校验 |
| 26 | `test_drain_tickfile_triggers_catchup` | Snapshot X flushes after order drain but before next order → catch-up scan finds X ≤ order_current_minute → tickfile generated | Case B gap 填补 |
| 27 | `test_cross_day_orphaned_raw_order_buffers_cleaned` | Order thread writes yesterday raw_order_buffers after force-generate → cleanup lock scope removes them | 跨日无泄漏 |
| 28 | `test_writer_corrupt_header_raises` | write_tickfile_rows encounters corrupt header → raises IOError (not silent return) | writer 契约 |
| 29 | `test_drain_after_flush_expired_order_minutes` | _flush_expired_order_minutes flushes minutes → _drain_tickfile_triggers called → tickfiles generated | 第二 drain 点 |
| 30 | `test_clock_thread_no_trigger_when_order_eq_minute` | order_current_minute == minute_key → clock thread `>` condition False → NO trigger → catch-up fires on next drain | `>` vs `>=` 边界 |
| 31 | `test_writer_all_rows_failed_raises` | write_tickfile_rows called with rows that all fail build_tickfile_row → raises IOError (not silent return) | writer 契约路径 2 |

### 6.2 Existing Test Regression

所有 279+ 个现有测试必须通过。重点检查：
- `test_tickfile.py` — tickfile 字段映射不变
- `test_writer.py` — tickfile I/O 不变
- `test_flusher.py` — flusher 基本流程
- `test_engine_late.py` — order late detection 不受影响
- Replay 全套测试 — 确认 replay 路径不受影响

### 6.3 End-to-End Live Test

```bash
# Terminal 1: Simulator
PYTHONPATH=src python -m data_simulator --speed 5 --order-speed 100 \
  --source-dir input --output-dir test/output --date 20260525

# Terminal 2: Minute bar
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini
```

**验证项**:
| 检查 | 预期 | 验证方法 |
|------|------|---------|
| Tickfile bid/ask 来源 | **真实 order**（非 carry-forward） | 解析 tickfile，检查 UpdateTime 在目标分钟窗口内 |
| raw_order_records 覆盖率 | **>99%**（vs Phase 16 的 95.8% carry-forward） | 统计非 NA bidprice 行数 / 总行数 |
| Seqno 数量 | == snapshot 文件数（动态计算，非硬编码） | `wc -l` vs snapshot count |
| Order 文件 | 数量与 Phase 16 一致 | `diff` 对比 |
| Snapshot 文件 | 内容不变 | `diff` 对比 |
| Pending depth log | 无 > 10 的 WARNING | 检查日志 |
| Tickfile generation thread | Case A（order thread）为主要路径 | 检查日志中 thread= 字段 |
| Replay tickfile 输出一致性 | Live 与 Replay tickfile 内容相同 | `diff` Phase 17 live tickfile vs replay tickfile（同输入数据） |

## 7. Files Changed

4 个文件修改 + 1 个新文件 + 1 个已审阅确认不改（replay.py）。

| 文件 | 改动类型 | 改动量 | 说明 |
|------|---------|--------|------|
| `src/minute_bar/aggregator.py` | 修改 | +7 行 | SharedState 新增 3 个字段 + 注释 |
| `src/minute_bar/flusher.py` | 修改 | ~90 行 | 解耦 tickfile + `_try_generate_tickfile` + overflow + EOF + 跨日 + reroute + config validation + orphaned cleanup |
| `src/minute_bar/engine.py` | 修改 | ~45 行 | `_drain_tickfile_triggers` helper + 条件修改 + `_tickfile_trigger_pending` 字段 + 跨日重置 + drain call sites |
| `src/minute_bar/replay.py` | 不改 | 0 | Replay 不需要同步机制（参见 Section 4.4）。已审阅确认：`_stream_orders` 先于 `_flush_snapshot_minute` 写入 `raw_order_buffers`，`DELAYED_FLUSH_ROUNDS=3` 保证数据到位。 |
| `src/minute_bar/writer.py` | 修改 | ~5 行 | `write_tickfile_rows` header corrupted + all-rows-failed 路径改为 raise IOError（非 silent return） |
| `tests/test_tickfile_sync.py` | 新建 | ~490 行 | 31 个同步专用测试 |

## 8. INVARIANTS Summary

| # | Invariant | 强制方式 |
|---|-----------|---------|
| 1 | `_tickfile_pending` 数据在 tickfile 生成后立即清除 | `_try_generate_tickfile` 中 `.pop()` |
| 2 | `raw_order_buffers[minute_key]` 仅在 tickfile 生成时 pop（enable_tickfile=True 时） | flusher 中 skip pop when enable_tickfile |
| 3 | Order 线程对 `_tickfile_pending` 中的分钟继续写 `raw_order_buffers` | 条件 `not flushed or in pending` |
| 4 | `_tickfile_seqno` 单调递增（同一日内），跨日重置为 0 | SharedState lock 内递增，recovery 仅在 init 时执行一次 |
| 5 | `_flushed_order_minutes` 保持 engine local（仅 order 线程 late 检测） | 不变。跨日时 `.clear()` 防止内存泄漏 |
| 6 | Tickfile 生成失败不影响 order/snapshot 文件写入 | `try/except` best-effort + pending + raw_order_buffers re-insert |
| 7 | `enable_tickfile=False` 时所有新代码不触发 | 所有新代码 gated by `enable_tickfile` |
| 8 | `order_current_minute` 仅在 `_drain_tickfile_triggers` 内更新（batch write 之后），不在 `_flush_order_minute` 内更新 | `_drain_tickfile_triggers` 代码位置强制 |
| 9 | `order_current_minute` 跨日时显式重置为 `""`（flusher + engine 双保险） | 两处清理代码 |
| 10 | `_tickfile_pending` 不超过 `MAX_TICKFILE_PENDING_MINUTES=10` | tick() 中 overflow check（单次 lock scope） |
| 11 | `flush_all_remaining` 后 `_tickfile_pending` 应为空（WARNING only，不 FATAL） | WARNING log + clear |
| 12 | `_try_generate_tickfile` IO 失败时 re-insert pending + raw_order_buffers，确保 EOF 可重试 | except block re-insert both |
| 13 | `_tickfile_pending` entry 和 `flushed_snapshot_minutes.add()` 在同一 lock scope，确保 order thread 的复合条件一致性 | 同一 lock scope |
| 14 | 跨日清理前先 force-generate 所有残留 pending（最小化 tickfile 丢失） | `_step1_cross_day_check` 中 force-generate before clear |
| 15 | Seqno gap 是 acceptable 的（IO 失败重试、empty selection 时产生） | 设计决策，消费者按 seqno 排序 |
| 16 | Lock ordering: `state.lock` 在 `checkpoint_lock` 之前（如果需要嵌套）。绝不反序 | 代码路径审阅 |
| 17 | `enable_tickfile=True` 要求 `enable_order=True` | 构造时 `if not ... raise ValueError` |
| 18 | `_reroute_buffer_to_late_queue` 在 enable_tickfile=True 时同时 pop `raw_order_buffers` 和 `_tickfile_pending` | 同一 lock scope 内两个 pop |
| 19 | Tickfile trigger 仅在 batch write 完成后执行（`_drain_tickfile_triggers`），不在 `_flush_order_minute` 内部 | `_tickfile_trigger_pending` deferred list |
| 20 | 跨日第一个 lock scope 中 `raw_order_buffers` pop 在 `enable_tickfile=True` 时跳过 | 条件跳过 |
| 21 | 跨日 cleanup lock scope 清除所有昨日 `raw_order_buffers` 孤立条目 | 第二 lock scope 中 `k[:8] != current_date` 过滤 |
| 22 | `_drain_tickfile_triggers` 在 batch write 后和每次 `_flush_expired_order_minutes` 后均被调用（三处 drain points） | 三处 drain call sites |
| 23 | `_drain_tickfile_triggers` 更新 `order_current_minute` 后执行 catch-up scan（`_tickfile_pending` 中 `mk <= order_current_minute`） | catch-up scan 代码 |
| 24 | `write_tickfile_rows` 在遇到不可恢复错误（corrupt header + all rows failed to build）时必须 raise，不能 silent return | writer.py 修改（两条路径） |
| 25 | `_tickfile_trigger_pending` 仅 order thread 访问（单写者），不需要线程同步 | 代码注释 + Engine 字段 |

## 9. Migration & Rollback

### 9.1 Rollout

- **无 config 变更**：Phase 17 使用现有 `enable_tickfile` 配置项
- **已有 `enable_tickfile=true` 的环境**：Phase 17 自动从同步生成切换为双线程 Join 生成，无需操作
- **行为变更对操作员透明**：输出文件格式、seqno 语义、文件数量均不变
- **推荐**：在上线前运行 Section 6.3 的 E2E live test

### 9.2 Rollback

- 停止 engine → 回滚代码到 Phase 16 → 重启
- Phase 17 写入的 tickfile 文件是有效的，Phase 16 代码可正常追加
- `_tickfile_pending` 是纯内存结构，回滚后不存在 → 丢失的是未生成的 tickfile（等同 Phase 16 行为）
- Checkpoint 格式不变（新增字段全是内存态，不持久化）→ Phase 16 代码可正常读取 checkpoint

### 9.3 Compatibility

- Phase 16 → Phase 17：无缝升级（所有 tickfile 字段兼容）
- Phase 17 → Phase 16：无缝回退（tickfile 文件格式兼容）
- 跨日 seqno 重置行为不变（Phase 16 和 Phase 17 都重置为 0）
