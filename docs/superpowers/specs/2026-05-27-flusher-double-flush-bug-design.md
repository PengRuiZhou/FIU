# Flusher Double-Flush Bug — Snapshot 分钟被覆盖

日期：2026-05-27
状态：待修复
来源：Snapshot drain loop 端到端验证发现
关联：snapshot-drain-loop-design（Phase 15 验证）

## 1. 问题描述

Snapshot drain loop 端到端验证（data_simulator speed=100）中，发现 `ClockWatermarkFlusher` 对 snapshot 1524 分钟执行了两次 flush，第二次覆盖了第一次的正确结果。

### 端到端验证结果

| Minute | Live Y | Replay Y | 状态 |
|--------|--------|----------|------|
| 1128 | 10,811 | 10,811 | MATCH（drain loop 修复生效） |
| 1129 | 12,201 | 12,201 | MATCH（drain loop 修复生效） |
| 1524 | 107 | 42,868 | **FAIL（double-flush 覆盖）** |
| 其余 326 分钟 | 一致 | 一致 | MATCH |
| Order 417 分钟 | 一致 | 一致 | MATCH |

### Engine 日志时间线（第二次运行）

```
16:23:13 Data-driven flush: 1 minutes (watermark=202605251524)
16:23:16 Wrote snapshot_minute_20260525_1524.csv (44,588 rows) ← 第一次 flush，正确！
16:23:26 Flushed 2505 late snapshot records across 186 minutes  ← Step 4 late record 处理
16:23:27 Skipped 3,779 carry-forward rows with future timestamps in 1524
16:23:27 Wrote snapshot_minute_20260525_1524.csv (764 rows)    ← 第二次 flush，覆盖！
```

**第一次 flush**：44,588 rows（与 replay 44,695 接近，差异来自 late records）
**第二次 flush**：764 rows（仅含 drain loop 写文件期间到达的新数据 + carry-forward），覆盖了正确的 44,588 rows

两次运行（13:55 和 16:17 启动）均复现，模式完全一致。

## 2. 根因分析

### 2.1 竞态条件：`flushed_snapshot_minutes` 更新时机

`_flush_minutes_internal`（flusher.py L204-L283）的关键流程：

```python
def _flush_minutes_internal(self, minute_keys, *, is_final=False):
    with self._state.lock:                           # ← Lock block 1
        ohlcv_data = {k: self._state.ohlcv_buffers.pop(k) for k in minute_keys}
        raw_data = {k: self._state.raw_snapshot_buffers.pop(k) for k in minute_keys}
        # ... pop all buffer data ...
        snapshot_copy = dict(self._state.latest_snapshot)
    # ← Lock released here

    for minute_key in minute_keys:                   # File writes OUTSIDE lock
        data = ohlcv_data.get(minute_key)
        self._write_minute_files(...)                # ← Takes ~3 seconds for 44K rows!

        with self._state.lock:                       # ← Lock block 2
            self._state.flushed_snapshot_minutes.add(minute_key)  # ← TOO LATE!
```

**竞态窗口**：从 Lock block 1 释放到 Lock block 2 的 `flushed_snapshot_minutes.add()` 之间，存在 ~3 秒的窗口（44K rows 的文件写入耗时）。

### 2.2 竞态时序

```
Clock-thread (flusher)              Data-thread (_data_loop)
========================            ========================

Lock block 1:
  pop ohlcv_buffers[1524]           process_snapshot(1524 record):
  pop raw_snapshot_buffers[1524]      lock:
Release lock                            1524 in flushed_snapshot_minutes? → NO
                                        → put in ohlcv_buffers[1524] (normal path)
                                      unlock

_write_minute_files(1524)           process_snapshot(1524 record):
  (takes ~3 seconds)                  lock:
                                        1524 in flushed_snapshot_minutes? → NO
                                        → put in ohlcv_buffers[1524]
                                      unlock

Lock block 2:
  flushed_snapshot_minutes.add(1524)

Next tick:
  _step3_minute_output:
    1524 in ohlcv_buffers? → YES (from race window)
    is_data_driven_expired(1524, 1530)? → YES
    → _flush_minutes_internal([1524]) → OVERWRITES with fewer rows!
```

### 2.3 为什么只影响 1524

1524 是 post-market 最后一个有 snapshot 数据的分钟。watermark 从 1524 直接跳到 1530（1525-1529 无 snapshot 数据）。这使得：
1. 1524 是 flusher 在 `watermark=1530` 下第一个检测到过期的分钟
2. 1524 的 flush 写入量大（44K rows），竞态窗口长（~3 秒）
3. drain loop 在此窗口内继续读取 1524 数据（data_simulator speed=100 下 1524 数据仍在写入）

其他分钟（如 1128/1129）不受影响，因为它们的 flush 写入量小（~10K-12K rows，< 1 秒），竞态窗口内 data_simulator 已无该分钟的新数据。

### 2.4 生产环境影响

生产环境 speed=1 下：
- 数据实时到达，每分钟 ~60 秒写入
- 文件写入耗时极短（毫秒级）
- 竞态窗口内新数据到达概率极低
- **理论上可能触发**（极端情况下 data thread 和 clock-thread 竞争），但实际概率极低

**定量分析**：1524 分钟的 flush 在 speed=1 下约需 3 秒（44K rows）。生产环境中 1524 的数据在 15:24:00-15:24:59 到达，flush 在 15:25:00 后触发（watermark advance 到 1525+），此时 1524 的新数据已基本停止到达。竞态窗口内新数据到达的唯一可能是数据源延迟（网络延迟、文件系统缓冲），通常仅个位数到数十条记录。

### 2.5 为什么 `_step3_minute_output` 不过滤已 flush 的分钟

```python
def _step3_minute_output(self):
    with self._state.lock:
        expired_keys = sorted(
            k for k in self._state.ohlcv_buffers
            if is_data_driven_expired(k, data_watermark, ...)
        )
```

`_step3_minute_output` 只检查 `ohlcv_buffers` 中哪些分钟已过期（基于 watermark），**不检查** `flushed_snapshot_minutes`。即使 1524 已被 flush 过，如果 buffer 中又有新数据（竞态窗口内到达），它会被再次 flush。

## 3. 影响范围

| 组件 | 影响 |
|------|------|
| `flusher.py` `_flush_minutes_internal` | 根因所在 |
| `aggregator.py` `process_snapshot` | 竞态另一端（数据线程） |
| `writer.py` `write_snapshot_file` | carry-forward 过滤正常工作（"Skipped 3779 carry-forward rows"） |
| Order 文件 | **无影响** — order flush 由 `_order_loop` 独立处理 |
| Replay 模式 | **无影响** — replay 不使用 flusher |
| 生产环境 speed=1 | 理论上可能但实际概率极低 |

## 4. 安全约束

- 不能在文件写入时持有 `SharedState.lock`（会阻塞 data thread 数秒）
- 不能丢失正常路径和 late 路径的任何记录
- 必须保证 carry-forward 时间过滤继续正确工作
- `_step3_minute_output` 的 expired_keys 检测逻辑不应被绕过
- `flushed_snapshot_minutes.add()` 必须在文件写入成功之后，不在之前（避免写入失败时误标记）

## 5. 不做的事

- 不改 `process_snapshot` 的 late detection 逻辑（已正确）
- 不改 `write_snapshot_file` 的 carry-forward 过滤（已正确）
- 不引入新的锁或改变锁的粒度
- 不改 `_step4_handle_late_records`（late record append 路径正确）
- 不提前标记 `flushed_snapshot_minutes`（不改 `_flush_minutes_internal` 的标记时机）
- **不改 `_flush_minutes_internal` 的 stall flush / cross-day flush 路径**：
  - stall flush 触发时意味着 data-thread 已停止（watermark 停滞 ≥ `stall_flush_sec`），不会有新数据写入 buffer，竞态窗口不存在
  - cross-day flush 在 `tick()` 内串行执行（step1 → step3），不存在与 `_flush_minutes_internal` 的并发
  - 两条路径均无需分流保护
  - **注意**：stall flush（L181-185）和 cross-day flush（L92-106）是 `_step3_minute_output` 之外的独立分支，直接调用 `_flush_minutes_internal` 而不经过 `already_flushed` 分流逻辑。因为两者触发时不存在竞态窗口，分流保护不适用

## 6. 修复方案：Step3 Late Re-route

### 6.1 核心思路

在 `_step3_minute_output` 中，将 `expired_keys` 分为两组：

- **normal_keys**：不在 `flushed_snapshot_minutes` 中的分钟 → 正常 flush
- **reflush_keys**：已在 `flushed_snapshot_minutes` 中但 buffer 中仍有数据的分钟 → 将 buffer 数据重新路由到 late queue

这样，竞态窗口内到达的数据不会被再次正常 flush（覆盖文件），而是通过 late queue 由 step 4 append 到已有文件。

### 6.2 改动 1：`_step3_minute_output` 分流

```python
def _step3_minute_output(self) -> None:
    try:
        with self._state.lock:
            data_watermark = self._state.current_minute
            expired_keys = sorted(
                k for k in self._state.ohlcv_buffers
                if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
                   or (self._enable_time_fallback and is_expired(k, self._output_delay_sec))
            )
            data_driven_keys = [
                k for k in expired_keys
                if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
            ]
            fallback_keys = [k for k in expired_keys if k not in data_driven_keys]
            # NEW: Check already-flushed in same lock block
            already_flushed = {k for k in expired_keys
                               if k in self._state.flushed_snapshot_minutes}
    except ValueError:
        logger.exception("...")
        return

    # ... stall detection (unchanged) ...

    if expired_keys:
        # NEW: Split into normal vs already-flushed
        normal_keys = [k for k in expired_keys if k not in already_flushed]
        reflush_keys = [k for k in expired_keys if k in already_flushed]

        if normal_keys:
            normal_data_driven = [k for k in normal_keys if k in data_driven_keys]
            normal_fallback = [k for k in normal_keys if k in fallback_keys]
            if normal_data_driven:
                logger.info("Data-driven flush: %d minutes (watermark=%s)",
                            len(normal_data_driven), data_watermark)
            if normal_fallback:
                logger.warning("Time-fallback flush: %d minutes", len(normal_fallback))
            self._flush_minutes_internal(normal_keys, is_final=False)

        if reflush_keys:
            logger.warning(
                "Re-routing %d already-flushed minutes to late queue (race window detected)",
                len(reflush_keys),
            )
            self._reroute_buffer_to_late_queue(reflush_keys)
```

### 6.3 改动 2：新方法 `_reroute_buffer_to_late_queue`

> **Import 注意**：实现时需在 `flusher.py` 顶部补充 `from minute_bar.aggregator import MAX_LATE_SNAPSHOT_QUEUE_SIZE`。

```python
def _reroute_buffer_to_late_queue(self, minute_keys: list) -> None:
    """Move buffer data for already-flushed minutes to the late queue.

    Happens when data arrives during the race window between buffer pop
    and flushed_snapshot_minutes.add() in _flush_minutes_internal.
    """
    rerouted = 0
    dropped = 0
    ohlcv_dropped_symbols = 0
    with self._state.lock:
        for k in minute_keys:
            # Pop ohlcv first to prevent re-detection on next tick
            ohlcv = self._state.ohlcv_buffers.pop(k, None)
            if ohlcv:
                ohlcv_dropped_symbols += len(ohlcv)

            raw = self._state.raw_snapshot_buffers.pop(k, None)
            if raw:
                for symbol, records in raw.items():
                    for record in records:
                        if len(self._state._late_snapshot_records) < MAX_LATE_SNAPSHOT_QUEUE_SIZE:
                            self._state._late_snapshot_records.append((k, record))
                            rerouted += 1
                        else:
                            dropped += 1
            # Clean up per-minute snapshot to prevent memory leak
            self._state._snapshot_at_minute_end.pop(k, None)
    if rerouted > 0 or dropped > 0 or ohlcv_dropped_symbols > 0:
        logger.warning(
            "Re-routed %d snapshot records to late queue for %d already-flushed minutes "
            "(dropped %d ohlcv symbols, %d raw records hit queue limit)",
            rerouted, len(minute_keys), ohlcv_dropped_symbols, dropped,
        )
```

### 6.4 设计决策

#### 6.4.1 为什么 `ohlcv_buffers` 数据被丢弃

`ohlcv_buffers` 存储的是聚合后的 OHLCV 数据，无法还原为单条记录。第一次 flush 已包含正确的聚合结果，竞态窗口内的数据对聚合结果的增量贡献极小（相对于 44K rows，竞态窗口内仅新增数百条）。

`raw_snapshot_buffers` 数据被保留（路由到 late queue），因为这些是原始记录，可以正确 append 到已有文件。

#### 6.4.2 为什么不改 `_flush_minutes_internal` 的标记时机

将 `flushed_snapshot_minutes.add()` 提前到第一次 lock block（文件写入前）虽然能解决竞态，但引入新风险：如果文件写入失败，分钟已被标记为 flushed 但实际未写入。当前设计中非 final flush 写入失败会 `SystemExit(1)`，但保持写入后才标记是更安全的语义。

#### 6.4.3 Kline 文件影响分析（低优先级）

`reflush_keys` **不调用** `_flush_minutes_internal`，因此 kline 文件不会被二次覆盖。丢弃 `ohlcv_buffers` 意味着 kline 文件的 OHLCV 聚合不包含竞态窗口内的增量数据。当前优先保证 snapshot 和 order 的生成正确性，kline 精度问题后续再处理。

#### 6.4.4 `_snapshot_at_minute_end` 清理

`_reroute_buffer_to_late_queue` 中清理 `_snapshot_at_minute_end[k]`，防止微量内存泄漏。这些 per-minute snapshot 数据不影响 re-route 正确性——step 4 的 `append_snapshot_records` 只追加原始记录行，不生成 carry-forward 行（carry-forward 已在第一次 flush 中完整生成）。

carry-forward 行的本质是"无交易活动的 symbol 的最新状态快照"。re-route 的 symbol 都是有交易活动的（否则不会有 raw records），所以不需要新的 carry-forward 行。

#### 6.4.5 `raw_order_buffers` 不需处理

`_reroute_buffer_to_late_queue` 不处理 `raw_order_buffers`。原因：

- Live engine 的 `_data_loop` 和 `_order_loop` **均不调用** `SharedState.process_order`（经代码审查确认：engine.py 无任何调用点）
- `_order_loop` 使用本地 `buffers: Dict[str, _OrderMinuteBuffer]`，通过 `parse_order_line` + `build_order_record` 直接构造 `OrderRecord`，完全不经过 SharedState
- 因此 Live 模式下 `raw_order_buffers` **始终为空**，`_flush_minutes_internal` 中的 `raw_order_buffers.pop(k, None)` 对空字典执行，始终返回 None，无副作用
- Order 的 late detection 基于 engine 本地 `_flushed_order_minutes` 集合，与 `flushed_snapshot_minutes` 完全独立
- **依赖声明**：`reflush_keys` 不调用 `_flush_minutes_internal`，因此不会 flush `raw_order_buffers`。由于 Live 模式下 `raw_order_buffers` 始终为空，此依赖无副作用。若未来 order 路径改为通过 `SharedState.process_order` 写入，需重新评估

#### 6.4.6 与 `_step4_handle_late_records` 的协作

Re-route 到 `_late_snapshot_records` 的数据会在同一次 tick 的 step 4 中被处理：
- `_step4_handle_late_records` pop 所有 late records → `append_snapshot_records` 追加到文件
- 追加而非覆盖，保证第一次 flush 的正确数据不被丢失

> **tick() 步骤顺序依赖**：`tick()` 方法中 step3（re-route）必须在 step4（late record 处理）之前执行，这保证了 re-route 的数据在同一次 tick 中被处理。未来重构时不可调换步骤顺序。

### 6.5 改动汇总

| 文件 | 改动 | 行数 |
|------|------|------|
| `flusher.py` `_step3_minute_output` | 分流 expired_keys 为 normal vs reflush | ~8 |
| `flusher.py` `_reroute_buffer_to_late_queue` | 新方法：buffer 数据路由到 late queue | ~20 |
| `flusher.py` `flush_all_remaining` | 增加 `_step4_handle_late_records()` 调用 | ~3 |

**不改动的文件：** `config.py`、`writer.py`、`aggregator.py`、`engine.py`、`replay.py`

### 6.6 Shutdown 路径：`flush_all_remaining` 必须处理 late records

**问题**：`flush_all_remaining`（关机时调用）只执行 `_flush_minutes_internal`，不执行 `_step4_handle_late_records`。如果 clock-thread 在 step3（re-route）和 step4（late record 处理）之间被 `_running = False` 打断并 join，`_late_snapshot_records` 中已 re-route 的数据不会被写入文件，**snapshot 数据丢失**。

**修复**：在 `flush_all_remaining` 的 `_flush_minutes_internal` 之后、`_write_checkpoint` 之前，增加一步 late record 处理：

```python
def flush_all_remaining(self) -> None:
    with self._state.lock:
        remaining_keys = sorted(self._state.ohlcv_buffers.keys())

    flush_error = None
    if remaining_keys:
        try:
            self._flush_minutes_internal(remaining_keys, is_final=True)
        except Exception as e:
            flush_error = e

    # Flush any late records that were re-routed but not yet processed by step 4.
    # After all threads are joined, no new late records will be added.
    self._step4_handle_late_records()

    try:
        self._write_checkpoint()
    except Exception:
        logger.exception("Failed to write checkpoint after final flush")
        if flush_error:
            logger.error("Also had flush errors (checkpoint error takes priority): %s", flush_error)
        raise
    if flush_error:
        raise flush_error
```

**安全性**：`flush_all_remaining` 在所有线程 join 之后调用（engine.py L263-264），不存在并发问题。`_step4_handle_late_records` 对空 `_late_snapshot_records` 是空操作，不影响现有 shutdown 路径。

## 7. 测试计划

### 现有测试

- **197 测试预计 0 个需要修改**
  - `test_watermark_flusher.py` — 测试 flusher 逻辑，但不模拟竞态窗口
  - 其他测试 — 不涉及 `_step3_minute_output` 的 expired_keys 分流

### 新增测试

1. `_reroute_buffer_to_late_queue` 将 `raw_snapshot_buffers` 数据路由到 `_late_snapshot_records`
2. `_reroute_buffer_to_late_queue` 丢弃 `ohlcv_buffers` 数据（不保留在 buffer）
3. `_reroute_buffer_to_late_queue` 在 `_late_snapshot_records` 已满时丢弃超出部分并记录计数
4. `_step3_minute_output` 对已 flush 分钟不调用 `_flush_minutes_internal`
5. `_step3_minute_output` 对已 flush 分钟调用 `_reroute_buffer_to_late_queue`
6. `_step3_minute_output` 对未 flush 分钟正常调用 `_flush_minutes_internal`
7. re-route 后 step 4 正确 append late records 到已有文件
8. re-route 后 checkpoint 不包含 re-route 的 buffer 数据（验证 checkpoint 一致性）
9. `_step3_minute_output` 分流逻辑在同一个锁块中完成（验证无 TOCTOU 窗口）— 建议通过代码结构审查验证（由代码结构决定，非运行时行为）
10. 并发时序：`flushed_snapshot_minutes = {1524}`，同时 `raw_snapshot_buffers[1524]` 有 3 条记录 + `_late_snapshot_records` 有 2 条记录（不同 symbol），验证 step3 re-route 后 step4 处理的总记录数 = 5，无重复
11. 四路分流：`expired_keys` 同时包含 data-driven 和 fallback 类型的分钟，且部分已 flush、部分未 flush，验证四路分流（data-driven×normal、data-driven×reflush、fallback×normal、fallback×reflush）均正确

### 端到端验证

重新运行 data_simulator speed=100：
- 修复前：1524 Live Y=107, Replay Y=42,868
- 修复后：预期 329 个 snapshot 文件全部一致（差异仅限 `seqno` 列）
