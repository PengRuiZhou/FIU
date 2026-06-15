# snapshot/order 并行处理 + 流式优化 — 设计文档

> **实施状态：已完成** (2026-05-21) — 全部 70 项测试通过
>
> 修改文件：`engine.py`、`replay.py`、`file_tailer.py`、`flusher.py`

## 背景

当前 ReplayEngine 处理 5.5M snapshot records + order records（5.7GB order + 737MB snapshot）耗时过长。核心问题：

1. **内存翻倍**：`_raw_by_minute` 存一份，`SharedState.raw_snapshot_buffers` 又存一份
2. **串行收集**：snapshot 和 order 文件顺序读取
3. **串行写入**：snapshot → kline → order 文件逐个写
4. **Live Engine 不处理 order**：`_data_loop` 只读 snapshot，`raw_order_buffers` 从未被填充

## 设计目标

- snapshot 和 order **并行**处理（live + replay 两种模式）
- **流式处理**：峰值内存 < 100MB（只保留当前分钟 buffer）
- Live Engine 新增 order 数据线程
- kline 保持正常工作，暂不优化

## 不变的部分

- `SharedState` / `process_snapshot` 接口不变
- `write_*` 系列函数不变
- `ClockWatermarkFlusher` 逻辑不变（已正确处理 `raw_order_buffers`，Live 模式下不再写入该 buffer，flusher 的 order 代码自然不触发）

## 核心架构决策

### snapshot/order 独立流式处理

snapshot 和 order 在业务上完全独立：

- snapshot 负责 snapshot 文件 + kline 文件输出
- order 负责 order 文件输出
- 两者不要求同一分钟同步落盘
- 只要求各自按分钟完整输出
- **不依赖 `seqno` 做 snapshot/order 跨类型全局排序**（见下方 seqno 语义说明）

因此，Live 和 Replay 模式下 order 处理均为独立流式架构：

```
snapshot stream → SharedState → snapshot/kline（通过 flusher 或直接写）
order stream    → 本地 buffer → 直接 write_order_file()
```

### seqno 语义

`seqno` 仅为程序内部处理序号，用于调试和排查，不作为业务排序字段。

- 不依赖 `seqno` 做 snapshot/order 跨类型全局排序
- Live 模式下，snapshot/order 各自独立处理，`seqno` 仅在 snapshot 的 `SharedState` 内递增
- Replay 模式下，snapshot/order 使用独立 seqno
- 下游如需排序，应使用 `time`、`rcvtime`、文件内顺序或文件 offset
- 如未来需要严格全局事件序列，应另行设计 `global_event_id`

### Checkpoint 并发策略

Live 模式下 snapshot 线程、order 线程、clock 线程均可能更新或读取 `FileState`。为避免 checkpoint 状态不一致，引入 `checkpoint_lock`。

**规则：**

- `FileState.offset` / `pending_line` 更新受 `FileTailer` 内部机制保护（单线程写）
- `CheckpointManager.write()` 需获取 `checkpoint_lock`，读取 snapshot/code/order `FileState` 的一致快照
- order 文件写成功后，才允许 checkpoint 记录对应 order offset
- 如果 order 文件写失败，程序 FATAL 退出，**不推进 checkpoint**
- 重启后允许重复生成同一分钟 order 文件，使用 `.tmp` → `rename` 保证幂等

**committed_offset 机制：**

order 线程需要区分两种 offset：

- `read_offset`：FileTailer 已读取到的字节位置（持续推进）
- `committed_offset`：对应分钟 order 文件成功写出后，才允许提交到 checkpoint 的 offset

checkpoint 中记录的是 `committed_offset`，不是 `read_offset`。如果 order 文件写失败，`committed_offset` 不推进，重启后从旧 offset 重新读取并重建该分钟 order 文件。

具体实现：`_file_states["order"]` 暴露给 CheckpointManager 的 offset 必须来自 `committed_offset`，而不是 `_order_tailer.state.offset`（实时 read_offset）。FileTailer 的 read_offset 仅用于线程内部继续读取，不直接写入 checkpoint。

**per-minute offset 精度：** FileTailer 一次 read chunk 可能读到多个分钟的数据，`read_offset` 可能已超过当前正在 flush 的分钟。因此每个 minute buffer 需保存该分钟最后一条完整记录的 `line_end_offset`。flush 该分钟成功后，`committed_offset` 只能推进到该 minute 的 `line_end_offset`，不能使用 FileTailer 当前全局 `read_offset`。

**写入顺序保证：**

```
order file write success → committed_offset = minute_buffer.line_end_offset → checkpoint_lock → write checkpoint
```

clock-thread 写 checkpoint 时同样获取 `checkpoint_lock`，确保读取的 snapshot/code/order FileState 是一致的。

**实现注意：** `checkpoint_lock` 内必须重新读取各 FileState 的最新值，不能使用锁外缓存的旧状态。否则可能出现 order 已推进 `committed_offset`，但 clock-thread 用旧快照写 checkpoint 覆盖回旧 offset。

---

## 1. Engine（live 模式）— 新增 order 独立流式线程

### 当前线程模型

```
data-thread:    FileTailer(snapshot) → parse → process_snapshot
clock-thread:   tick() → flush expired minutes → write files
```

### 改进后

```
snapshot-thread:  FileTailer(snapshot) → parse → SharedState.process_snapshot()
order-thread:     FileTailer(order) → parse → 本地 buffer → watermark flush → write_order_file()  ← 新增
clock-thread:     tick() → flush expired snapshot/kline minutes → write files
```

### 具体改动

**engine.py:**

- `__init__` 新增 `_order_tailer = FileTailer(csv_dir, "order", ...)`
- `_file_states` 增加 `"order": self._order_tailer.state`
- `start()` 新增 `_order_thread = Thread(target=self._order_loop, ...)`
- 新增 `_order_loop()`：
  - 与 `_data_loop` 轮询结构相同
  - 读 order 文件 → `parse_order_line` → `build_order_record`（使用本地 seqno 计数器）
  - **不经过 `SharedState.process_order()`**：无共享状态，不争 `SharedState.lock`
  - **不执行 `_maybe_refresh_code()`**：code table 刷新由 snapshot 线程负责
  - **order buffer 由 order_loop 独占管理**：`finally` 中完成 stop-driven final flush
- `stop()` 只设置 `_stop_event`，等待 order_loop 自行 flush 残留 buffer 并退出，然后 join `_order_thread` → close `_order_tailer`
- `_restore_from_checkpoint()` 中恢复 order FileState

### Live order flush 策略

Live 模式下，order flush **不能仅依赖"下一条 record 的分钟变化"触发**。如果 order 流断流或收盘最后一分钟没有后续数据到达，当前分钟 buffer 会一直留在内存。

order 线程内部必须定时检查 JST 当前时间，对已过期分钟执行 flush：

```
expired(order_minute) = minute_end_time + output_delay_sec <= current_jst_time
```

具体触发时机（满足任一即 flush）：

1. **Record-driven**：解析 record 后检测到 `minute_key` 变化，flush 上一分钟
2. **Watermark-driven**：每轮轮询末尾检查 JST 时间，flush所有已过期分钟（即使没有新 record 到来）
3. **Stop-driven**：order_loop 检测到 `_stop_event` 后，在 `finally` 中 flush 残留 buffer（buffer 由 order_loop 独占，主线程不直接操作）
4. **Cross-day**：检测到 `record.time` 日期变化，flush 前一天所有残余分钟 buffer

### 线程安全

- snapshot 线程独占 `SharedState.lock`（和 clock 线程共享 SharedState，已有正确的 lock 保护）
- order 线程完全独立，不访问 SharedState，无需任何共享锁
- 两个线程唯一的交集是 `_file_states` 字典（读取受 `checkpoint_lock` 保护，见下方 Checkpoint 并发策略）

### 内存分析

order 线程保留少量未过期 minute buffers：

| 数据 | 单分钟大小 | 峰值 |
|------|-----------|------|
| order buffer (1 min) | ~120K records ≈ 30MB | 30MB |

Replay 模式下严格只保留当前分钟 buffer（分钟边界触发 flush）。Live 模式下由于 watermark delay，可能短时保留 1-2 个未过期分钟 buffer（~60MB）。即使断流，watermark 也会在 `output_delay_sec` 后强制写出。

**内存保护：** `max_pending_minutes` 参数（默认 3）限制同时保留的 order buffer 数量。超过阈值时强制 flush 已过期分钟；如果仍超阈值，记录 WARNING 并强制 flush 最早的分钟。WARNING 日志包含被强制 flush 的 minute_key 和当前未过期状态，便于后续排查是否因 order 延迟导致某分钟 order 不完整。

### Cross-day 处理

order 线程自主检测日切：

1. 解析 record 的 `time` 字段，提取日期
2. 检测到新日期时，flush 前一天所有残余分钟 buffer
3. 重置本地状态，继续处理新日期数据

### Order 写入失败处理

order 文件写失败时：

1. 记录 FATAL 日志
2. **不推进 checkpoint**（保留 order FileState 的旧 offset，重启后可重新处理）
3. 抛出异常，触发 Engine 停止
4. 文件写入使用 `.tmp` → `rename` 模式，保证原子性：如果 rename 前失败，重启后覆盖 `.tmp` 重新生成

**异常传递：** Python `Thread` 内部异常不会自动传播到主线程。order_loop 捕获异常后需存储到 `self._order_thread_error`，并设置 `_stop_event`。主线程（或 clock-thread）定期检查该字段，发现异常时 re-raise，确保 Engine 不静默退出。

---

## 2. ReplayEngine — 流式 + 并行

### 架构

```
run():
  1. load code_table
  2. 并行启动:
     Main thread:          _stream_snapshots()
     Background thread:    _stream_orders()
  3. join order thread
```

### `_stream_orders()`（独立后台线程）

- 读 order.csv → `parse_order_line` → `build_order_record`（使用本地 seqno 计数器）
- 检测分钟边界 → `write_order_file()` → 释放当前分钟 buffer
- **不经过 `SharedState.process_order()`**：独立处理，无共享状态
- `enable_order=False` 时跳过（不启动 order 线程）

### `_stream_snapshots()`（主线程）

- 读 snapshot.csv → `state.process_snapshot()` → 分钟边界触发 `_flush_minute()`
- `_flush_minute()`:
  - 提交 snapshot + kline 写任务到 engine 级 `write_executor`（`ThreadPoolExecutor(max_workers=2)`，run 期间复用）
  - **提交后必须等待两个 future 完成并调用 `result()`**，任一写入失败则 Replay 失败。第一版采用每分钟同步等待模式，不积压异步写任务
  - pop `ohlcv_buffers` / `raw_snapshot_buffers` 释放内存
  - 更新 `last_totalvol_by_symbol` / `last_totalamount_by_symbol`
- `write_executor` 在 `run()` 开始时创建，结束时 `shutdown(wait=True)`

### 内存分析

| 数据 | 单分钟大小 | 峰值 |
|------|-----------|------|
| raw_snapshot_buffers (1 min) | ~17K records ≈ 5MB | 5MB |
| ohlcv_buffers (1 min) | ~4500 aggs ≈ 1MB | 1MB |
| latest_snapshot | ~4500 records ≈ 1.5MB | 1.5MB（常驻） |
| order buffer (1 min) | ~120K records ≈ 30MB | 30MB |
| **总计** | | **~40MB** |

对比原始方案（全量加载）：~3-5GB → 40MB，降低 100 倍。

### 异常处理

- 任一线程异常，`ReplayEngine.run()` 整体失败
- order 线程异常必须传播到主线程
- **快速停止**：任一流失败时通过 `stop_event` 通知另一条流尽快停止，不继续无意义处理大文件

```python
stop_event = threading.Event()

try:
    order_future = executor.submit(self._stream_orders, stop_event)
    self._stream_snapshots(stop_event)
    order_future.result()
except Exception:
    stop_event.set()  # 通知另一条流停止
    raise
```

`_stream_orders()` 和 `_stream_snapshots()` 在循环中定期检查 `stop_event.is_set()`，发现被设置后立即 break。最终不生成 success summary。

### EOF final flush

文件读完后，最后一个分钟不会再遇到"下一分钟边界"，必须显式 flush：

- `_stream_snapshots()` 读到 EOF 后，flush 最后一个 snapshot/kline minute
- `_stream_orders()` 读到 EOF 后，flush 最后一个 order minute
- 两个线程各自的 EOF flush 独立执行，互不依赖

### 输入排序假设

Replay 流式模式假设输入文件按 `time`/`rcvtime` 基本有序。对于已输出分钟的迟到数据：

- 记录 WARNING 日志（含迟到 record 的 symbol、time、当前处理到的 minute_key）
- **不回写已输出分钟**
- 如需修正，走离线补算

### 完整性校验

Replay 结束后生成 `replay_summary.json`，记录各类型输出的分钟范围和文件数量：

```json
{
  "date": "20260520",
  "snapshot_minutes": 301,
  "kline_minutes": 301,
  "order_minutes": 301,
  "snapshot_first_minute": "202605200900",
  "snapshot_last_minute": "202605201500",
  "order_first_minute": "202605200900",
  "order_last_minute": "202605201500",
  "status": "success"
}
```

如果任一输出流失败，Replay 不生成 success summary。

---

## 3. 影响范围

### 修改文件

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `engine.py` | 修改 | 新增 order_tailer + order_thread + order_loop（独立流式） |
| `replay.py` | 重写 | 流式架构 + 并行处理 |

### 不修改文件

| 文件 | 原因 |
|------|------|
| `aggregator.py` | 接口不变，Live 模式下 `process_order` 不再被调用 |
| `writer.py` | 写入逻辑不变 |
| `flusher.py` | 已正确处理 raw_order_buffers，Live 模式下 raw_order_buffers 始终为空，order 代码自然不触发 |
| `models.py` | 数据结构不变 |
| `config.py` | 已有 enable_order 配置 |

### 测试影响

- `test_replay.py`：ReplayEngine.run() 内部重构，输出格式不变（除 seqno 外），需要补充新旧输出一致性测试
- `test_order.py`：同上；seqno 不作为业务比对字段，只比较 symbol、time、price、volume、amount 等核心字段
- `test_integration.py`：不涉及 Engine 或 ReplayEngine 内部，不受影响
- `test_writer.py`：不涉及，不受影响

### 测试策略

- 串行结果作为 golden baseline
- 并行流式结果与 baseline 比对（排除 seqno 字段）
- 验证 replay_summary.json 的分钟范围与预期一致

### 补充测试 case

1. **order thread 写失败检测**：模拟 order 文件写入异常，验证 `self._order_thread_error` 能被主流程检测到，Engine 退出且不推进 checkpoint
2. **committed_offset 精度**：构造 FileTailer 一次 chunk 读到多个分钟的场景，验证 checkpoint 只推进到已成功 flush minute 的 `line_end_offset`，不跳过中间分钟
3. **max_pending_minutes 强制 flush**：设置低阈值触发强制 flush，验证最早分钟被写出并记录 WARNING 日志（含 minute_key、是否未过期、pending 数量、触发原因）
