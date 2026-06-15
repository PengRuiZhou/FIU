# Order 线程性能优化设计

日期：2026-05-27
状态：待实施
评审：3 Agent × 4 轮评审，0 CRITICAL/IMPORTANT，已收敛

## 1. 问题描述

实盘测试发现 order 分钟文件生成速度严重滞后：

| 时段 | 文件大小 | 生成延迟 |
|------|---------|---------|
| 盘前 (0800-0859) | 0.2–5.7 MB | ~1 min（正常） |
| 开盘 0900 | 58 MB | 3 min 落后 |
| 盘中平均 | 30–40 MB | 逐分钟累积 |
| 1005 | 30 MB | 57 min 落后 |

Snapshot 分钟文件生成正常，不受影响。

## 2. 根因分析

### 2.1 读取端：吞吐量不足（根本原因）

`_order_loop` 每次迭代读取一个 65KB chunk，然后固定 sleep 200ms：

```
while self._running:
    for line in self._order_tailer.read_lines():  # 一个 65KB chunk
        parse → buffer → maybe flush
    time.sleep(200ms)  # 无论是否有数据都 sleep
```

**吞吐量上限：** 65KB / 0.2s = 325 KB/s = **19.5 MB/min**
（盘前 buffer 阶段更慢：65KB / 1s = 65 KB/s = **3.9 MB/min**，但盘前数据量小所以不受影响）
**市场时段需：** 30–60 MB/min
**缺口：** 10–40 MB/min，逐分钟累积

Snapshot 不受影响因为 snapshot 数据只有 ~5 MB/min，远低于 19.5 MB/min 上限。

### 2.2 写入端：同步写阻塞读取（加剧因素）

`write_order_file` 当前实现：

1. 构建 500K+ 字符串列表
2. `"\n".join(lines)` → 一个 50MB 巨字符串
3. `atomic_write` → write + **fsync** + rename
4. 总耗时 **0.8–2.6s**，期间**零读取**

fsync 是主因：强制刷盘，50MB 文件在典型服务器上需 0.5–2s。

## 3. 设计方案

分两个阶段实施，第一阶段解决 90% 问题。

### Phase 1：读取连续化 + 写入流式化（~30 行改动）

#### 3.1 读取端：drain loop + 增大 chunk

**改动文件：** `engine.py` `_order_loop`

**当前：**
```python
while self._running:
    today = self._get_target_date()
    self._order_tailer.set_date(today)

    for line in self._order_tailer.read_lines():
        ...  # parse, buffer, flush

    # watermark flush, memory protection, stall detection ...

    interval = get_poll_interval_ms(self._config) / 1000.0
    time.sleep(interval)  # ← 固定 sleep，即使有数据也不读
```

**改为：**
```python
# 注意：drain loop、checkpoint 更新、watermark flush、sleep 均在 inner try 块内，
# 与当前代码结构一致（engine.py L429-543）。sleep 在 inner try 之外，仅成功迭代后执行。

while self._running:
    today = self._get_target_date()
    self._order_tailer.set_date(today)

    data_read = False
    drain_count = 0
    while True:  # ← drain loop：有数据时连续读
        lines = list(self._order_tailer.read_lines())
        if not lines:
            break
        data_read = True
        drain_count += 1

        for line in lines:
            ...  # parse, buffer, record-driven flush（逻辑不变）

        # 安全边界：每 100 次 drain 迭代执行一次保护逻辑
        if drain_count >= 100:
            self._flush_expired_order_minutes(
                buffers, output_dir, output_delay_sec, current_minute or ""
            )
            self._enforce_max_pending(buffers, output_dir)
            drain_count = 0

    # checkpoint 状态更新
    with self._checkpoint_lock:
        self._file_states["order"] = FileState(
            offset=self._committed_order_offset,
            pending_line=b"",
            date=today,
        )

    # watermark flush, memory protection, stall detection（每轮外循环必执行）
    self._flush_expired_order_minutes(...)
    self._enforce_max_pending(buffers, output_dir)
    ...  # stall detection 不变

    if data_read:
        time.sleep(0.001)  # 1ms yield，防 CPU 饥饿（Linux ~1ms，Windows ~15ms）
    else:
        interval = get_poll_interval_ms(self._config) / 1000.0
        time.sleep(interval)  # 无数据时才用配置间隔
```

**drain loop 安全机制：**

| 机制 | 位置 | 说明 |
|------|------|------|
| Record-driven flush | drain loop 内（每行解析后） | minute advance 时立即 flush 上一分钟，逻辑不变 |
| 过期分钟 flush | 每 100 次 drain + 每轮外循环 | 防止长时间 drain 导致 buffer 堆积 |
| 内存保护 `_enforce_max_pending` | 每 100 次 drain + 每轮外循环 | 防止 buffer 超过 `MAX_PENDING_ORDER_MINUTES=3` |
| Checkpoint 状态更新 | 每轮外循环 | drain 结束后更新 `_file_states["order"]` |
| Stall 检测 | 每轮外循环 | `time.monotonic()` 判断不变 |

**同时**，order tailer 的 chunk size 可配置化，生产环境建议 512KB（默认保持 65KB 向后兼容）：

```python
# config.py — InputConfig 新增字段
@dataclass
class InputConfig:
    ...
    order_chunk_size_bytes: int = 65536   # order tailer 专用；默认 65KB 保持向后兼容，生产环境设 524288

# engine.py — Engine.__init__ 使用新配置
self._order_tailer = FileTailer(
    config.input.csv_dir, "order",
    chunk_size=config.input.order_chunk_size_bytes,
    encoding=config.input.file_encoding,
)
```

**吞吐量预估（`order_chunk_size_bytes=524288` 时）：** drain loop + 512KB chunk → 受 Python 解析速度限制（~2-5μs/行），实际可达 **50-100 MB/min**，远超 30-60 MB/min 需求。默认 65KB 时 drain loop 仍消除 sleep 瓶颈，吞吐量提升显著但低于 512KB 配置。

#### 3.2 写入端：streaming write + 保留锁 + 去 fsync

**改动文件：** `writer.py` `write_order_file`

**当前：**
```python
def write_order_file(output_dir, minute_key, order_records):
    ...
    lines = [ORDER_HEADER]
    for rec in order_records:           # 500K 次 append
        lines.append(_format_order_row(rec))
    atomic_write(path, "\n".join(lines) + "\n")  # join → 50MB 字符串 → write + fsync
```

**改为：**
```python
def write_order_file(output_dir, minute_key, order_records):
    if not order_records:
        return

    path = get_order_file_path(output_dir, minute_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"

    with _get_write_lock(path):              # 保留文件级写锁（防御性）
        with open(tmp_path, "w", encoding="utf-8", newline="",
                  buffering=1_048_576) as f:  # 1MB I/O buffer，减少 syscall
            f.write(ORDER_HEADER)
            f.write("\n")
            for rec in order_records:         # 逐行写入，无大字符串分配
                f.write(_format_order_row(rec))
                f.write("\n")
        os.replace(tmp_path, path)            # atomic rename，无 fsync

    logger.info("Wrote order file: %s (%d records)", path, len(order_records))
```

**关键决策：**

1. **去掉 fsync** — Order 文件是**派生数据**，可从源 CSV replay 重生成。Checkpoint 写入仍然使用 fsync，崩溃恢复安全。最坏情况：崩溃时丢失最后 1-2 分钟 order 文件，replay 可补回。

2. **保留 `_get_write_lock`** — 虽然当前 order 线程是唯一写入者（竞态不可能发生），但：
   - 锁成本忽略不计（~50ns uncontended）
   - `raw_order_buffers` 管线已在 flusher 中存在，未来若启用 `process_order` 会引入并发写入
   - 与 `append_order_records` 保持对称一致性（两者使用同一把锁）
   - 注意：`write_order_file` 和 `append_order_records` 仅在 order-thread 内被调用，不会并发。late-order 路径通过 `_flushed_order_minutes` 集合保证时序：先 flush（`write_order_file`）→ add to set → 后 late（`append_order_records`）

3. **`buffering=1_048_576`（1MB）** — Python 默认文本模式 buffer 8KB，写 50MB 需要 ~6400 次 write syscall。1MB buffer 降至 ~50 次，在 Linux 上减少用户态-内核态切换开销。选值 1MB 覆盖约 2 个 512KB chunk，平衡内存和 syscall 次数。

4. **使用 `get_order_file_path` helper** — 当前 `write_order_file` 内联拼接路径（lines 243-245），改为复用已有 helper（lines 37-41），消除重复代码。

**写入耗时预估：** 0.8–2.6s → **0.15–0.4s**（3-8x 提速）

#### 3.3 Phase 1 改动汇总

| 文件 | 改动 | 行数 |
|------|------|------|
| `engine.py` `_order_loop` | drain loop + 每 100 次保护检查 | ~15 行 |
| `engine.py` `__init__` | order tailer 使用 `config.input.order_chunk_size_bytes` | 1 行 |
| `config.py` `InputConfig` | 新增 `order_chunk_size_bytes` 字段 + 解析 | ~3 行 |
| `writer.py` `write_order_file` | streaming write + 保留锁 + 去 fsync + 1MB buffer | ~15 行 |
| `config/production.ini` | 新增 `order_chunk_size_bytes = 524288` | 1 行 |

**部署注意：** `config.py`（字段定义+解析）和 `production.ini`（配置值）必须同时部署。单独部署 INI 文件时，`order_chunk_size_bytes` 会被 `configparser` 静默忽略，使用默认 65KB，优化无效。

**不改动的文件：** `file_tailer.py`、`checkpoint.py`、`flusher.py`、`aggregator.py`、`csv_parser.py`、`replay.py`

**不改的函数：** `atomic_write`（snapshot/kline 仍使用）、`append_order_records`（late order append 不变）

### Phase 2（可选）：Producer-Consumer 读写分离

如果 Phase 1 在极端硬件（网络盘、慢 HDD）上仍不够，可进一步拆分：

- **Reader 线程**：读取 + 解析 + buffer → enqueue 到 `queue.Queue(maxsize=2)`
- **Writer 线程**：dequeue → 写文件 → 更新 `_committed_order_offset`

估计改动 ~100 行，限于 `engine.py`。当前不实施，留作后备。

## 4. 安全性分析

### 4.1 Snapshot 不受影响

| 资源 | 证明 |
|------|------|
| `_data_loop` 方法 | 不修改，snapshot 读取路径不变 |
| `_snapshot_tailer` | 独立 FileTailer 实例，chunk_size 不变 |
| `write_snapshot_file` / `write_kline_file` | 不修改，仍走 `atomic_write` + fsync |
| `SharedState.lock` | Order 线程不触碰 SharedState |
| `checkpoint_lock` | 加锁位置和频率不变 |
| CPU GIL | I/O 释放 GIL；解析时 GIL 竞争在多核服务器上 <1% |
| 磁盘 I/O | Order 写无 fsync 且瞬时完成；Snapshot 写文件小（~5MB） |

### 4.2 Checkpoint 安全

- `_flush_order_minute` 中 `_committed_order_offset` 的更新时机不变（写入完成后、checkpoint_lock 内）
- `os.replace()` 在 ext4（`data=ordered` 模式，Linux 默认）上是原子操作，文件不会损坏
- 无 fsync 时，崩溃恢复可能丢失最后 1-2 分钟 order 文件，可从 replay 补回
- 注意：`os.replace()` 在 NTFS 上对同卷操作是原子的；跨卷不保证（本项目部署为同卷）
- `.tmp` 文件残留：`recover_flushed_minutes` 通过正则匹配只认 `.csv` 文件，自动忽略 `.tmp`

### 4.3 Write Lock 安全性（评审结论）

3 Agent 一致确认：**当前无竞态，保留锁为防御性措施。**

| 问题 | 结论 |
|------|------|
| `write_order_file` 的调用者 | 仅 order-thread（engine.py L565 flush, L477 late fallback）和 replay（单线程） |
| `append_order_records` 的调用者 | 仅 order-thread（engine.py L472 late append） |
| clock-thread 的 order 写入路径 | `flusher.py:330` 存在但 `SharedState.raw_order_buffers` 未被填充（`process_order()` 未调用），是 dead code |
| 并发竞态可能？ | **NO** — 单线程顺序执行，`_flushed_order_minutes` 保证严格时序 |
| 为何仍保留锁？ | 成本忽略（~50ns）；`raw_order_buffers` 管线已存在，未来启用可能引入并发；与 `append_order_records` 对称一致 |

### 4.4 内存影响

| 组件 | 当前 | Phase 1 后 |
|------|------|-----------|
| Order buffer（0-3 分钟） | ≤ 375 MB（3 × 500K records × 250B） | 相同（上限不变） |
| Write 临时峰值 | ~100 MB（500K 列表 + 50MB join 字符串） | **0 MB**（逐行写入） |
| 512KB chunk buffer | 65 KB | 512 KB（+447 KB） |
| Drain loop 临时 `list(lines)` | N/A | ~20K lines × ~25B ≈ 0.5 MB per chunk |
| **峰值合计** | ~475 MB | **~375 MB**（略降） |

读取更快 → 追上进度 → buffer 积累更少 → 稳态内存更低。

Drain loop 期间 buffer 可能短暂超过 `MAX_PENDING_ORDER_MINUTES=3`（最多多出 100 次 drain 迭代的数据），但每 100 次迭代执行 `_enforce_max_pending` 限制超标幅度。外循环结束时再次执行，保证最终一致性。

### 4.5 边界情况

| 场景 | 行为 |
|------|------|
| 文件无新数据 | drain loop 立即退出（`lines` 为空），走配置 sleep |
| 持续高吞吐（>60MB/min） | drain loop 持续运行，CPU 占满单核，正确行为 |
| 跨日重置 | `record_date != current_date` 逻辑在 drain loop 内，不变 |
| `MAX_PENDING_ORDER_MINUTES=3` | 每 100 次 drain 检查 + 外循环兜底；buffer 可能短暂超标（约 100 次 drain 的数据量），外循环结束时再次执行保证最终一致 |
| Stall 检测 | 逻辑在 drain loop 外，`time.monotonic()` 判断不变 |
| Late order append | `append_order_records` 走独立路径，与 `write_order_file` 使用同一把锁 |
| Late order file missing | `write_order_file` 创建新文件，输出格式与当前 byte-identical |
| `pending_line` 残留 | drain loop 每次调用 `read_lines()` 处理一个 chunk 的完整行；不完整行留到下次调用，正确行为 |
| `read_lines()` 返回空（无换行的 chunk） | drain loop 退出，1ms 后重试，等待外部写入换行符 |
| Replay 模式 | `replay.py` 调用 `write_order_file`，输出格式 byte-identical，不受影响 |

## 5. 性能预期

| 指标 | 当前 | Phase 1 后 |
|------|------|-----------|
| 读取吞吐量 | 19.5 MB/min | 50-100 MB/min（**5x+**）¹ |
| 单次写入 50MB | 0.8-2.6s | 0.15-0.4s（**3-8x**） |
| 0900 延迟 | 3 min | < 10s |
| 0930 延迟 | 41 min | < 30s |
| 1005 延迟 | 57+ min | < 30s |
| 峰值内存 | ~475 MB | ~375 MB（略降） |

性能预估依赖：SSD 或更快存储。HDD 上 fsync 耗时更长，去掉 fsync 后提速更明显。SSD 上 fsync 本身较快，提速倍数略低但绝对延迟仍大幅改善。

¹ 读取吞吐量预估基于 `order_chunk_size_bytes=524288`（512KB）。默认 65KB 时 drain loop 仍消除 sleep 瓶颈，吞吐量由 Python 解析速度决定，仍远超当前 19.5 MB/min。

## 6. 测试计划

### 现有测试

- **176 测试全部通过，预计 0 个需要修改**
  - `test_writer.py` — `TestWriteOrderFile`、`TestAppendOrderRecords` 用 `csv.reader` 验证输出格式，streaming write 格式 byte-identical，无需改动
  - `test_order.py` — `TestReplayWithOrder` 通过 `ReplayEngine` 调用 `write_order_file`，不受影响
  - `test_watermark_engine.py` — 不直接执行 `_order_loop`，使用 mock，不受影响
  - `test_engine_late.py` — 仅测试 `recover_flushed_minutes`，不受影响

### 新增测试

1. `write_order_file` streaming write 输出与当前实现 byte-identical
2. drain loop 在无数据时正确 sleep（使用配置间隔）
3. drain loop 在有数据时连续读取（不 sleep）
4. drain loop 每 100 次迭代执行 `_flush_expired_order_minutes` + `_enforce_max_pending`
5. drain loop 内 minute-boundary 变化时 record-driven flush 正确触发
6. `write_order_file` streaming write 仍获取 `_get_write_lock`
7. `order_chunk_size_bytes` 配置项解析正确

### 实盘验证

对比优化前后 order 文件生成延迟。部署时机：收盘后（15:30 JST）停止服务 → 部署 → 次日开盘前（08:00 JST）启动。Checkpoint 恢复机制保证平滑过渡。

## 7. 不做的事

- 不改 `FileTailer.read_lines()` 接口
- 不改 `atomic_write` 函数（snapshot/kline 仍使用）
- 不改 `append_order_records`（late order append 不变）
- 不改 `flusher.py`、`aggregator.py`、`csv_parser.py`
- 不改 `replay.py`
- `config.py` 仅新增 `order_chunk_size_bytes` 字段及解析，其余配置不变
- 不引入 asyncio 或第三方库
- Phase 2 producer-consumer 暂不实施
