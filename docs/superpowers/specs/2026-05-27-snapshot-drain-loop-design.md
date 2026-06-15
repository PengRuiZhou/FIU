# Snapshot `_data_loop` Drain Loop 补充设计

日期：2026-05-27
状态：已通过终审，待实施
来源：Phase 14 端到端验证发现
评审：3-agent 审阅 + 3-agent 深度论证（2026-05-27）

## 1. 问题描述

Phase 14 Order 线程性能优化的端到端验证中，发现 snapshot 分钟文件有 3 个边界分钟的 `update_flag=Y` 记录数严重不足：

| 文件 | Live Y 数 | Replay Y 数 | 差异 |
|------|-----------|-------------|------|
| `snapshot_minute_20260525_1128.csv` | 37 | 10,811 | -10,774 |
| `snapshot_minute_20260525_1129.csv` | 76 | 12,201 | -12,125 |
| `snapshot_minute_20260525_1524.csv` | 168 | 42,868 | -42,700 |

其余 326 个 snapshot 分钟文件的 `update_flag=Y` 记录数完全一致（差异仅限 `seqno` 列，由 live/replay 序号分配机制不同导致）。

Order 分钟文件全部 417 个、70,809,005 条记录完全一致，0 差异。

## 2. 根因分析

### 2.1 `_data_loop` 仍是旧模式

`_data_loop`（engine.py）每次迭代只读一个 chunk（~65KB），然后固定 sleep：

```python
# engine.py _data_loop 当前实现（简化）
def _data_loop(self):
    while self._running:
        try:
            today = self._get_target_date()
            self._snapshot_tailer.set_date(today)
            self._code_table._tailer.set_date(today)

            for line in self._snapshot_tailer.read_lines():  # 一个 65KB chunk
                parsed = parse_snapshot_line(line, ...)
                if parsed:
                    self._state.process_snapshot(parsed)      # buffer + watermark

            self._maybe_refresh_code()

            with self._checkpoint_lock:
                self._file_states["snapshot"] = self._snapshot_tailer.state
                self._file_states["code"] = self._code_table.get_state()

        except Exception as e:
            ...

        interval = get_poll_interval_ms(self._config) / 1000.0
        time.sleep(interval)                                   # 固定 sleep
```

### 2.2 事件时间线（data_simulator speed=100）

#### 1128/1129 时间线（lunch break 边界）

```
数据时间线（20260525 snapshot）：
  1127: 10,677 records
  1128: 10,811 records  ← 受影响
  1129: 12,201 records  ← 受影响
  1130: 4,674 records   ← lunch break close

Wall-clock 事件（speed=100，每真实秒 ≈ 100 秒数据时间）：
  T+0.0s  data_simulator 写完 1127 数据，开始写 1128
  T+0.1s  _data_loop 读 1128 的第 1 个 chunk（~37 records）
          → watermark 推进到 1128
  T+0.1s  data_simulator 已写完 1128、1129、1130 全部数据
          （speed=100 下，1127→1130 的 3 分钟数据时间 ≈ 0.03 秒真实时间）
  T+0.2s  _data_loop 读下一个 chunk → 包含 1129/1130 数据
          → watermark 跳到 1130
  T+1.0s  clock-thread flusher tick:
          is_data_driven_expired(1128, watermark=1130, delay=1min) → TRUE
          is_data_driven_expired(1129, watermark=1130, delay=1min) → TRUE
          → FLUSH 1128（仅 37 条）和 1129（仅 76 条）
  T+1.1s  _data_loop 继续读 1128/1129 的剩余数据
          → 1128 已在 flushed_snapshot_minutes → LATE RECORD 路径
```

#### 1524 时间线（post-market 边界）

```
数据时间线（20260525 snapshot）：
  1524: 42,868 records   ← 受影响
  1525: 0 records        ← 无 snapshot 数据（仅 order 数据）
  ...
  1529: 0 records        ← 无 snapshot 数据
  1530: close data       ← market close

Wall-clock 事件（speed=100）：
  T+0.0s  data_simulator 写完 1524 数据，开始写 1525+
  T+0.0s  1525-1529 无 snapshot 数据 → data_simulator 跳过
          data_simulator 直接到 1530（market close）
  T+0.1s  _data_loop 读 1524 的第 1 个 chunk（~168 records）
          → watermark 推进到 1524
  T+0.2s  _data_loop 读下一个 chunk → 包含 1530 数据
          → watermark 直接跳到 1530（跳过 1525-1529，因为这些分钟无 snapshot 数据）
  T+1.0s  clock-thread flusher tick:
          is_data_driven_expired(1524, watermark=1530, delay=1min) → TRUE
          → FLUSH 1524（仅 168 条）

机制分析：
  1524 与 1128/1129 完全同构。唯一表面差异是 1525-1529 无 snapshot 数据，
  所以 watermark 从 1524 直接跳到 1530（而非经过中间分钟）。
  本质相同：无 drain loop → watermark 在数据未完全读入时就已推进。
```

**关键问题：** `_data_loop` 单次只读一个 chunk（~500 条 snapshot），watermark 就更新了。data_simulator 在 100x 速度下，数据在极短时间内全部写入，但 `_data_loop` 来不及在 flusher 下一次 tick 前读完所有数据。

### 2.3 为什么生产环境不受影响

生产环境 speed=1，数据实时到达：
- 每分钟数据在 ~60 秒内陆续到达
- watermark 每分钟推进 1 分钟
- `_data_loop` 有充足时间读完所有数据
- flusher 的 `data_flush_delay_minutes=1` 保证当前分钟不会被提前 flush

**但 drain_count 保护仍有价值：** 生产环境下如果 snapshot 突然积压（如网络恢复后大量数据涌入），drain loop 需要定期让出以更新 checkpoint 和检测 order 线程错误。生产环境虽不受此 watermark 提前推进的 bug 影响，但 drain loop 在生产场景中提供了额外的防御性保障（见 §4.4）。

### 2.4 为什么 `_order_loop` 没有这个问题

Phase 14 已为 `_order_loop` 加了 drain loop：
```python
data_read = False
drain_count = 0
while True:                                      # drain loop
    lines = list(self._order_tailer.read_lines())
    if not lines:
        break
    data_read = True
    drain_count += 1
    for line in lines:
        ...  # parse, buffer, flush
    if drain_count >= 100:
        self._flush_expired_order_minutes(...)    # flush 过期分钟
        self._enforce_max_pending(...)            # 强制清理超限 buffer
        drain_count = 0                           # ← 重置，继续 drain
# drain 结束后 watermark 已反映所有可用数据
```

Drain loop 保证：watermark 更新前，所有可用数据已读入 buffer。flusher 看到的 watermark 是准确的。

**关键行为差异：** `_order_loop` 在 `drain_count` 触发后执行 flush 维护并 **重置计数器继续 drain**（不退出 drain loop），而提议的 `_data_loop` 是 **break 退出 drain loop**（让出以更新 checkpoint + 检测错误）。这是因为 snapshot flush 由 clock-thread 独立负责，data loop 不承担 flush 职责。

## 3. 设计方案

### 3.1 给 `_data_loop` 加 drain loop + drain_count 保护

**改动文件：** `engine.py` `_data_loop`

**改为：**
```python
def _data_loop(self) -> None:
    while self._running:
        try:
            self._check_order_thread_error()

            current_date = self._get_target_date()
            self._snapshot_tailer.set_date(current_date)
            self._code_table._tailer.set_date(current_date)

            data_read = False
            drain_count = 0
            while True:                                  # ← drain loop
                lines = list(self._snapshot_tailer.read_lines())  # materialize 一个 chunk 的所有行
                if not lines:
                    break
                data_read = True
                drain_count += 1

                for line in lines:
                    parsed = parse_snapshot_line(line, self._config.input.file_encoding)
                    if parsed:
                        record_date = str(parsed.time)[:8]
                        if record_date != current_date:
                            continue
                        self._state.process_snapshot(parsed)

                self._maybe_refresh_code()

                if drain_count >= 1000:                  # ← drain_count 保护
                    # break 退出 drain loop 以更新 checkpoint + 检测错误；
                    # 与 _order_loop 不同（order 在阈值处 flush + reset+继续），
                    # 因为 snapshot flush 由 clock-thread 独立负责
                    logger.debug("Snapshot drain yield at %d chunks", drain_count)
                    break

            with self._checkpoint_lock:
                self._file_states["snapshot"] = self._snapshot_tailer.state
                self._file_states["code"] = self._code_table.get_state()

        except Exception as e:
            logger.error("Data thread error: %s", e, exc_info=True)
            self._running = False
            return
        # Note: 无 finally 块 — snapshot flush 由 clock-thread 负责（见 stop() 中
        # flush_all_remaining），snapshot tailer 由 Engine.stop() 统一 close

        if data_read:
            time.sleep(0.001)                            # ← 1ms yield
        else:
            interval = get_poll_interval_ms(self._config) / 1000.0
            time.sleep(interval)                         # ← 无数据时配置间隔
```

### 3.2 设计决策说明

#### 3.2.1 `drain_count >= 1000` 保护（阈值选择）

**为什么需要 drain_count：**
1. **Checkpoint 更新频率** — 没有 drain_count，5M snapshot 行（~77,000 chunks）可能连续 drain 50-100 秒，期间 checkpoint 冻结。进程崩溃 = 数据丢失。
2. **`_check_order_thread_error()` 检测** — order 线程崩溃时，data 线程需要在外层循环才能检测到。
3. **Clock-thread lock 响应性** — `process_snapshot` 内部获取 `SharedState.lock`，长时间 drain 可能增加与 clock-thread flusher 的锁竞争。

**为什么阈值 1000：**
- snapshot 每 chunk ~65-500 条（取决于 chunk 密度，65KB / 130-1000 字节每行）
- 1000 chunks × 65-500 条/chunk = ~65,000-500,000 条，耗时约 0.5-2 秒
- 这给出合理的 checkpoint 更新间隔（秒级），同时足够大以避免在高速测试中频繁让出
- 对比 `_order_loop` 的阈值 100：order 每 chunk ~800 条，100 chunks = ~80,000 条
- **注意**：两者 drain_count 触发行为不同 — `_order_loop` 做 flush 后 `drain_count=0` 继续 drain，`_data_loop` 是 `break` 退出 drain loop。阈值对齐是为了 checkpoint 更新频率的一致性（秒级），而非行为等价

**为什么只 `break` 不做 flush：**
- Snapshot flush 由 clock-thread 的 `ClockWatermarkFlusher.tick()` 独立负责
- `raw_snapshot_buffers` 无 `MAX_PENDING` 限制，由 flusher 在 `SharedState.lock` 下管理
- Data loop 不应承担 flush 职责，避免与 clock-thread 的 flusher 竞争

#### 3.2.2 `_maybe_refresh_code` 从外层循环移入 drain loop 内

**位置变更（非简单保留）：** 当前代码中 `_maybe_refresh_code` 在 `for line in read_lines()` 循环**之外**、外层 `while self._running` 内部（engine.py L369）。设计将其移入 drain loop 内每次迭代调用。

**移入的理由：**
1. **可忽略的性能开销** — `_maybe_refresh_code` 内部使用 `time.monotonic()` 节流（`code_refresh_sec`），每次调用 ~100ns（实际刷新触发时有 I/O，但由 `code_refresh_sec` 节流），不会显著增加开销
2. **架构一致性** — 与 `_order_loop` 中每轮迭代做必要维护的模式一致
3. **防御性** — 如果未来 `code_refresh_sec` 调小或数据量增大，保持 code table 刷新频率不退化

**关于异常处理：** `_maybe_refresh_code` 内部有独立的 try-except，异常会被捕获并记录日志，不会中断 drain loop。`process_snapshot` 的异常同样有独立处理路径。

#### 3.2.3 与 `_order_loop` drain loop 的完整对比

| 特性 | `_order_loop` | `_data_loop` |
|------|--------------|--------------|
| Drain loop | `while True` + `list(read_lines())` | 同左 |
| drain_count 阈值 | 100（~80K records） | 1000（~65K records） |
| drain_count 触发动作 | `_flush_expired_order_minutes` + `_enforce_max_pending` + **`drain_count=0` 继续 drain** | `break`（**退出 drain loop**，让出以更新 checkpoint + 检测错误） |
| `_maybe_refresh_code` 位置 | N/A | drain loop 内每次迭代（节流由内部保证） |
| Adaptive sleep | 有数据 1ms，无数据配置间隔 | 同左 |
| Flush 职责 | data loop 内负责 | clock-thread flusher 独立负责 |
| 异常处理 | 内层 try-except → 设 `_order_thread_error`，外层 continue | 内层无 try-except → 外层 try-except 捕获，设 `_running=False` 并 return |
| stop() 响应 | drain loop 内无 `_running` 检查，依赖 drain_count 保证延迟远低于 join timeout（10s） | 同左 |
| finally 块 | 有（flush 残余 buffer + close tailer） | 无（snapshot flush 由 clock-thread 负责，tailer 由 `Engine.stop()` close） |

**两者 drain_count 触发动作不同的原因：**
- Order flush 是 record-driven（每条记录检查 minute advance），需要在 drain 内主动 flush
- Snapshot flush 是 data-driven（clock-thread tick 驱动），data loop 不应承担 flush 职责

### 3.3 改动汇总

| 文件 | 改动 | 行数 |
|------|------|------|
| `engine.py` `_data_loop` | drain loop + drain_count 保护 + adaptive sleep + `_maybe_refresh_code` 从外层循环移入 drain loop 内 | ~15 行 |

**不改动的文件：** `config.py`、`writer.py`、`flusher.py`、`aggregator.py`、`replay.py`、`file_tailer.py`、`clock.py`

## 4. 安全性分析

### 4.1 对现有逻辑的影响

| 组件 | 影响 | 说明 |
|------|------|------|
| `process_snapshot` | 无变化 | 仍在 drain loop 内每行调用，late detection 逻辑不变 |
| `_maybe_refresh_code` | 无变化 | 保留在 drain loop 内，内部 `time.monotonic()` 节流 |
| `_checkpoint_lock` | **改善** | drain_count >= 1000 保证 checkpoint 每秒级更新（vs 无限制时可能 50-100 秒冻结） |
| `ClockWatermarkFlusher.tick()` | **改善** | watermark 更准确，减少过早 flush |
| `_order_loop` | 无影响 | 独立线程，不共享资源 |
| `SharedState.lock` | 无影响 | `process_snapshot` 内部使用，drain loop 不改变加锁行为 |
| Replay 模式 | 无影响 | `_data_loop` 仅 Live 模式使用 |

### 4.2 内存影响

Drain loop 期间 buffer 可能短暂增大（snapshot 每分钟 ~5MB），但：
- Snapshot 数据量远小于 order（639MB vs 4.4GB）
- 每分钟 ~10,000-40,000 条记录，即使 buffer 3 分钟也仅 ~60-120MB（含 Python 对象开销，单条 `ParsedSnapshot` 约 400-800 字节 vs raw ~200 字节）
- drain_count >= 1000 限制每次 drain 最多 ~65,000-500,000 条（~30-130MB），不会无限增长
- Drain loop 期间 watermark 准确推进，flusher 及时清理过期分钟

### 4.3 CPU 影响

Drain loop 在有数据时连续读取，CPU 占用单核。与 `_order_loop` 相同模式：
- `_order_loop` 已在生产环境验证，CPU 无问题
- Snapshot 数据量小（~5 MB/min），drain loop 持续时间短
- drain_count >= 1000 保证不会无限占用 CPU
- 无数据时走配置 sleep（10ms），CPU 使用率接近 0

### 4.4 边界情况

| 场景 | 行为 |
|------|------|
| 文件无新数据 | drain loop 立即退出（`lines` 为空），走配置 sleep |
| 持续高吞吐 | drain loop 持续运行，每 1000 chunks 让出一次更新 checkpoint |
| `process_snapshot` 触发 minute advance | watermark 推进，flusher 在下一次 tick flush 上一分钟 |
| Late snapshot record | `process_snapshot` 内 `minute_key in flushed_snapshot_minutes` 检测，路由到 late queue，逻辑不变 |
| `read_lines()` 返回空 | drain loop 退出，adaptive sleep |
| `_maybe_refresh_code` 异常 | 内部 try-except 捕获，日志记录，不中断 drain loop |
| drain_count >= 1000 触发 | `break` 退出 drain loop，更新 checkpoint，重新进入外层 `while self._running` 循环 |
| 进程崩溃 | checkpoint 已在 drain_count 保护下定期更新，单轮 drain 最大未 checkpoint 数据量 ~65,000 条（约 2-6 分钟 snapshot 数据，取决于吞吐密度） |

## 5. 测试计划

### 现有测试

- **186 测试预计 0 个需要修改**
  - `test_watermark_engine.py` — 不直接执行 `_data_loop`，使用 mock
  - `test_watermark_flusher.py` — 测试 flusher 逻辑，不涉及 `_data_loop`
  - 其他测试 — 不涉及 `_data_loop`

### 新增测试

1. `_data_loop` drain loop 在无数据时正确 sleep（使用配置间隔）
2. `_data_loop` drain loop 在有数据时连续读取（不 sleep）
3. `_data_loop` drain loop 正确更新 `SharedState.current_minute`（watermark）
4. `_data_loop` drain loop drain_count >= 1000 时 break 退出并更新 checkpoint
5. `_data_loop` drain loop `_maybe_refresh_code` 在每轮迭代被调用（但受内部节流控制）
6. `_data_loop` drain loop 在 `stop()` 触发后（`_running=False`）能及时退出，clock-thread `flush_all_remaining()` 正确刷出剩余数据
7. `_data_loop` drain loop 在 clock-thread 已 flush 某分钟后，后续读到的 late record 正确路由到 `_late_snapshot_records` 队列
8. `_data_loop` drain loop 在跨日边界时，`record_date != current_date` 的记录被正确过滤
9. `_data_loop` drain loop 在 `parse_snapshot_line` 返回 None 时不中断 drain，继续处理后续行
10. `_data_loop` drain loop drain_count break 后，外层循环重新进入 drain loop 能正确恢复继续读取

### 端到端验证

对比修复前后 snapshot 文件的 `update_flag=Y` 记录数：
- 修复前：3 个文件不一致（1128, 1129, 1524）
- 修复后：预期 329 个文件全部一致（差异仅限 `seqno` 列）

## 6. 不做的事

- 不改 `FileTailer.read_lines()` 接口
- 不给 `_data_loop` 加 flush 逻辑（snapshot flush 由 clock-thread 负责）
- 不改 `_data_loop` 的 chunk size（snapshot 数据量小，65KB 足够）
- 不改 `config.py`、`writer.py`、`flusher.py`
- 不引入 asyncio 或第三方库

## 7. 评审记录（2026-05-27）

### 7.1 三方审阅结论

| 审阅者 | 主要关注点 | 结论 |
|--------|-----------|------|
| 审阅 A | `_maybe_refresh_code` 放置位置 | 保留在 drain loop 内（零开销 + 架构一致性） |
| 审阅 B | drain_count 保护必要性 | 必须加（checkpoint 更新 + 错误检测 + lock 响应性） |
| 审阅 C | 1524 分钟根因分析 | 与 1128/1129 完全同构（watermark 跳跃机制相同） |

### 7.2 关键论证结果

1. **`_maybe_refresh_code` 放置**：保留在 drain loop 内。`time.monotonic()` ~100ns 调用开销，实际刷新由 `code_refresh_sec` 节流。与 `_order_loop` 每轮做维护的模式一致。

2. **`drain_count` 阈值与动作**：阈值 1000（~65K records），仅 `break` 不做 flush。原因：snapshot flush 是 clock-thread 职责；break 保证 checkpoint 每秒级更新，且 `_check_order_thread_error()` 能及时响应。

3. **1524 根因**：1525-1529 分钟无 snapshot 数据（仅 order 数据），watermark 从 1524 直接跳到 1530。1524 在 watermark=1530 时被判定过期并 flush，此时仅读了 168 条。机制与 1128/1129（lunch break 1130 导致 watermark 跳跃）完全同构。

### 7.3 二轮审阅（修正后复查）

| 审阅者 | 角色 | 结论 |
|--------|------|------|
| A | 逻辑正确性 & 代码一致性 | Pass — 无 P0，2 条 P1 建议（stop 延迟措辞、_maybe_refresh_code 位置变更说明） |
| B | 数据完整性 & 生产安全 | Pass — 可实施，无 P0/P1，3 条 P2 注意事项 |
| C | 完整性 & 可实现性 | YES — 可实施，文档自包含，§3.1 代码可直接替换 |

**二轮修正项：**
- §3.2.3 stop() 响应延迟 "<1s" → "远低于 join timeout（10s）"
- §3.2.2 标题 "保留" → "从外层循环移入"，补充位置变更说明
- §3.3 改动汇总补充 `_maybe_refresh_code` 位置变更
- §4.4 崩溃损失措辞精确化（"最大丢失" → "单轮 drain 最大未 checkpoint 数据量"）
- §3.1 补充 `list()` materialize 注释
- §7.3 本节

### 7.4 三轮审阅（终审）

| 审阅者 | 角色 | 结论 |
|--------|------|------|
| A | 代码正确性 | PASS — 无 P0/P1/P2，§3.1 代码可作为当前 `_data_loop` 直接替换 |
| B | 数据安全 | PASS — 可安全实施，无 P0/P1，3 条 P2 备注（极端时序窗口、stop timeout、finally 选择均已确认安全） |
| C | 可实现性 | YES — 可实施，文档自包含，§3.1 代码可直接复制粘贴 |

**三轮结论：无阻塞性问题，设计文档已通过终审，可进入实施。**
