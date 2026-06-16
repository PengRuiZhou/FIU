# Findings

## 项目上下文
- 设计文档位于 `docs/superpowers/specs/2026-05-20-jp-market-minute-bar-design.md`
- 技术栈：Python，使用 threading（数据线程+时钟线程），csv 模块，dataclasses
- 0519 真实数据：snapshot.csv 702MB，code.csv 3.96MB，4496 symbols，329 分钟

## 架构关键决策
- **纯时钟驱动输出**：数据到达时不触发输出，只有时钟线程检查 watermark
- **rb 模式读取**：checkpoint 是 byte offset，必须用 bytes 操作
- **frozen=True**：SnapshotRecord 不可变，浅拷贝安全
- **RLock**：可重入锁，保护共享状态
- **原子写入**：所有输出使用 .tmp + rename
- **Replay 模式**：单线程逐分钟处理，不依赖时钟，适合历史数据回放

## Snapshot 输出设计决策（与设计文档的差异）
- **不做 decimal 除法**：SnapshotRecord 存原始值（float 类型但值等于原始整数），输出保持原始数据不变
- **保留全部列**：输出包含所有原始字段 + name(seqno, update_flag)，不减少任何列
- **保留全部记录**：同一分钟内同一 symbol 的所有更新记录都输出，不丢弃中间记录
- **K 线计算在 OHLCVAggregate 内部做 decimal 除法**

## Order.csv 数据源
- 列映射：symbol(0), time(1), bidprice(2), bidsize(3), askprice(4), asksize(5), decimal?(6), rcvtime?(7)
- 实际 6-8 列，设计文档定义最小 6 列，最大 8 列
- 处理方式：按列位置索引、跳过 header、保留原始值、不做任何聚合，仅按 time 切分钟
- 不像 snapshot/code 需要关联 name，order 直接输出原始数据 + seqno
- raw_order_buffers 是 `Dict[str, List[OrderRecord]]`（按分钟聚合所有记录，不按 symbol 分组）
- 无 order.csv 文件时 ReplayEngine 不报错，正常输出 snapshot
- CSV header 列数不完整（snapshot header 18 列但数据 21 列），必须按列位置索引
- CSV 首行是 header（以 `symbol,` 开头），需自动跳过
- snapshot 实际 17-21 列，code 实际 7-17 列
- snapshot.csv 开头包含上一交易日收盘数据（time 为上一日日期），需按目标日期过滤
- code.csv 每个文件约 3.96MB，单次 `read_lines()` 只读 64KB chunk，必须循环读完
- time 字段 JST (UTC+9)，rcvtime 字段 CST (UTC+8)
- 同一 symbol 同一分钟内可能有多次更新（不同 lasttradeqty/totalvol）

## FileTailer 关键发现
- `read_lines()` 是生成器，每次调用只读一个 chunk 的完整行
- CodeTable.load() 和 Replay 数据收集都需要 `while True` 循环调用直到返回空
- code.csv 刷新同理，单次调用可能只读部分数据
- 新增 `line_offset` 属性：`read_lines()` 内部追踪每条 yield 行的 byte offset（`chunk_start_offset + pos`），用于 per-minute committed_offset 精度

## 并行 + 流式架构关键决策
- **snapshot/order 独立流式处理**：两者业务完全独立，不依赖 seqno 做跨类型全局排序
- **ReplayEngine 线程模型**：主线程 `_stream_snapshots()` + ThreadPoolExecutor 提交 `_stream_orders()`，stop_event 快速失败
- **Engine order 线程**：独立 `_order_loop`，不经过 SharedState，不争 lock，仅通过 `_file_states["order"]` + `_checkpoint_lock` 与 clock-thread 交互
- **committed_offset 语义**：order 文件写成功后才允许 checkpoint 记录对应 offset；重启后从旧 offset 重新读取
- **per-minute offset 精度**：`_OrderMinuteBuffer.line_end_offset` 保存该分钟最后一条记录的 offset，flush 成功后 `committed_offset` 只推进到该值
- **内存保护**：`MAX_PENDING_ORDER_MINUTES=3`，超限强制 flush 最早分钟并记录 WARNING
- **Flusher 不变**：Live 模式下 `raw_order_buffers` 始终为空，flusher 的 order 代码自然不触发

## Flusher Double-Flush Bug（2026-05-27 发现，已修复）

### 现象
Snapshot drain loop 端到端验证（data_simulator speed=100）中，`ClockWatermarkFlusher` 对 1524 分钟执行了两次 flush，第二次覆盖第一次的正确结果（764 rows 覆盖 44,588 rows）。

### 根因
`_flush_minutes_internal` 中 Lock block 1（pop buffer）到 Lock block 2（`flushed_snapshot_minutes.add()`）之间存在 ~3 秒竞态窗口（44K rows 写入耗时）。窗口内 data-thread 的 `process_snapshot` 将新数据写入 `ohlcv_buffers[1524]`（因为 `flushed_snapshot_minutes` 还没有 1524）。下一个 tick `_step3_minute_output` 检测到该分钟已过期，再次 flush → 覆盖。

### 修复方案：Step3 Late Re-route（已实施）
在 `_step3_minute_output` 中将 `expired_keys` 分为 `normal_keys`（未 flush → 正常 flush）和 `reflush_keys`（已 flush → buffer 数据路由到 late queue）。Step 4 通过 `append_snapshot_records` 追加到文件，不覆盖。

### 关键设计决策
- `already_flushed` 在第一个 `with self._state.lock` 块内计算（消除 TOCTOU）
- `ohlcv_buffers` 数据丢弃（第一次 flush 已含正确聚合），`raw_snapshot_buffers` 保留（路由到 late queue）
- `flush_all_remaining` 增加 `_step4_handle_late_records()` 处理 shutdown 前未处理的 late records
- stall flush / cross-day flush 是独立分支，不经过分流逻辑（触发时无竞态窗口）

### 数据完整性确认
- 4 轮 12 agent 审查：无覆盖路径、无静默丢失、无重复记录
- Order 数据完全独立（`_order_loop` 本地 buffer + 独立 `_flushed_order_minutes`）
- `raw_order_buffers` 在 Live 模式下始终为空（`process_order` 未被调用）

### 端到端验证结果（2026-05-27）
- **修复前**：1524 Live Y=107（被覆盖），Replay Y=42,868
- **修复后**：1524 Live Y=42,868 = Replay Y=42,868 — **完全匹配**
- **全量验证**：329 snapshot 文件 update_flag=Y 计数全部一致，Y 原始记录以 (symbol,time,price,vol) 为 key 0 missing / 0 extra
- 202 tests passed（11 new），0 regressions
- **Replay summary**：`replay_summary_{date}.json` 记录各类型分钟范围和文件数量，仅成功时生成
- **Replay 第一版同步等待**：`write_executor` 提交 snapshot/kline 写任务后同步等待 `f.result()`，不积压异步

## 延迟 flush 策略（偏离原设计）
- **原设计假设**"输入文件按 time/rcvtime 基本有序"→ **实际不成立**：0519 数据按 symbol 分组写入，不按 time 全局排序
- 即时 flush 导致 130 万条 late data 警告，丢失 ~90% 数据
- **修复**：`DELAYED_FLUSH_ROUNDS=3`，每轮 poll 后只 flush 不再活跃的分钟
- 内存从 ~40MB 增加到 ~120MB（保留 3 轮分钟 buffer），仍远低于旧架构 3-5GB
- Live 模式不受影响（Live 由 watermark 驱动 flush，数据天然按时间到达）

## 延迟 flush EOF 丢数据问题（已修复）

### 修复方案
采用方案 B（EOF/实时 追加迟到 raw records 到已有文件），已实现为 **Live + Replay 统一迟到记录处理机制**（Phase 10）。

### 修复后的数据完整性
- Snapshot: 5,550,002 条有效输入 → 5,550,002 条输出（**0 丢失**）
- Order: 87,783,573 条有效输入 → 87,783,573 条输出（**0 丢失**）

### 实现要点
- **writer.py**：新增 `append_order_records` / `append_snapshot_records`，文件级写锁（`_get_write_lock`）防止并发 append + atomic_write 冲突
- **aggregator.py**：`process_snapshot` 检测 `minute_key in flushed_snapshot_minutes` → 路由到 `_late_snapshot_records`（在 `SharedState.lock` 内原子完成）
- **flusher.py**：新增 `_step4_handle_late_records`（pop late records → append 到文件 → update latest_snapshot）；checkpoint 移到 late append 之后（`_step5_write_checkpoint`），确保 late records 落盘后才推进 snapshot offset
- **engine.py**：`_order_loop` 中检测 `_flushed_order_minutes` → 直接 append；`recover_flushed_minutes` 从输出目录重建 flushed_minutes（重启恢复）
- **replay.py**：两个 stream 增加迟到检测（`minute_key in flushed_minutes` → 收集到 `late_records_by_minute` → 每轮 poll 结束后 flush）；EOF flush 兜底处理 residual buffer
- **snapshot_minute 同 symbol 多行规则**：late append 后同一 symbol 可同时有 `update_flag=N`（原 carry-forward）和 `update_flag=Y`（late record），不是错误，下游按 `update_flag` 解释

### 原问题描述（保留供参考）

### 现象
0519 真实数据 replay 后，精确计数发现少量记录丢失：
- Snapshot: 5,550,002 条有效输入 → 5,544,792 条输出，**丢失 5,210 条**（0.094%）
- Order: 87,783,573 条有效输入 → 87,781,755 条输出，**丢失 1,818 条**（0.002%）

### 根因分析
1. 输入数据按 **symbol 分组写入**，不按 time 全局排序
2. 延迟 flush 策略（`DELAYED_FLUSH_ROUNDS=3`）在分钟不再活跃时 flush 并 **pop 掉 buffer**
3. 之后同一分钟的迟到数据到达时，buffer 会重新创建（fresh entries）
4. EOF flush 时 `if mk not in flushed_minutes` 检查跳过了这些已被 flush 过的分钟
5. 迟到数据残留在 buffer 中但从未写出

### 代码位置
- `replay.py` `_stream_snapshots()` 第 135-142 行：EOF flush 跳过已 flush 分钟
- `replay.py` `_stream_orders()` 第 259-268 行：同上
- `replay.py` `_flush_snapshot_minute()` 第 183-188 行：pop buffer 清除数据

### 数据验证结果
- 0 条解析失败（parse_snapshot_line / parse_order_line 全部成功）
- 0 条校验失败（validate_snapshot 全部通过）
- 唯一的过滤是日期过滤：snapshot 跳过 14,383 条上一交易日数据，order 跳过 4,317 条
- 排除日期过滤后，丢失记录全部来自延迟 flush EOF 边界条件

### 修复方案选项
| 方案 | Snapshot | Order | 代价 |
|------|----------|-------|------|
| A: 不 pop buffer，EOF 统一 flush | 内存 ~2-3GB（5.5M records） | 内存 ~15-20GB（87.7M records），不可行 | Snapshot 可行，Order 内存爆炸 |
| B: EOF 时 append 迟到数据到已有文件 | 需追加 raw records + 更新 carry-forward | 追加独立 records，简单 | Snapshot 复杂（carry-forward 状态），Order 简单 |
| C: 增大 DELAYED_FLUSH_ROUNDS | 减少丢数据概率但不保证 | 同左 | 内存线性增长，仍有极端迟到数据风险 |
| D: 接受 <0.1% 丢失 | - | - | 数据不完全精确 |

### 建议
- ~~Snapshot 采用方案 B：EOF 时 append 迟到 raw records 到已有文件（迟到数据以 update_flag="Y" 追加）~~
- ~~Order 采用方案 B：EOF 时 append 迟到 records 到已有文件（records 独立，append 简单）~~
- ~~两者都需要修改 `writer.py` 支持 append 模式~~
- **已全部实现**（Phase 10，见上方修复方案）

## 迟到记录处理架构决策

### Live 模式迟到检测
- **Snapshot**：在 `process_snapshot` 内检测（`SharedState.lock` 保护，与 `flushed_snapshot_minutes` 原子判断）
- **Order**：在 `_order_loop` 内检测（`_flushed_order_minutes` 是 Engine 字段，order 线程独占访问）
- **为什么 snapshot 用 SharedState 字段而 order 用 Engine 字段？**
  - Snapshot 的 flushed_minutes 由 flusher（clock 线程）写入、data 线程读取，需 SharedState.lock 保护
  - Order 的 flushed_minutes 由 order 线程自己写入和读取，无跨线程竞争

### Checkpoint 安全
- **核心原则**：late record 只有在 append 成功后才允许推进对应输入 offset
- **实现**：checkpoint 写入从 `_step3_minute_output` 移到新的 `_step5_write_checkpoint`（在 `_step4_handle_late_records` 之后）
- **保护机制**：`_step5_write_checkpoint` 检查 `_late_snapshot_records` 是否为空，非空则跳过本轮 checkpoint
- **跨日场景**：跨日 flush 后立即写 checkpoint（在清理状态之前），避免重启后重复输出

### Live 重启恢复
- `recover_flushed_minutes` 从输出目录扫描已有文件重建 `flushed_snapshot_minutes` / `_flushed_order_minutes`
- 选择扫描文件而非持久化到 checkpoint 的理由：flushed_minutes 与实际输出文件状态保持一致，避免 checkpoint 与文件不一致
- Replay 不需要此逻辑（每次从空 flushed_minutes 开始）

### Snapshot checkpoint 安全约束
- 如果 data thread 已读到 late record 并放入 `_late_snapshot_records`，FileTailer 的 read offset 可能已前进
- CheckpointManager 写入前必须确认 `_late_snapshot_records` 为空，否则重启后 late queue 丢失且 offset 已跳过

### 文件级写锁
- `_write_locks: Dict[str, threading.Lock]` 按文件路径粒度加锁
- `atomic_write` 和 `append_*_records` 共用同一锁，防止并发写损坏
- 不同文件路径使用不同锁，无不必要的序列化

## 参考文档
- `docs/FIU金融数据服务日股的ws说明文档.pdf`
- `docs/FIU多源行情系统_接收服务_东南亚股票市场(日本行情)_json&pb_V1.6_20250331.pdf`

## 数据模拟器（Data Simulator）关键发现

### 设计动机
- Live 模式无法用静态文件充分测试：FileTailer 的增量读取、半行拼接、多线程并发都需要动态写入来验证
- 真实 FIU 接收程序持续 append 数据，静态文件一次性就绪的 replay 模式无法覆盖这些场景

### 关键设计决策
- **默认保留源文件原始顺序**（`--order-mode original`）：真实 FIU 文件不保证按 time 全局排序，按 symbol 分批写入。如果模拟器排序后再写出，会掩盖乱序问题，测不出 late record
- **code 默认 preload**：code.csv 是码表不是行情流，启动时一次性写入更真实（FIU 接收程序在交易日开始前就有完整码表）
- **共享 global_min_time**：所有线程从同一时间基准计算回放进度，保证 order/snapshot 之间的相对时间关系真实。global_min_time 只从 order/snapshot 的 time 字段计算，code 的第 2 列是 market 不是 time
- **流式处理**（original 模式）：46M 行 order.csv 约 1.4GB，不能全量加载到内存。original 模式逐行读取+写入，仅 time 模式需要预加载排序

### 并发安全
- **文件级写锁**：late writer 和主线程共享同一文件句柄，必须加锁防止交错写入
- **split line 锁跨 sleep**：半行写入的前半段和后半段之间有 delay（模拟真实 append 过程中的部分写入），必须持锁跨 sleep，否则 late writer 在两段之间插入完整行产生 bad line
- **stop() 防重入**：`run()` 正常退出和外部 `stop()` 调用都会触发清理，加 `if self._stop_event.is_set(): return` 防止重复关闭

### 时间戳处理
- order/snapshot 的 time 字段是 17 位数字（YYYYMMDDHHMMSSMMM），单位是毫秒的千分之一
- 计算回放等待时间时需除以 1,000,000,000 转为秒
- original 模式下 time 可能倒退（源文件乱序），sleep_time < 0 时直接写入不等待

### Windows CRLF 问题（已修复）
- Python 在 Windows 上 `open(path, "w")` 默认将 `\n` 转为 `\r\n`
- 源文件用 LF 换行，输出必须保持一致：所有 `open()` 调用加 `newline=""` 参数
- 涉及 4 处：输出文件打开、preload code 源文件/目标文件、流式读取源文件、时间戳扫描源文件
- 修复后 code.csv 逐字节 IDENTICAL，order/snapshot 行结尾均为 LF

### split-line 概率对性能的影响
- split-line delay 是真实毫秒（不受 `--speed` 影响）
- `split-line-prob=0.01` 在 65M 行 order 文件上 = 650K 次 × 50ms = **~9 小时**额外延迟
- 合理值：`--split-line-prob 0.0001`（约 6,500 次 × 50ms = ~5 分钟）
- 对 snapshot 影响小（4.7M 行 × 0.0001 = ~470 次 × 50ms = ~23 秒）

## Live 模式端到端测试发现的问题（2026-05-22）

### 测试配置
- Simulator: `--speed 100 --split-line-prob 0.0001 --late-prob 0.001 --file-types order,snapshot,code`
- Minute bar: `config/config-live-test.ini`（input_dir=test/output, output_dir=test/live_output）
- 输入文件：`order.csv.20260522`（65M行）、`snapshot.csv.20260522`（4.7M行）、`code.csv.20260522`（14K行）

### Bug 1: 前一日数据未过滤（严重）

**现象：**
- 输入文件 `*.csv.20260522`，但生成了大量 20260521 日期的输出文件
- snapshot: 185 个文件中 182 个是 20260521 日期
- order: 13 个文件中 11 个是 20260521 日期
- `snapshot_minute_20260522_0900.csv` 第一行 time=`20260521153000000`（前一日收盘数据混入当日文件）

**根因：**
- `order.csv.20260522` 和 `snapshot.csv.20260522` 文件开头包含前一交易日收盘数据（time=20260521153000000）
- Live 模式的 `process_snapshot()` 和 `_order_loop` 没有按文件日期过滤数据
- Replay 模式有日期过滤（只处理目标日期数据），Live 模式缺少此逻辑

**影响：**
- 前一日数据被正常处理，`First data received at minute=202605211530`
- 前一日分钟被 flush 后进入 `flushed_snapshot_minutes` / `_flushed_order_minutes`
- 后续当日数据因同 symbol 被前一日 carry-forward 覆盖，大量进入 late 路径

**修复方向：**
- Live 模式应在 `process_snapshot` 和 `_order_loop` 入口按 FileTailer 的当前文件日期过滤数据
- 或在 `parse_snapshot_line` / `parse_order_line` 之后增加日期校验：只处理 `record_date == file_date` 的数据
- 与 Replay 模式共享同一过滤逻辑

### Bug 2: Snapshot 被 late record 持续阻塞（严重）

**现象：**
- 每 5 秒 flush 约 500 条 late snapshot records 到同一个 minute
- 持续不停，从 16:57 一直重复到 17:04（进程被强杀时仍在继续）
- snapshot 输出只到 20260522_0900 为止，不再推进

**根因：**
- Bug 1 导致前一日数据被 flush，大量分钟进入 `flushed_snapshot_minutes`
- 当日数据到达时，`process_snapshot` 检测 `minute_key in flushed_snapshot_minutes` → 路由到 `_late_snapshot_records`
- Flusher 的 `_step4_handle_late_records` 将 late records append 到对应文件
- 但 `latest_snapshot` 被前一日数据占据（preclose/close 等值是 20260521 的），当日数据的更新不断触发 late append
- 形成死循环：每轮 tick 都有 ~500 条 late records，flush 后 next tick 又有新的 late records

**影响：**
- Snapshot 处理完全卡住，无法推进到新的分钟
- 大量无意义的 late append IO 操作
- `latest_snapshot` 状态被前一日数据污染

**修复方向：**
- 修复 Bug 1 后，前一日数据不再被处理，此 Bug 自动消除
- 额外保护：late record 应有速率限制或总量限制，避免无限循环

### Bug 3: Order 处理卡在 0800/0801（严重）

**现象：**
- Order 只输出了 `20260522_0800`（769 records）和 `20260522_0801`（769 records）两个文件
- 之后不再有新的 order 输出

**根因：**
- 与 Bug 2 类似：前一日数据产生了 20260521 的 order 分钟并 flush
- 当日 0800/0801 正常处理（可能是首个分钟，尚未被前一日污染）
- 后续分钟的 order 数据因 `minute_key in _flushed_order_minutes` 判断进入 late 路径
- `MAX_PENDING_ORDER_MINUTES=3` 限制导致缓冲区快速被清空，所有数据都走 late append

**修复方向：**
- 同 Bug 1，修复日期过滤后自动解决

### Bug 4: Code table 刷新过于频繁（轻微）

**现象：**
- `code_refresh_sec=30`，但日志显示每 30 秒都在刷新 code table
- 每次刷新增加数百 symbols（`+699 symbols, +663 symbols, +645 symbols...`）

**原因：**
- Code table 使用 `add_new_symbols` 追加新发现的 symbols
- 日志中的 `+N symbols` 是本次 scan 新发现的 symbols（可能是按 symbol 排序写入的 code.csv，每次读到新的 position）
- 这是正常行为，不是 Bug

### Bug 5: Invalid time field 数据（数据质量，非代码 Bug）

**现象：**
- 8,992 个 `Invalid time field (not 17 digits): 0` 错误
- 涉及 symbol 如 1439, 1451, 1464, 9S63, 9S64, 9997 等

**原因：**
- 真实数据中部分 symbols 的 time 字段为 0（可能是停牌、退市或特殊状态的 symbols）
- Validator 正确识别并跳过这些记录，不影响处理

### 测试统计

| 指标 | 数值 |
|------|------|
| 运行时长 | ~8 分钟（16:55:37 - 17:04:25） |
| Snapshot 文件 | 185 个（182 个 20260521 + 3 个 20260522） |
| Order 文件 | 13 个（11 个 20260521 + 2 个 20260522） |
| Late flush 次数 | 96 次 |
| Late records/flush | ~500 条（稳定） |
| Validator errors | 8,992 个（time=0 的无效数据） |
| Code table symbols | 4,498 |
| Cross-day reset | 1 次（16:57:05 检测到 20260522） |

### 修复优先级

| 优先级 | Bug | 影响 | 修复复杂度 |
|--------|-----|------|-----------|
| P0 | Bug 1: 日期过滤缺失 | 导致 Bug 2、3 的根因 | 中等（加日期过滤） |
| P0 | Bug 2: Late snapshot 死循环 | Snapshot 完全卡住 | 修复 Bug 1 后自动解决 |
| P0 | Bug 3: Order 卡住 | Order 停止产出 | 修复 Bug 1 后自动解决 |
| P2 | Bug 5: Invalid time | 数据质量，已正确跳过 | 无需修复 |

## Bug 1 修复详情（2026-05-22 实施）

### 修复内容
1. **日期过滤**：`engine.py` 的 `_data_loop` 和 `_order_loop` 增加 `record_date != target_date` 过滤，与 Replay 模式共享同一逻辑
2. **Late record 队列上限**：`aggregator.py` 增加 `MAX_LATE_SNAPSHOT_QUEUE_SIZE=50000`，`engine.py` 增加 `MAX_LATE_ORDER_RECORDS_PER_MINUTE=50000`，防止未知边界条件导致无限 late record 循环
3. **Checkpoint 写入重试**：`checkpoint.py` 的 `os.replace()` 增加 5 次重试（每次间隔 100ms），修复 Windows 上 PermissionError 导致进程崩溃

### 新增测试
- `test_aggregator.py::TestSharedStateLateRecords::test_late_snapshot_queue_limit`：验证超限丢弃行为

---

## Live 模式测试环境问题与改进（2026-05-25）

### 问题 1: target_date 配置缺失

**现象：** 5月25日无法运行 5月22日的测试数据，FileTailer 查找 `snapshot.csv.20260525` 但文件名是 `snapshot.csv.20260522`。

**根因：** Live 模式用 `jst_now_yyyymmdd()` 决定读取哪个日期的文件，但测试数据日期与当前日期不同。

**修复：** `[input]` section 新增 `target_date` 配置项（可选，默认为空则使用 `jst_now_yyyymmdd()`）。Replay 模式从命令行参数拿 date，Live 模式从 config 拿 target_date。

```ini
[input]
csv_dir = D:/FIU/test/output
target_date = 20260522   # 可选，不设置则用当天日期
```

**影响文件：** `config.py`（InputConfig + load_config）、`engine.py`（`_get_target_date()` 方法）

### 问题 2: Watermark flush 与加速测试数据的不匹配（核心问题）

**现象：**
- Snapshot 0900 之前的输出质量极差：0800 文件只包含 ~500 个 symbol（应该 4496），0830 文件 99% 是 carry-forward
- 持续大量 late record flush（~2500 条/秒），大量 IO 开销
- Order 输出过慢（每分钟数百条而非数万条）

**根因分析：**

`is_expired()` 使用 **真实 JST 时钟** 对比数据时间，判断分钟是否过期：

```python
def is_expired(minute_key, delay_sec):
    return now_jst() >= minute_key + 1min + delay_sec
```

当真实时间（如 10:10 JST）远大于数据时间（0800 JST），所有分钟在 buffer 出现的**瞬间就被判为过期**。

**数据到达 vs flush 的时间线（speed=100 测试）：**

```
源数据实际按时间分组（非按 symbol 排序）：
  - minute 0800: 4449 symbols 连续排列（line 13504-17952）
  - minute 0830: 47 symbols
  - minute 0900: 59521 records
  ...

FileTailer 每次读 64KB ≈ ~500 条 → 读完 0800 块需 ~9 chunks ≈ ~1.8s

T+0.0s  Data thread 读 chunk 1 (~500 symbols of 0800) → buffer[0800] 有 500 symbols
T+1.0s  Clock thread tick:
          is_expired(0800): now_jst(10:10) >> 0800+5s → TRUE → FLUSH!
          → 0800 只包含 500/4449 symbols ← 质量极差
T+1.2s  Data thread 读 chunk 2 (~500 more symbols of 0800)
          → 0800 已在 flushed_snapshot_minutes → LATE RECORD!
T+1.4s  ... 继续 late record
T+1.8s  读完 0800 全部 4449 symbols（但只有前 500 正常输出，其余 3949 都是 late）
```

**生产环境为什么没问题：**
- 真实时间 08:00:05 时 `is_expired(0800)` = TRUE（合理）
- 此时所有 4496 symbols 的 0800 数据已通过实时网络到达，buffer 完整
- 真实时钟 ≈ 数据时钟，flush 时机正确

### 数据驱动 Flush 方案（Data-Driven Watermark）

**核心思路：** 用 data thread 的 `current_minute`（数据处理进度）替代真实时钟作为 flush watermark。真实时钟仅作 fallback 兜底。

**方案对比：**

| 方案 | 优点 | 缺点 |
|------|------|------|
| A: 数据驱动 flush（推荐） | 无需配置 speed，任何速率自动适配，生产/测试统一 | 需改动 flusher flush 判定逻辑 |
| B: 虚拟时钟加速 | 改动较小 | 需手动同步 simulator 和 minute bar 的 speed |
| C: 不改，分测 | 零改动 | 无法用加速数据验证 live 端到端 |

**数据驱动 flush 详细设计：**

```
Clock thread tick:
  data_watermark = state.current_minute   ← data thread 的处理进度

  # 主逻辑：数据驱动判定
  for minute_key in ohlcv_buffers:
    if data_watermark > minute_key:          ← 数据已推进到下一分钟
      flush(minute_key)

  # Fallback：真实时钟兜底（60s 无新数据强制 flush）
  for minute_key in ohlcv_buffers:
    if now_jst() >= minute_key + 1min + 60s:
      flush(minute_key)
```

**三种场景验证：**

1. **生产环境（实时数据）：**
   - data thread 实时处理 → `current_minute` 实时推进
   - 0800 数据全部到达后，`current_minute` 推进到 0801 → flush 0800
   - 时机与现有真实时钟方案一致

2. **测试环境（speed=100）：**
   - data thread 快速读完 0800 块（~1.8s）→ `current_minute` = 0800
   - 读到 0830 数据 → `current_minute` = 0830 → flush 0800（此时已包含全部 4449 symbols）
   - flush 跟随数据速度自动适配

3. **异常场景（data thread 卡死）：**
   - `current_minute` 停止推进 → 数据驱动判定永远不触发
   - 60s 后 fallback 真实时钟触发强制 flush → 安全兜底

**影响范围：**

| 组件 | 改动 |
|------|------|
| `is_expired()` | 新增 `data_watermark` 参数，优先用数据进度判断 |
| `flusher._step3_minute_output()` | 传入 `state.current_minute` 作为 data_watermark |
| `engine._flush_expired_order_minutes()` | 传入 order 的 `current_minute` |
| `clock.py` | 可能需要新增 `is_expired_by_data()` 函数 |
| config | 无需新增配置 |

**待确认：**
- `output_delay_sec` 在数据驱动模式下是否还需要？（用于等待可能的 late data）
- order 的 `current_minute` 跟踪方式是否与 snapshot 一致？
- fallback 超时时间是否 60s 合适？

## Data-Driven Watermark Flush 实现总结（2026-05-25）

### 核心问题
Live 模式使用真实时钟 `now_jst()` 判断 flush 时机。测试环境加速回放时（speed=100），真实时钟远超数据时间，导致分钟数据尚未完整就被 flush（如 0800 只包含 500/4449 symbols），后续数据全部走 late 路径。

### 解决方案
用 data-thread 的 `current_minute`（数据实际处理进度）替代 `now_jst()` 作为 flush 主判定。真实时钟作为可配置 fallback（`enable_time_fallback`，默认 true）。

### 关键设计决策

1. **OR 架构**：数据驱动始终启用 + 真实时钟 fallback 可配置。两个条件是 OR 关系，任一满足即 flush
2. **`is_data_driven_expired`**：纯函数 `watermark >= minute_key + delay_minutes`，`minute_key_to_start_time` 使用 `datetime.strptime(minute_key, "%Y%m%d%H%M")` 一步完成格式+语义校验
3. **`_flush_minutes_internal`**：共享 flush 方法，lock 内 pop → lock 外 IO → lock 内更新状态。`_step3` 和 `flush_all_remaining` 复用
4. **`current_minute` 单调递增**：`YYYYMMDDHHMM` 字符串 `>` 比较（字典序=时间序）。`aggregator.process_snapshot` 和 `_order_loop` 均使用条件赋值
5. **跨日策略**：SharedState 用零点基准条件赋值（`current_date + "0000"`，防跨线程竞态），Order loop 用 None（局部变量无竞态）+ try-finally 清理旧日期 buffer
6. **`stop()` 异常安全**：flush_error 保留原始 Exception 对象（非字符串）；`from flush_error` 链式传播；资源独立 try-except；join 超时 `sys._current_frames()` dump 线程栈
7. **`flush_all_remaining`**：`except Exception`（非 RuntimeError）防止 SystemExit 绕过 checkpoint；flush 错误与 checkpoint 错误独立捕获
8. **Watermark 停滞检测**：`time.monotonic()` wall-clock（非 tick 计数），`_stall_warned` 标志保证单次告警，恢复时输出 INFO 日志
9. **Flush 触发来源日志**：lock 内构建 data_driven_keys / fallback_keys，data-driven 用 INFO，fallback 用 WARNING
10. **配置**：`data_flush_delay_minutes`（默认 1，非负校验，>10 WARNING）、`enable_time_fallback`（默认 true，false 时启动 WARNING）

### 评审历程
设计文档经 6 轮并行 agent 评审（每轮 3 个 agent，共 18 个 agent 审查），前 5 轮修复了 ~15 个 Critical、~25 个 Major、~20 个 Minor 问题。第 6 轮三个 agent 均确认无需修改。收敛轨迹：算法缺陷 → 实现安全 → 文档一致性 → 防御性编程 → 理论边界 → 无新问题。

## Data-Driven Watermark Flush Live 测试问题（2026-05-25）

### 测试配置
- Simulator: `--speed 100 --file-types order,snapshot,code`
- Minute bar: `config-watermark-test.ini`（`enable_time_fallback = false`, `data_flush_delay_minutes = 1`）

### 问题 1（P1）: Snapshot 中出现"未来"数据

**现象**：
- `0830.csv` 全部是 `0900` 时间戳的数据
- `1516.csv` 中出现 `1517` 的数据
- `1524.csv` 中出现 `1530` 的数据

**根因**：
`latest_snapshot` carry-forward 机制。`flusher.py` 在 `_step3_minute_output` 中 flush 时，先复制 `snapshot_copy = dict(self._state.latest_snapshot)`。此时 `latest_snapshot` 包含截至 flush 时刻 data-thread 已处理的所有 symbol 最新值。

在 speed=100 加速模式下，data-driven watermark 跳跃式推进（可能一次跳多个分钟），导致：
1. flush 0830 分钟时，`latest_snapshot` 已包含 0900 的数据（data-thread 处理进度远超 0830）
2. `write_snapshot_file` 使用 `_minute_end_threshold` 过滤 carry-forward 行（`rec.time < minute_end`），但 `latest_snapshot` 的值本身就是 0900 的值，time 字段 > 0830 的 minute_end → 被 0830.csv 包含

**本质**：这不是 data-driven watermark 的 bug，而是 carry-forward 机制在加速测试下的预期行为。生产环境（speed=1）watermark 每次只推进 1 分钟，carry-forward 数据几乎总是上一分钟的。

### 问题 2（P2）: 1530.csv 未正常生成

**现象**：
- `snapshot_minute_1530.csv` 和 `order_minute_1530.csv` 未生成

**根因**：
`enable_time_fallback = false` 配置下，flush 完全依赖 data-driven 判定。`is_data_driven_expired` 要求 `watermark >= minute_key + delay_minutes`。1530 是交易日最后一分钟，之后没有新数据推进 watermark，因此 1530 永远不会满足 data-driven 过期条件。

`flush_all_remaining()` 在 `stop()` 中被调用，理论上应该 flush 残留 buffer。但如果 simulator 进程先退出，minute_bar 进程可能收到 EOF 后直接退出，未触发 final flush。

**修复方向**：
1. 确保 `stop()` 中 `flush_all_remaining()` 在所有线程 join 后无条件执行
2. 或者对交易日最后一分钟（1530）特殊处理，EOF 时无条件 flush

**2026-05-26 更新：Issue #2 和 #3 已在 Phase 13 全部修复。** Issue #2 修复：Per-minute snapshot + writer 时间过滤。Issue #3 修复：Stall-triggered flush（`stall_flush_sec` 可配置）。Order 线程同类问题一并修复。Live 验证：1530 正常生成，0 条 carry-forward 未来数据。详见 `docs/superpowers/specs/2026-05-25-carry-forward-future-data-fix.md`。

### 问题 3（P2）: 0913 之前大量 symbol 的 name 缺失

**现象**：
- 0830-0912 的 snapshot 文件中，大量 symbol 的 `name` 列为空

**根因**：
`code_refresh_sec = 30`（默认值），即每 30 秒真实时间刷新一次 code table。在 speed=100 模式下，30 秒真实时间 = 50 分钟数据时间。code table 在启动时 preload 一次，之后每 30 秒刷新，但 0830-0912 期间可能只刷新了 0-1 次。

code.csv 的数据在文件中是按时间追加的，`CodeTable.refresh()` 通过 FileTailer 读取增量数据。如果 refresh 间隔太长，中间时间段的数据对应的 symbol 就没有 name 信息。

**修复方向**：
1. 测试环境使用更小的 `code_refresh_sec`（如 1 秒）
2. 或 simulator 在 speed 模式下考虑 code 的刷新频率缩放

**2026-05-26 更新：已验证修复。** `config-watermark-test.ini` 设置 `code_refresh_sec=1`，0910 起填充率达 100%。

## Order 线程性能问题（2026-05-27）

### 现象
实盘测试发现 order 分钟文件生成速度严重滞后：

| 时段 | 文件大小 | 生成延迟 |
|------|---------|---------|
| 盘前 (0800-0859) | 0.2–5.7 MB | ~1 min（正常） |
| 开盘 0900 | 58 MB | 3 min 落后 |
| 盘中平均 | 30–40 MB | 逐分钟累积 |
| 1005 | 30 MB | 57 min 落后 |

Snapshot 分钟文件生成正常，不受影响。

### 根因分析

#### 读取端：吞吐量不足（根本原因）

`_order_loop` 每次迭代读取一个 65KB chunk，然后固定 sleep 200ms：
- **吞吐量上限：** 65KB / 0.2s = 325 KB/s = **19.5 MB/min**
- **市场时段需：** 30–60 MB/min
- **缺口：** 10–40 MB/min，逐分钟累积

盘前不受影响因为数据量小（< 6 MB/min），远低于上限。

Snapshot 不受影响因为 snapshot 数据只有 ~5 MB/min，远低于 19.5 MB/min 上限。

#### 写入端：同步写阻塞读取（加剧因素）

`write_order_file` 当前实现：构建 500K+ 字符串列表 → `"\n".join(lines)` → 50MB 巨字符串 → `atomic_write`（write + fsync + rename），总耗时 0.8–2.6s，期间零读取。

fsync 是主因：强制刷盘，50MB 文件在典型服务器上需 0.5–2s。

### 设计方案（Phase 14）

**Phase 1（~30 行改动，解决 90% 问题）：**

1. **Drain loop**：有数据时连续读取（不 sleep），无数据时走配置间隔。每 100 次迭代执行保护检查（过期分钟 flush + 内存保护）
2. **512KB chunk**：`order_chunk_size_bytes=524288`（默认 65KB 向后兼容）
3. **Streaming write**：逐行 `f.write` 替代 `"\n".join()`，1MB I/O buffer 减少 syscall
4. **去 fsync**：Order 文件是派生数据可 replay 重生成，崩溃丢失最后 1-2 分钟可补回
5. **保留 `_get_write_lock`**：当前无竞态（单线程顺序执行），防御性保留（`raw_order_buffers` 管线预留）

**性能预期：**
- 读取吞吐量：19.5 MB/min → 50-100 MB/min（5x+）
- 单次写入 50MB：0.8-2.6s → 0.15-0.4s（3-8x）
- 0900 延迟：3 min → < 10s
- 峰值内存：~475 MB → ~375 MB（略降，去除 join 巨字符串分配）

**Phase 2（可选，~100 行）：** Producer-Consumer 读写分离（Reader 线程 + Writer 线程 + Queue），仅在极端硬件（网络盘、慢 HDD）上 Phase 1 不够时实施。

### 设计评审历程
- 3 Agent × 4 轮评审（共 12 个 agent 审查）
- 第 1 轮：2 CRITICAL + 4 IMPORTANT（C2 vs I3 矛盾：锁保留 vs 移除）
- 派遣 3 个补充 agent 分析：一致确认无竞态，推荐保留锁作防御性措施
- 第 2-4 轮：逐步收敛，最终 0 CRITICAL / 0 IMPORTANT

### 不做的事
- 不改 `FileTailer.read_lines()` 接口
- 不改 `atomic_write` 函数（snapshot/kline 仍使用）
- 不改 `append_order_records`（late order append 不变）
- 不改 `flusher.py`、`aggregator.py`、`csv_parser.py`、`replay.py`
- 不引入 asyncio 或第三方库

## Order 线程性能优化实施结果（2026-05-27）

### 改动文件
| 文件 | 改动 | 行数 |
|------|------|------|
| `config.py` `InputConfig` | 新增 `order_chunk_size_bytes` 字段 + 解析 | ~3 行 |
| `engine.py` `__init__` | order tailer 使用 `order_chunk_size_bytes` | 1 行 |
| `engine.py` `_order_loop` | drain loop + 每 100 次保护检查 + adaptive sleep | ~15 行 |
| `writer.py` `write_order_file` | streaming write + 保留锁 + 去 fsync + 1MB buffer | ~15 行 |
| `config/production.ini` | 新增 `order_chunk_size_bytes = 524288` | 1 行 |

### 新增测试（10 个）
- `test_order_drain.py`（5 个）：drain loop sleep 行为、连续读取、100 次保护检查、分钟推进 flush
- `test_writer.py` `TestStreamingWriteOrderFile`（3 个）：输出一致性、锁获取、无 tmp 残留
- `test_config.py`（2 个）：`order_chunk_size_bytes` 解析 + 默认值

### 端到端验证

#### 测试配置
- Simulator: `--speed 100 --source-dir input --output-dir test/output --date 20260525`
- Minute bar live: `config/test-order-live.ini`（`order_chunk_size_bytes = 524288`, `enable_time_fallback = false`）
- Minute bar replay: `config/test-order-replay.ini`（`csv_dir = D:/FIU/input`, `output_dir = D:/FIU/test/replay_output`）
- 输入数据：`order.csv.20260525`（4.4GB, 70,809,006 行）、`snapshot.csv.20260525`（639MB）、`code.csv.20260525`（3.8MB）

#### Order 结果
| 检查项 | 结果 |
|--------|------|
| Live 文件数 | 417（0800–1530） |
| Replay 文件数 | 417（0800–1530） |
| Live 总记录 | 70,809,005 |
| Replay 总记录 | 70,809,005 |
| Live vs Replay 差异 | **0**（逐文件记录数完全一致） |
| Live vs Replay 内容 | **0 差异行**（0900: 694,731 行全比对、0906: 625,896 行全比对） |
| 源数据行数 | 70,809,006（header + 70,809,005 data） |
| 数据丢失 | **0** |
| Order 生成延迟 | **全程无积压**（0900 开盘 694K records 实时生成，0 延迟） |

#### Snapshot 结果（Phase 15b 修复后）
| 检查项 | 结果 |
|--------|------|
| 文件数 | 329 live vs 329 replay |
| **update_flag=Y** | **全部 329 个文件一致** |
| **1524 行数** | Live 44,696 = Replay 44,696（Phase 15b 修复前 Live=781） |
| Y 原始记录 | **0 missing, 0 extra**（key: symbol+time+price+vol） |
| 19 个 name 差异 | 仅 session 开头分钟 code name 列，非数据问题 |

## Snapshot `_data_loop` Drain Loop 缺失问题（2026-05-27 发现，Phase 15a 已修复）

### 现象
端到端测试（data_simulator speed=100）发现 3 个 snapshot 分钟文件的 update_flag=Y 记录数不一致：

| 文件 | Live Y 数 | Replay Y 数 | 源数据数 |
|------|-----------|-------------|---------|
| snapshot_minute_20260525_1128.csv | 37 | 10,811 | 10,811 |
| snapshot_minute_20260525_1129.csv | 76 | 12,201 | 12,201 |
| snapshot_minute_20260525_1524.csv | 168 | 42,868 | — |

### 根因
`_data_loop`（engine.py L361）仍使用旧模式：`for line in self._snapshot_tailer.read_lines()` + 固定 `time.sleep(interval)`。

- 每次只读一个 chunk（~65KB），然后更新 checkpoint/watermark
- data_simulator 100x 速度下，lunch break（1130）和 post-market（1530）前的数据间隔导致 watermark 快速跳跃
- 1128/1129 的数据尚未完全读入，watermark 已推进到 1130+
- clock-thread 的 flusher 看到 watermark 超前，判定 1128/1129 过期并 flush → 只有几十条记录

### 为什么生产环境不受影响
生产环境 speed=1，watermark 每分钟只推进 1 分钟，数据有充足时间完全到达。但 drain loop 仍是更健壮的方案。

### 对比：Order vs Snapshot 处理模式
| | `_order_loop`（Phase 14 已修复） | `_data_loop`（待修复） |
|---|---|---|
| 读取 | `while True: lines = list(read_lines())` drain loop | `for line in read_lines()` 单次 chunk |
| Sleep | 有数据 1ms，无数据配置间隔 | 固定配置间隔 |
| 保护 | 每 100 次 drain 检查 | 无 |

### 修复方案（Phase 15a 已实施）
给 `_data_loop` 加与 `_order_loop` 相同模式的 drain loop，约 10 行改动。修复后 1128/1129 记录数恢复正确。1524 的差异由独立问题（Flusher Double-Flush Bug）导致，Phase 15b 修复。

### 影响范围
- 仅修改 `engine.py` `_data_loop` 方法
- 不影响 flusher、writer、replay、其他线程
- 修复后 329 个 snapshot 文件的 update_flag=Y 记录数与 replay 完全一致

## Tickfile 生成设计关键发现（2026-06-02）

### 设计文档
- `docs/superpowers/specs/2026-06-01-tickfile-generation-design.md`（Round 14, 15轮审阅通过）

### 需求来源
需求定义 65 列 tickfile 输出格式，数据来源为 snapshot + order 分钟数据。设计文档中通过 Q&A 澄清了所有模糊点（decimal 转换、carry-forward 规则、排序、seqno、时区等）。

### 核心架构决策
1. **全天一个文件，append 模式**：与 snapshot/order 的 per-minute 文件不同，tickfile 按日聚合。首次 atomic_write，后续 append + fsync
2. **SharedState bridge**：order 数据通过 `raw_order_buffers` 和 `latest_order_by_symbol` 从 order thread 传递到 clock/main thread 供 tickfile 使用
3. **Late record 不重新生成 tickfile**：tickfile 是按行合成的汇总文件，迟到记录 append 到 snapshot/order 文件时不触发 tickfile 重写
4. **`latest_order_by_symbol` 跨日过滤清空**：与 `latest_snapshot`（跨日保留）不同，order cache 跨日按日期过滤，因为旧 bid/ask 对新交易日无意义
5. **`process_order` 标记 TEST ONLY**：live 模式 `_order_loop` 直接写入 SharedState，不调用 `process_order`（否则 seqno 重复）
6. **`LATE_CACHE_MARKER = object()`**：替代字符串 sentinel，用 `is` 比较，避免 minute_key 碰撞
7. **多分钟 batch 共享 order cache 快照**：`_flush_minutes_internal` 顶部一次性拷贝 `latest_order_by_symbol`，batch 内共享。已知行为，carry-forward 数据仍为有效记录
8. **Replay atomic pop**：`raw_order_buffers.pop(minute_key)` 和 `latest_order_by_symbol` 拷贝在同一 lock scope 内完成，防止竞态窗口
9. **`update_flag` 等价性 INVARIANT**：`raw_records` 仅含实时数据（等价 update_flag=Y），`snapshot_copy` 代表 carry-forward（等价 update_flag=N）。如果未来修改 raw_records 填充逻辑包含 carry-forward，此等价性将破坏

### 字段映射关键决策
- **0→NA 规则**：仅适用于价格字段（LastPrice、OpenPrice、ClosePrice 等），volume 字段 0 是合法值输出 `0`
- **Turnover**：`totalamount / 10^decimal`（累计值，与 kline amount 增量语义不同），`totalamount=0` 输出 `0.0`
- **IntraDailyReturn -0.0 归一化**：`result = result + 0.0`（利用 IEEE 754 下 `-0.0 + 0.0 == 0.0`）
- **UpdateTime 秒数丢弃**：始终 `:00`（分钟粒度），短 rcvtime（<12 位）输出 NA
- **2-10 档 NA**：order 仅提供 1 档，2-10 档全部输出 NA（包括 volume 字段，与 1 档 volume=0 输出 `0` 不同）

### 审阅收敛轨迹
15 轮审阅中，前 5 轮修复了大量基础问题（字段映射、decimal 处理），6-10 轮聚焦工程安全（crash recovery、线程安全），11-13 轮完善边界条件（late record、orphaned data、跨日），14 轮微调文档清晰性，15 轮最终验证通过。典型收敛模式：Critical → Major → Minor → 文档清晰性 → 无问题。

## Tickfile 生成实施结果（2026-06-02）

### 改动总览
7 个文件修改 + 2 个新文件（`tickfile.py`、`test_tickfile.py`），共 71 个新测试，278 个测试全部通过。

### 关键实施发现

1. **Replay SharedState 重构**（最复杂改动）：
   - `state` 局部变量提升为 `self._state` 实例变量，使 `_stream_snapshots` 和 `_stream_orders` 共享 SharedState
   - 所有 `state.` 引用替换为 `self._state.`，所有 `_flush_snapshot_minute(state, ...)` 签名中移除 `state` 参数
   - `_flush_snapshot_minute` 内 atomic pop：`raw_order_buffers.pop(minute_key)` 和 `latest_order_copy` 拷贝在同一 lock scope，防止 copy-then-pop 竞态

2. **Live order bridge**（engine.py `_order_loop`）：
   - `LATE_CACHE_MARKER = object()` sentinel 替代字符串 minute_key，`is` 比较避免碰撞
   - `pending_shared_orders: list = []` 在 inner while loop 前创建，每轮 batch-write 到 SharedState 后 clear
   - late record 路径也写入 cache（`LATE_CACHE_MARKER`），确保 late order 数据可用于 tickfile carry-forward

3. **Flusher 集成**（flusher.py）：
   - `_flush_minutes_internal`：两处 lock scope 均拷贝 `latest_order_by_symbol`
   - `_write_minute_files`：新增 `latest_order_copy` 参数（default None 向后兼容），tickfile block gated by `enable_tickfile`
   - `_step1_cross_day_check`：order cache 按日期过滤（`str(rec.time)[:8] == current_date`），`_tickfile_seqno = 0`

4. **Tickfile I/O**（writer.py）：
   - `write_tickfile_rows`：首次 atomic_write（header + data），后续 append + fsync。处理 header corrupted、empty file overwrite、truncated last line 修复
   - `recover_tickfile_seqno`：O(N) 时间 O(1) 空间扫描，200MB 保护上限，跳过非 65 列行和非整数 seqno

5. **字段映射实现细节**（tickfile.py）：
   - `_safe_divide(value, decimal)`：`10**decimal if decimal > 0 else 1`，防止 decimal≤0 导致除零
   - `_price_or_na(value, decimal)`：0→NA 仅限价格字段，volume 字段保留 0
   - `build_tickfile_row(snapshot, order, seqno, code_table_getter)`：snapshot 和 order 均可为 None，`select_tickfile_records` 保证 snapshot 非 None 才输出
   - IntraDailyReturn：`result + 0.0` 归一化 -0.0；preclose=0→NA
   - `select_tickfile_records`：raw_records 优先（最早记录），fallback snapshot_copy carry-forward；order 同理 + `latest_order_by_symbol` carry-forward

6. **测试修复**（实施过程中发现）：
   - `test_volume_none_snapshot_is_na`：`build_tickfile_row(None, None, ...)` 返回单个 `"NA"` 字符串而非 65 列行，因为此场景被 `select_tickfile_records` 过滤。修正测试期望值
   - `test_local_time_format`：17 位时间格式解析中 `s[14:17]` 是毫秒部分（000），`s[12:14]` 是秒（13），修正期望值

## Tickfile Live 测试 Bug（2026-06-03）

### Bug 1: `flusher.py:283` — `latest_order_copy` 未传递

**现象：** Tickfile bid/ask 覆盖率 0%（1,482,098 行全为 NA）。

**根因：** `_flush_minutes_internal` 调用 `_write_minute_files(minute_key, minute_snapshot, data, raw, orders)` 时缺少第 6 个参数 `latest_order_copy`。函数签名 `latest_order_copy=None` → `select_tickfile_records(raw_records, snapshot_copy, order_records, {})` → 空 carry-forward dict → 所有 order 字段 NA。

**修复：** 传入 `latest_order_copy`。修复后 bid/ask 覆盖率 95.8%。

**Why:** L283 是 normal flush 路径，L127 是 cross-day flush 路径。Cross-day 路径已正确传参，normal 路径遗漏。

### Bug 2: `engine.py` — `raw_order_buffers` 内存泄漏

**现象：** Order 线程到 1257 分钟时占用大量内存，且持续增长。

**根因：** `raw_order_buffers[minute_key]` 设计用于 tickfile 生成（flusher pop 后写入 tickfile）。当 snapshot 全天 flush 完毕（1530）后，flusher 不再 pop `raw_order_buffers`。但 order 线程继续运行，每条记录仍写入 `raw_order_buffers[0925]`、`raw_order_buffers[0926]` 等。这些数据永远不会被消费 → 内存泄漏。

**修复：** `_order_loop` 写入 SharedState 时，检查 `mk not in self._state.flushed_snapshot_minutes`，跳过已 flush 的分钟。`latest_order_by_symbol` 仍更新（每 symbol 仅一条记录，内存可控）。

**INVARIANT:** `raw_order_buffers` 仅在 snapshot 分钟未 flush 时写入。一旦 snapshot flush 完成，该分钟不再写入。

### Bug 3: `code_table.py` — 占位行 decimal 覆盖有效值

**现象：** 2026 个 symbol 报 "Snapshot decimal (2) != code decimal (0)" warning。Limit price 值放大 100 倍。

**根因：** code.csv 同一 symbol 有多行数据。第一行含有效值（decimal=2, limitup=518000, limitdown=378000），后续行为价格清零的占位行（decimal=0, limitup=0, limitdown=0）。CodeTable 直接 `self._table[parsed.symbol] = parsed`（后覆盖前），导致有效值被占位行覆盖。

**影响：** `build_tickfile_row` 中 `_safe_divide(code_entry.limitup, code_entry.decimal)` 使用 decimal=0 → 不做除法 → limit price 输出原始值（518000 而非 5180.00）。同时 snapshot 的 decimal=2 与 code 的 decimal=0 不一致，影响其他依赖 code decimal 的计算。

**修复：** 新增 `_merge_symbol` 方法，保留已有非零 `limitup`、`limitdown`、`decimal` 值。占位行的 0 不覆盖有效值。

**设计决策：** 选择"保留非零值"而非"取第一行"或"取最新非零行"，因为 code.csv 中有效值和占位行交替出现，保留非零值语义清晰且向后兼容。

## Tickfile 同步生成问题（2026-06-03，Phase 17 设计完成）

### 问题描述
Live 模式下 tickfile 生成绑定在 snapshot flush 上。由于 order 数据量是 snapshot 的 ~8 倍（5.6GB vs 698MB），order 线程系统性滞后。Tickfile 写入时 `raw_order_buffers[该分钟]` 为空，只能用 `latest_order_by_symbol` carry-forward 数据。

虽然 carry-forward 覆盖率 95.8%，但这些是 **前几分钟的旧 bid/ask**，不是当前分钟的真实 order 数据。对于盘口变化剧烈的分钟（如开盘 0900），carry-forward 的 bid/ask 可能已严重偏离实际值。

### Live 测试数据佐证
```
Seqno=1   (0800): bid=47.1%  — 盘前，部分 symbol 无 order
Seqno=2   (0830): bid=84.5%  — carry-forward 开始积累
Seqno=4+  (0900+): bid=95.8% — 稳定，但全部是 carry-forward
```

### 根因分析
```
Timeline (speed=5, order-speed=100):

T=0s   Simulator 开始写入
T=60s  Snapshot 0800→ 写完 snapshot 文件
       Flusher 检测 0800 过期 → flush snapshot → 写 tickfile（此时 order 在 0800，raw_order_buffers 有数据 ✓）
T=120s Snapshot 0920→ 但 order 只到 0859
       Flusher flush 0900→ 写 tickfile（raw_order_buffers[0900] 为空 → carry-forward ✗）
```

问题本质：**tickfile 是 snapshot flush 的同步下游，但 order 数据到达天然滞后**。

### Phase 17 解决方案：双线程 join 同步

**核心概念**：Tickfile 是 snapshot 分钟文件和 order 分钟文件的下游产物。两者都完成才生成。

**实现要点**：
1. SharedState 新增 `_tickfile_pending`（暂存等 order 的 snapshot 数据）
2. SharedState 新增 `order_current_minute`（order 线程进度，供 flusher 判断）
3. SharedState 新增 `_tickfile_seqno`（从 flusher 迁移，两个线程都可见）
4. Flusher 解耦 tickfile：flush snapshot 时存 `_tickfile_pending`，不立即写 tickfile
5. Order 线程触发：flush order 后检查 `_tickfile_pending`，两者完成则生成 tickfile
6. Flusher 兜底：snapshot flush 后检查 `order_current_minute > X`，若 order 已过则直接生成
7. EOF 兜底：order 永远没到的分钟用 carry-forward
8. Order 写条件修改：允许对 `_tickfile_pending` 中的分钟继续写 `raw_order_buffers`

### 设计审阅关键发现（8 轮 24+ agents）

#### R4-R5 Critical 修复（最重要的 3 个时序问题）
1. **batch-write timing（R4 C1/C2）**：`_flush_order_minute` 在 `for line in lines` 内部调用，batch write 在循环外。如果在此处更新 `order_current_minute`，clock thread 可能在 batch write 前看到并触发不完整 tickfile。**修复**：`order_current_minute` 仅在 `_drain_tickfile_triggers`（batch write 后）更新。
2. **Cross-day raw_order_buffers pop（R4 C3）**：flusher.py 第一个 lock scope 无条件 pop `raw_order_buffers`，导致 force-generate 拿不到数据。**修复**：`enable_tickfile=True` 时条件跳过 pop。
3. **Reroute 不清除 _tickfile_pending（R5 C1）**：`_reroute_buffer_to_late_queue` 只 pop `raw_order_buffers`，留下悬挂的 `_tickfile_pending` entry。**修复**：同一 lock scope 内两个 pop。

#### R6-R8 Major 修复（文档 + 实现指导）
4. **config 变量未定义（R6）**：`recover_tickfile_seqno_lazy` 引用 flusher 中不存在的 `config`。修复为 `jst_now_yyyymmdd()` 直接调用。
5. **第三 drain point（R7）**：engine.py line 565 处还有 `_flush_expired_order_minutes` 调用，spec 未覆盖。修复：新增 Drain point 3。
6. **Re-insert replace vs extend（R8）**：`.extend()` 在极端情况下可能追加到 order thread 重创建的 list。**修复**：改为 `self._state.raw_order_buffers[minute_key] = order_records`（replace）。

#### 已知的 pre-existing 问题（不阻塞 Phase 17）
- `write_tickfile_rows` readlines() I/O 放大（每次 append 读全 daily file）。建议 Phase 18 优化为 `seek(-N, 2)` 尾部检查。
- `write_tickfile_rows` silent return 路径（corrupt header + all rows failed）。Phase 17 同步修复为 raise IOError。
- `_write_locks` dict 无界增长（1 entry/day，pre-existing）。

### 设计规格
- **Spec**: `docs/superpowers/specs/2026-06-03-tickfile-sync-design.md`（Round 8 Final, Status: Review Passed）
- **25 INVARIANTS**, **31 tests**, **3 drain points**, **5 Cases (A-E)**
- **Implementation Plan**: 待 `writing-plans` skill 创建

详见 `task_plan.md` Phase 17 实施步骤。

### 关键设计决策

#### 1. 为什么不用 `_flushed_order_minutes` 共享？
`_flushed_order_minutes` 是 engine local 变量，仅 order 线程访问（late 检测）。若提升到 SharedState，order 线程每次 late 检查都需获取 SharedState lock，影响性能。

改用 `order_current_minute`（order 线程在 SharedState lock 下更新），flusher 检查 `order_current_minute > minute_key` 即可知道 order 是否已过该分钟。

#### 2. 为什么 `raw_order_buffers` 条件需要改？
Phase 16 Bug 2 修复引入了 `mk not in flushed_snapshot_minutes` 检查，防止内存泄漏。但这也阻止了 order 线程在 snapshot flush 后继续写 `raw_order_buffers`。

新条件：`mk not in flushed_snapshot_minutes or mk in _tickfile_pending`
- 非 pending 分钟：跳过（同 Phase 16 Bug 2 修复）
- pending 分钟：继续写入（为 tickfile 收集真实 order 数据）
- tickfile 生成后清理 `_tickfile_pending` → 条件自然失效

#### 3. 为什么 tickfile 生成可以安全地由任一线程触发？
- `_tickfile_pending` 和 `raw_order_buffers` 的 pop 操作在 SharedState lock 内原子完成
- `select_tickfile_records` 和 `write_tickfile_rows` 在 lock 外执行（CPU 密集但无共享状态）
- `write_tickfile_rows` 内部有文件级写锁（`_get_write_lock`）防止并发写同一文件
- `_tickfile_seqno` 递增在 lock 内完成

#### 4. Order 线程 flush 后 `raw_order_buffers` 一定完整吗？
是的。`pending_shared_orders` 的 batch-write 在 `_flush_expired_order_minutes` 之前完成：
```python
# engine.py _order_loop inner while:
for line in lines:
    pending_shared_orders.append((mk, rec))
# batch write to SharedState (line 533)
if pending_shared_orders:
    with self._state.lock: ... raw_order_buffers ...
    pending_shared_orders.clear()
# THEN flush expired (line 550)
if drain_count >= 100:
    _flush_expired_order_minutes(...)
```
时序保证：order file 写入时，`raw_order_buffers[X]` 已包含全部 X 记录。

#### 5. Replay 不需要改动
Replay 的 `_stream_orders` 和 `_stream_snapshots` 在同一进程内交叉执行。`DELAYED_FLUSH_ROUNDS=3` 确保 order 数据在 `_flush_snapshot_minute` 调用前已写入 `raw_order_buffers`。Tickfile 同步问题仅存在于 live 多线程模式。

### Simulator 增强（Phase 17 已完成）
- 新增 `--order-speed` / `--snapshot-speed` CLI 参数，支持 per-file-type speed
- 用法：`--speed 5 --order-speed 100`（snapshot 5x, order 100x）
- 解决 order 数据量 8 倍于 snapshot 导致 live 测试节奏不匹配的问题

## E2E Fix 实施关键发现（2026-06-09，Phase 19）

### P0 根因：reroute race condition

`_reroute_buffer_to_late_queue` 中 `_tickfile_pending.pop(k)` 是 tickfile 缺失的主因。Race window:
1. snapshot 分钟 N 被 flush → `_tickfile_pending["N"]` 被设置
2. race window 检测到分钟 N 又有新数据 → `_reroute_buffer_to_late_queue(["N"])`
3. `_tickfile_pending.pop("N")` → tickfile 数据被清除
4. tickfile writer dequeue N → `pending is None` → **静默跳过**（Fix-B 之前无日志）

Fix-A 修复：移除 pop。INV-TF1 确立：只有 `_try_generate_tickfile` 和 `_step1_cross_day_check` 可移除 `_tickfile_pending`。

### Fix-F 去重 guard 的关键时序

`.add(minute_key)` 必须在 `write_tickfile_rows` 成功之后（Implementation Note #1）。如果放在 write 之前：
- IO 失败 → key 已在 set 中 → re-insert 被跳过 → 数据永久丢失
- 部分写入 → key 已在 set 中 → retry 被拒绝 → 不完整文件

正确顺序：guard check（write 前）→ pop pending + seqno（同一 lock）→ select + write（lock 外）→ `.add()`（write 后）

### Fix-C Layer 3 为什么用 `snaps & orders` 交集

盘前/盘后分钟（0800, 0830）仅有 snapshot 无 order，tickfile CSV 中无对应行。如果无条件比较 `flushed_snapshot_minutes - generated`，会产生 false-positive MISSING。Intersection 确保只比较有 order+snapshot 的分钟。

### Fix-D 为什么默认 1M

- 生产环境 late records 极少（< 1000/分钟），1M cap 远超需求
- 100x 测试 busiest minute（0900）有 ~750K records，1M 留 33% headroom
- 每条 record ~300-400 bytes Python 内存，1M × 400B ≈ 400MB/分钟峰值

### 测试修复发现

1. `_engine_ref` 在测试中为 None — Fix-B 新增的 skip counter 需要 None guard
2. `write_tickfile_rows` 是在方法内部 `from minute_bar.writer import`，不能 patch `minute_bar.flusher.write_tickfile_rows`（需 patch `minute_bar.writer.write_tickfile_rows` 或 `minute_bar.tickfile.select_tickfile_records`）
3. `MagicMock(spec=AppConfig)` 阻止 attribute assignment — 去掉 `spec=` 使用 `MagicMock()` 即可

## Tickfile BG Writer E2E Live Test 发现（2026-06-08）

### Bug 1: tickfile overflow 强制生成无 order 数据

**现象：** `tickfile pending overflow: 58 minutes pending (max=10), forcing 48 oldest` — tickfile 生成到分钟 1428，但全部是 `0 orders` 的 carry-forward 行。Order watermark 卡在 0859。

**根因：** `flusher.py tick()` overflow（line 108-124）直接按 pending_keys 排序取 oldest，未检查 `order_current_minute`。当 snapshot flush 远超 order 处理进度时（speed=100 下 order 0900 有 685,145 条记录需 ~3.7 分钟处理），pending 积累到 200+ 分钟，overflow 全部强制生成 → 产生无 order 数据的 tickfile 行。

**修复：** overflow 增加 `eligible_keys` 过滤，只 force-enqueue `order_watermark > minute_key` 的分钟。未达到 order watermark 的 pending 条目保留在内存中等待。同时日志输出 `total/eligible` 帮助运维判断状态。

**设计决策：**
- 为什么不直接限制 pending 总数？因为 pending 中有"等 order 的有效数据"和"order 已过的可生成数据"两种，限制总数会丢弃有效数据
- 为什么不取消 overflow？极端情况下 order 长时间卡死时仍需溢出保护，否则 pending 无限增长导致 OOM

**文件：** `src/minute_bar/flusher.py:108-134`
**测试：** 364 passed（含 overflow 相关已有测试）

### Bug 2: UpdateTime 未按分钟递增（carry-forward 行语义错误）

**现象：** Carry-forward 行（如 symbol 6633）在 seqno=12-19 的 UpdateTime 全部为 `20260528 08:13`，不随生成的分钟递增。

**根因：** `build_tickfile_row` 用 `primary_rcvtime`（来自 snapshot 的 rcvtime 字段）生成 UpdateTime。Carry-forward 行的 snapshot 数据来自几分钟前的原始记录，rcvtime 不变。但 UpdateTime 语义应为「本条 tickfile 记录生成的本地时间」，应随 minute_key 递增。

**语义澄清（用户确认）：**
- **UpdateTime** = 本条 tickfile 记录生成的本地时间（北京时间 UTC+8），对应 minute_key 的分钟
- **LocalTime** = 交易所当地时间（日本时间 UTC+9），来自原始数据的时间戳
- Carry-forward 行：LocalTime 保持不变（原始交易时间），UpdateTime 递增（记录生成时间）

**修复：** `build_tickfile_row` 新增 `minute_key: str = ""` 参数，UpdateTime 优先从 `minute_key` 派生（`YYYYMMDD HH:MM:00`），fallback 到 `rcvtime`（向后兼容）。`write_tickfile_rows` 调用时传入 `minute_key`。

**文件：** `src/minute_bar/tickfile.py:28-137`, `src/minute_bar/writer.py:312`
**验证：** carry-forward 行 UpdateTime=08:00/08:30 随 seqno 递增，LocalTime=08:00 保持原始时间

### Order 处理延迟分析（非 Bug）

**现象：** Order watermark 从 0859 停滞 3 分 44 秒后跳到 0900（685,145 records），之后每 ~30 秒推进 1 分钟。

**根因：** 日股开盘（09:00 JST）的 order 数据量暴增：
- 分钟 0800-0859：~30K records/min（盘前）
- 分钟 0900：685,145 records（开盘 rush，~23x 正常量）
- 分钟 0901-0903：174K-409K records
- 之后回落到 ~30K/min

Order 线程的 drain loop 以 512KB chunk 连续读取，但 0900 分钟的 52MB 数据需要多轮处理。这不是 tickfile IO 阻塞（Phase 18 已解决），而是纯 CPU+IO 吞吐量瓶颈。

**数据：** `order.csv.20260528` 总量 4.6GB，分钟 0900 数据占 ~52MB（1.1% 时间产生 ~15% 数据量）。

## Rust Order Acceleration 实施发现（2026-06-11，Phase 20）

### GIL 瓶颈与 Rust 解决方案

**问题**: Order thread 0900 peak minute（747K records）耗时 147s，远超 60s SLA。Root cause 是 Python GIL — parse+build 占 83.6% CPU time 且全程持有 GIL，snapshot thread 完全无法运行。

**解决方案**: Rust/PyO3 extension `order_accel` 提供 `parse_order_batch()` 函数，在 `py.allow_threads()` 中执行全部 parsing，GIL 完全释放。Snapshot thread 在 Rust parse 期间自由运行。

**架构**: 仅 parse 移到 Rust，其余逻辑（buffering, locking, minute detection, file writing）保留在 Python。

### PyO3 返回类型性能对比（关键发现）

**这是实施中最重要的发现**：PyO3 return type 的性能在 debug vs release 之间差距巨大。

| 配置 | Tuple Return | Flat Binary | Rust Parse Only |
|------|-------------|-------------|-----------------|
| **Debug build** | 6.723s | 6.949s | 6.148s |
| **Release build** | **1.222s** | 1.785s | 0.980s |

**结论**:
1. **Release build 是硬性要求** — debug build 不可用于任何性能敏感场景（5.5x 性能差距）
2. **Tuple return 在 release 下是最优选择** — PyO3 在 release 模式下对 tuple return 有优化，快于 flat binary（1.22s vs 1.79s）
3. **Flat binary 保留为备用路径** — 如果 PyO3 未来版本性能退化，flat binary path 已就绪

**为什么 spec 预估与实际不符**: Spec 预估 PyO3 tuple return ~468ms，实际 release 1.22s。原因是 spec 未考虑 747K × 8 = 5,976,000 Python object allocations 的 GC 开销（~250ms）和 refcount 操作。

### `_process_parsed_record` 重构（最高风险步骤）

从 engine.py lines 694-772 提取为独立方法，是整个实施中最高风险的改动。

**Record-scoped（放入 `_process_parsed_record`）**:
- Cross-day flush + reset（lines 708-726）
- Late-order detection + append（lines 732-751）
- Record-driven flush（lines 754-757）
- Buffer append + pending_shared_orders.append（lines 760-767）
- Watermark update（lines 770-772）

**Batch-scoped（必须留在 batch loop caller）**:
- `pending_shared_orders` state-lock processing（lines 774-791）：~117 lock acquires vs 747K if inside per-record function
- `drain_count` increment（line 692）：once per batch
- Periodic flush check（lines 793-808）：once per batch

**关键陷阱**: 如果错误地将 batch-scoped 逻辑放入 `_process_parsed_record`，会导致 747K lock acquisitions（而非 ~117），严重影响性能。Spec Section 4.4 明确标注了这个风险。

### `LATE_CACHE_MARKER` → `"__LATE__"` String Sentinel

原始代码使用 `LATE_CACHE_MARKER = object()` 作为 sentinel（identity comparison）。但 `_process_parsed_record` 是 instance method，无法引用 caller scope 的 `object()` 实例。

**解决方案**: 改用字符串 `"__LATE__"` 作为 sentinel。在 batch-scoped 的 state-lock 处理中，用 `mk == "__LATE__"` 替代 `mk is LATE_CACHE_MARKER`。

**权衡**: 字符串比较比 identity comparison 稍慢，但 late order 数量极少（production <1000/min），性能影响可忽略。选择字符串而非重新设计为 class-level marker 是因为 `_process_parsed_record` 的参数列表已经很长（14 parameters）。

### `time_to_minute_key` 整数除法优化

**验证**: `str(20260528090000123 // 100_000) == str(20260528090000123)[:12]` → True for all 17-digit timestamps。

Rust path 使用 `str(record.time // 100_000)`，消除一个中间 17-char string allocation（~75-150ms GIL-held savings for 747K records）。

**注意**: Python fallback 路径仍使用 `time_to_minute_key(record.time)`（`s[:12]`），保持 exact original behavior。

### Rust Extension 17-digit Timestamp Guard

Rust 拒绝非 17-digit 时间戳（`10^16 <= time < 10^17`）。Python 接受任意位数。

**这是 by design**: 对于非标准时间戳，`str[:8]` 和 `//10^9` 会产生不同日期。Rust 宁可拒绝也不产生不一致结果。被拒绝的记录计入 `skipped_count`，engine.py 日志 WARNING when >50% lines skipped。

### Build System 修复

`pyproject.toml` 的 `build-backend = "setuptools.backends._legacy:_Backend"` 在 setuptools 82+ 不存在（empirically verified）。改为 `setuptools.build_meta:__legacy__` 后 `pip install -e .` 恢复正常。

**Windows 注意**: `cargo.toml` 和 `Cargo.toml` 在 Windows 上是同一文件（case-insensitive）。`rm cargo.toml` 会删除 `Cargo.toml`。

### Memory Budget (747K records peak)

| Component | Estimated Size |
|-----------|---------------|
| NamedTuples (747K × 112B) | ~80MB |
| minute_key strings (747K × 28B) | ~21MB |
| Buffer dict (747K × ~150B) | ~107MB |
| GC promotions | ~20-100MB |
| **Total peak** | **~230-410MB** |

Must validate with `tracemalloc` in Phase 4.

### Rollback Plan

1. Set `enable_order_accel = false` in INI config → Restart → Python fallback
2. Delete `_order_accel.pyd` from `src/minute_bar/` → Import fallback → System starts normally
3. Both paths tested and verified producing byte-identical output

---

## Rust 去 GIL 解耦合阶段最终发现（2026-06-15，Phase 21 + late-order 修复）

> 阶段总结：order / snapshot / tickfile 三条热路径全部 Rust 化，Phase 4 的 order vs snapshot
> GIL 35x 竞争根因彻底解决。本节记录阶段末尾的关键发现（late-order 瓶颈、Q1 stale 残留、Q2 吞吐）。

### Finding 1: late-order 写盘是 Phase 21 Rust 化的遗漏盲点

Phase 21 把 `process_order_batch`（parse + group + buffer build）移入 Rust，但**写盘仍在 Python**。
Rust 的 `late_order_buf` 解码后，Python 逐条 `append_order_records([rec])` —— 每条一次 `open(path, "a")`。

**为什么 0900 峰值触发**: 0900 附近 minute 快速 flush（0850-0900 已 flush），大量 record 落入
`late_order_buf`（单批数千条），逐条 `open()` 把 order 线程拖死在 I/O（非 GIL）。

**修复**: 抽取 `_write_late_orders_batch`，按 minute_key 分组，每分钟一次写盘。
`open()` 次数: `len(decoded_late)`(数千) → `len(late_by_minute)`(~1-3)。

**与 Phase 4 GIL 问题的区别**:
- Phase 4: GIL 竞争（snapshot 抢 89% GIL），Rust 化释放 GIL 解决
- 本次: 文件 I/O 系统调用（order 自己卡 open），批量写减少 open 次数解决
- snapshot 表现是判据: 本次 snapshot 正常推进（证明非 GIL 竞争）

### Finding 2: tickfile 是 append-only，stale 行永久残留（Q1）

`write_tickfile_rows` (writer.py:346): 文件存在 → **追加**，不存在 → 原子创建。一个交易日一个文件，
append-only。

**重启恢复链路全验证**:
- `_generated_tickfile_minutes`(去重集) + `_tickfile_pending` 均**纯内存，不持久化**
- `recover_tickfile_seqno` 只恢复 seqno，**不**恢复已生成分钟
- `flushed_snapshot_minutes` 从磁盘恢复**全部** snapshot 分钟（含 gap 分钟）→ snapshot 不重 flush → 不重填 `_tickfile_pending`
- 无任何 truncate tickfile 逻辑

**结论**: shutdown 产生的 stale 行（order 未到分钟，用冻结 carry-forward）重启后:
`_try_generate_tickfile(gap_mk)` → `pending = _tickfile_pending.pop(gap_mk)` = None → "skipped, no pending"
→ **不重新生成，stale 行永久残留**。唯一修复: 删 tickfile + ReplayEngine 重跑。

### Finding 3: 100Kx benchmark 是人为压力测试，非实时能力判据（Q2）

之前看到"order 慢、stale 198 分钟"易误判为实时能力不足。实测:
- 引擎吞吐天花板（100Kx 饱和）: **87,000 lines/sec**
- 实时峰值需求（源 0903 分钟 759,890 lines）: 12,667 lines/sec
- **余量 6.9x（峰值）/ 25x（均值）**

100Kx = 实时 100,000 倍灌入，order 必然跟不上、必然积压、停止必然大 gap。
**真实 1x 生产**: order 吞吐远超需求，order/snapshot 同步，停止 gap ≈ 0-1 分钟。
benchmark 失败（RSS 2.2GB > 400MB）是积压内存压力，非实时吞吐不足。

### Finding 4: tickfile 生成门控（live gate 正确）

`flusher.py:448` tickfile enqueue 门控 `order_current_minute > minute_key` **正确**，
日志 `Tickfile pending: N total, 0 eligible` 证明运行期间 0 抢跑。
stale 行只来自两条**无检查的强制生成路径**:
- shutdown: `flush_all_remaining` (flusher.py:487-499)
- cross-day: `_step1_cross_day_check` (flusher.py:225-234)

---

## Minute-Key Round-Up 发现（2026-06-16，左开右闭语义变更）

### Finding 5: spec 的"单点覆盖"假设错误——5 个独立推导点

调查（6-agent）否定了设计 spec 的核心假设"改 `time_to_minute_key` 就覆盖一切"。
实际有 **5 个不经过该函数的内联推导点**必须同步改，否则 order（round-up）和 snapshot（round-up）的边界记录会分裂到两个分钟文件：
- Rust order 热路径 `lib.rs:861`（生产 order 全量，`preprocess_line` 内联 `&time_str[..12]`）
- Python `engine.py` 4 处（`_write_late_orders_batch` 859、Phase 21 re-bucket 962、Rust-accel 1031、Python fallback 1050）

**修复**：全部改走 `time_to_minute_key`。date 切片（`[:8]`）独立、保持 round-down（驱动跨日检测）。

### Finding 6: 收盘时间戳正好 15:30:00.000 → 左开右闭

源数据收盘 order/snapshot 时间戳是 `20260528153000000`（**正好 15:30:00.000**，SSMMM=0）。
初版严格 floor+1 把它推到 **1531**（spurious 分钟）。改为**左开右闭** `((M-1):00.000, M:00.000]`：
精确边界时间戳归自身分钟，收盘正确归 **1530**。

- 非边界（SSMMM>0，如 09:00:01.000）仍 round-up（→0901）
- 精确边界（SSMMM=0，如 15:30:00.000）归自身（→1530）
- 全天仅 ~35K 条精确边界记录（基本都在收盘），盘中上亿 tick 不变
- `end_time`=M、`start_time`=M−1min 保持不变（已匹配 `(M−1,M]` 区间）；`is_expired` 的 wall-clock flush 时机因"label +1 与 end_time −1min 抵消"而与 round-down 一致

### Finding 7: Python↔Rust 逐字节一致

Python `time_to_minute_key`（`datetime+timedelta`）与 Rust（手写进位 + `increment_yyyymmdd`，无 chrono）
对 8 个边界用例（含闰年 2/29、世纪非闰 2100、跨小时、跨日）输出完全一致。

### Finding 8: is_trading_minute 是死代码（doc 勘误）

spec 原称"is_trading_minute 过滤 1531"是**错的**——`is_trading_minute`（clock.py）在 live 管道中
**从无调用**。1531（现已是 1530）作为正常最后分钟 flush。非 bug，仅文档不准。
