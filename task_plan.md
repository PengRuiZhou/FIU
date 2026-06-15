# FIU 日股分钟级行情数据生成器 — 任务计划

## 目标
读取 FIU 接收服务实时写入的 snapshot.csv / code.csv / order.csv 文件，每分钟生成全市场行情快照、OHLCV K 线文件和 order 分钟文件。

## 设计文档
- `docs/superpowers/specs/2026-05-20-jp-market-minute-bar-design.md`（基础架构）
- `docs/superpowers/specs/2026-05-21-parallel-snapshot-order-design.md`（并行+流式优化）
- `docs/superpowers/specs/2026-05-22-late-record-handling-design.md`（迟到记录统一处理）
- `docs/superpowers/plans/2026-05-22-late-record-handling.md`（实现计划）
- `docs/superpowers/specs/2026-05-22-data-simulator-design.md`（数据模拟器）
- `docs/superpowers/specs/2026-05-25-data-driven-watermark-design.md`（数据驱动 Watermark Flush）
- `docs/superpowers/plans/2026-05-25-data-driven-watermark.md`（Watermark 实现计划）
- `docs/superpowers/specs/2026-05-27-order-write-performance-design.md`（Order 线程性能优化）
- `docs/superpowers/specs/2026-06-01-tickfile-generation-design.md`（Tickfile 生成设计，Round 14，15 轮审阅通过）
- `docs/superpowers/specs/2026-06-03-tickfile-sync-design.md`（Tickfile 同步生成设计，Phase 17）
- `docs/superpowers/specs/2026-06-04-tickfile-bg-writer-design.md`（Tickfile Background Writer + Seek Optimization，Phase 18，v11）

---

## Phase 1: 项目骨架与数据结构 `[complete]`
- [x] 创建项目目录结构 (`src/`, `tests/`, `config/`)
- [x] 实现 `SnapshotRecord` (frozen dataclass, float 类型存原始值不做 decimal 除法)
- [x] 实现 `OHLCVAggregate` dataclass (内部除以 10**decimal 做 K 线计算)
- [x] 实现 `FileState` dataclass
- [x] 实现配置文件解析 (`config.ini`)
- [x] 单元测试：数据结构构造与校验

## Phase 2: 文件读取管线 `[complete]`
- [x] 实现 `FileTailer` — 按 bytes offset 轮询读取文件增量
- [x] 实现 `BinaryLineAssembler` — 按 `\n` 切完整行，处理截断拼接
- [x] 实现 `CsvRowParser` — 按列位置索引，自动跳过 header 行（snapshot + code）
- [x] 单元测试：不完整行拼接、列数校验、默认值填充

## Phase 3: 数据校验与聚合 `[complete]`
- [x] 实现 `Validator` — 字段校验、错误分级 (WARN/ERROR/FATAL)
- [x] 实现 `MinuteAggregator` — 更新 latest_snapshot、ohlcv_buffer、raw_snapshot_buffers
- [x] 实现 OHLCV 计算规则（OHLCVAggregate 内部除 decimal，volume 差分）
- [x] 实现 code.csv 加载（循环读完所有 chunk）与刷新
- [x] 单元测试：校验规则、OHLCV 聚合、边界条件

## Phase 4: 输出与原子写入 `[complete]`
- [x] 实现 `AtomicWriter` — .tmp + rename 写入
- [x] 实现 snapshot_minute CSV 输出 — 保留全部原始列 + name + seqno + update_flag
- [x] 实现 kline_minute CSV 输出（除 decimal 后的 K 线数据）
- [x] 每个分钟文件保留该分钟内所有记录（不丢弃中间更新）
- [x] 单元测试：输出格式、原子写入、幂等性

## Phase 5: 时钟驱动与 Checkpoint `[complete]`
- [x] 实现 `ClockWatermarkFlusher` — 纯时钟驱动过期检查
- [x] 实现跨日检查逻辑 (Step 1)
- [x] 实现首次数据检查 (Step 2)
- [x] 实现分钟输出与断流追赶 (Step 3)
- [x] 实现 `CheckpointManager` — JSON 读写、恢复
- [x] 实现重启恢复（从 snapshot_minute 文件恢复 latest_snapshot）
- [x] 单元测试：watermark 过期、跨日、重启恢复

## Phase 6: 多线程与主程序 `[complete]`
- [x] 实现数据线程（FileTailer + Parser + Aggregator 循环）
- [x] 实现时钟线程（每秒检查 + 输出）
- [x] 实现共享状态与 RLock
- [x] 实现交易时段轮询频率切换
- [x] 实现 `main.py` 入口（argparse + 配置加载 + 线程启动）
- [x] 实现 error log（RotatingFileHandler）
- [x] 集成测试：端到端数据流

## Phase 7: Replay 模式与验收测试 `[complete]`
- [x] 实现 `ReplayEngine` — 离线回放历史数据，单线程逐分钟处理
- [x] 按 `--replay YYYYMMDD` 过滤目标日期数据
- [x] 逐分钟处理确保 snapshot 状态正确（非全天最终状态）
- [x] `enable_kline = false` 配置跳过 K 线输出
- [x] 创建 systemd service 配置
- [x] 60 个单元测试和集成测试全部通过
- [x] 0519 真实数据回放验证通过（702MB snapshot, 329 分钟）

## Phase 8: Order 分钟文件 `[complete]`
- [x] 实现 `ParsedOrder` 数据结构（6-8 列：symbol, time, bidprice, bidsize, askprice, asksize, decimal?, rcvtime?）
- [x] 实现 `parse_order_line`（在 csv_parser.py 中，按列位置索引，自动跳过 header）
- [x] 实现 `OrderRecord` (frozen dataclass，保留原始值不做任何处理，仅按 time 切分)
- [x] 在 `SharedState` 中添加 `raw_order_buffers: Dict[str, List[OrderRecord]]` + `process_order` 方法
- [x] 实现 `write_order_file` — 保留全部原始列 + seqno，输出 order_minute 文件
- [x] 在 `ClockWatermarkFlusher` 中集成 order 输出（含跨日处理）
- [x] 在 `ReplayEngine` 中集成 order 回放（`_collect_raw_data` 复用于 order）
- [x] 添加 `enable_order` 配置项（默认 true）
- [x] 单元测试：order 解析、输出格式、replay 端到端
- [x] 无 order.csv 文件时不报错，正常输出 snapshot

## Phase 9: snapshot/order 并行处理 + 流式优化 `[complete]`
- [x] ReplayEngine 重写为流式架构（不再全量加载，峰值内存 ~40MB vs 3-5GB）
- [x] ReplayEngine snapshot/order 并行处理（主线程 + ThreadPoolExecutor）
- [x] ReplayEngine stop_event 快速失败机制
- [x] ReplayEngine EOF final flush + 迟到数据 WARNING
- [x] ReplayEngine 生成 `replay_summary_{date}.json`
- [x] Engine 新增 order 独立流式线程（`_order_loop` + `_order_tailer`）
- [x] Engine 4 种 order flush 触发：record-driven / watermark-driven / stop-driven / cross-day
- [x] Engine `committed_offset` 机制（per-minute offset 精度，写入成功才推进 checkpoint）
- [x] Engine `checkpoint_lock` 保护 FileState 一致性（order 线程 + clock 线程）
- [x] Engine 内存保护 `MAX_PENDING_ORDER_MINUTES=3`（超限强制 flush + WARNING 日志）
- [x] Engine 线程异常传播（`_order_thread_error` → data/clock 线程检测并 re-raise）
- [x] FileTailer 新增 `line_offset` 属性（per-line byte offset 追踪）
- [x] Flusher 新增 `checkpoint_lock` 可选参数（不改变现有逻辑）
- [x] 70 个测试全部通过（输出格式不变，seqno 独立）

## Phase 10: 迟到记录统一处理 `[complete]`
- [x] `writer.py` 新增 `append_order_records` / `append_snapshot_records`（追加行到已有文件，不写 header）
- [x] `writer.py` 新增 `_get_write_lock`（按文件路径粒度加锁，防止并发 append + atomic_write 冲突）
- [x] `writer.py` 新增 `get_snapshot_file_path` / `get_order_file_path`（路径计算公共函数）
- [x] `writer.py` 修改 `atomic_write` 使用文件级写锁
- [x] `aggregator.py` SharedState 新增 `flushed_snapshot_minutes` / `_late_snapshot_records` / `late_snapshot_count` / `late_snapshot_minutes`
- [x] `aggregator.py` SharedState 新增 `pop_late_snapshot_records()` / `maybe_update_latest_unlocked()`
- [x] `aggregator.py` `process_snapshot` 增加迟到检测：`minute_key in flushed_snapshot_minutes` → 路由到 `_late_snapshot_records`
- [x] `flusher.py` 新增 `_step4_handle_late_records`（从 `_late_snapshot_records` 取出并 append 到已有文件）
- [x] `flusher.py` 新增 `_step5_write_checkpoint`（检查点移到 late append 之后，跳过有 pending late records 的轮次）
- [x] `flusher.py` `_step3_minute_output` 记录 `flushed_snapshot_minutes`
- [x] `flusher.py` `_step1_cross_day_check` 跨日清理 late 统计字段
- [x] `engine.py` 新增 `recover_flushed_minutes`（从输出目录扫描已有文件恢复 flushed_minutes）
- [x] `engine.py` `_order_loop` 增加迟到检测和直接 append
- [x] `engine.py` `_flush_order_minute` 记录 `_flushed_order_minutes`
- [x] `engine.py` `_restore_from_checkpoint` 调用 `recover_flushed_minutes` 恢复重启状态
- [x] `replay.py` `_stream_snapshots` 增加迟到检测 + `_flush_late_snapshots`
- [x] `replay.py` `_stream_orders` 增加迟到检测 + 直接 append
- [x] `replay.py` EOF final flush 修改：已 flush 分钟的 residual buffer → late append
- [x] `replay.py` `_write_summary` 新增 late record 统计字段
- [x] 108 个测试全部通过（+38 新测试，零回归）

### 修改文件
| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `writer.py` | 修改 | 新增 append 函数、文件级写锁、路径函数；`atomic_write` 加锁 |
| `aggregator.py` | 修改 | SharedState 新增 late 字段和方法；`process_snapshot` 迟到检测 |
| `flusher.py` | 修改 | 新增 step4/step5；记录 flushed_snapshot_minutes；跨日清理；checkpoint 安全 |
| `engine.py` | 修改 | 新增 recover_flushed_minutes；order 迟到检测；flushed_minutes 恢复 |
| `replay.py` | 修改 | 两个 stream 增加迟到检测；EOF flush 兜底；summary late 统计 |

### 修改文件
| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `replay.py` | 重写 | 流式架构 + 并行处理 + summary |
| `engine.py` | 修改 | 新增 order_tailer + order_thread + order_loop + checkpoint_lock |
| `file_tailer.py` | 修改 | 新增 `line_offset` per-line offset 追踪 |
| `flusher.py` | 修改 | 新增 `checkpoint_lock` 可选参数 |

## Phase 11: 数据模拟器（Live 端到端测试工具） `[complete]`
- [x] 设计文档 `docs/superpowers/specs/2026-05-22-data-simulator-design.md`
- [x] 创建 `src/data_simulator/` 包（`__init__.py`, `simulator.py`, `__main__.py`）
- [x] 实现核心回放逻辑：从 `input/` 读取历史 CSV，追加写入 `test/output/`
- [x] 实现 global_min_time 共享时间基准（order/snapshot 时间关系真实）
- [x] 实现 `--order-mode original`（保留源文件原始乱序，默认）和 `--order-mode time`（按时间戳排序）
- [x] 实现 `--code-mode preload`（启动时一次性写入，默认）和 `--code-mode stream`（按行追加）
- [x] 实现半行写入模拟（`--split-line-prob` + `--split-line-delay-ms`）
- [x] 实现 late record 注入（`--late-prob` + `--late-delay-sec`，独立线程延迟写入）
- [x] 实现批量 flush（`--batch-size` + `--flush-interval-ms`）
- [x] 实现文件级写锁（防止 late writer 与主线程并发写入竞争）
- [x] 实现安全清理（`--clean` 只删除匹配的 CSV 文件，打印列表）
- [x] 修复 `original` 模式内存问题（46M 行文件流式处理，不预加载）
- [x] 修复文件句柄泄漏（handles 提升为实例变量，`stop()` 中关闭，防重入）
- [x] 更新 `pyproject.toml` 添加 data_simulator 包
- [x] 验证通过：1000x 速度 5 秒写入 129 万行；split+late 模式 0 bad line

### 新增文件
| 文件 | 说明 |
|------|------|
| `src/data_simulator/__init__.py` | 包标记 |
| `src/data_simulator/__main__.py` | CLI 入口，所有参数定义 |
| `src/data_simulator/simulator.py` | 核心回放逻辑（Simulator、_BatchWriter、_LateWriter） |

## Phase 12: Data-Driven Watermark Flush `[complete]`

- [x] 设计文档 `docs/superpowers/specs/2026-05-25-data-driven-watermark-design.md`（1300+ 行，6 轮评审收敛）
- [x] 实现计划 `docs/superpowers/plans/2026-05-25-data-driven-watermark.md`
- [x] `clock.py`：新增 `minute_key_to_start_time()`（datetime.strptime 完整校验）、`is_data_driven_expired()`、`STALL_WARN_SECONDS = 300`
- [x] `config.py`：`RecoveryConfig` 新增 `data_flush_delay_minutes`、`enable_time_fallback`；`load_config` 解析新字段
- [x] `flusher.py`：`_step3_minute_output` 改为数据驱动+fallback 双判定（OR 架构）；新增 `_flush_minutes_internal` 共享方法（lock 内 pop → lock 外 IO → lock 内更新状态）；新增 `flush_all_remaining`（shutdown final flush）；`_step1_cross_day_check` 新增 watermark 零点基准条件赋值；watermark 停滞检测（`time.monotonic()` wall-clock + 恢复日志）
- [x] `engine.py`：`_order_loop` 处理顺序重构（parse → cross-day → seqno → late → flush → buffer → watermark）；record-driven flush `!=` 改 `>`；`_flush_expired_order_minutes` 新增 `order_watermark` 参数；`stop()` 完整重写（final flush + 异常安全 + flush_error 保留原始异常对象 + `from` 链式传播 + 资源独立 try-except + join 超时线程栈 dump）；`data_flush_delay_minutes` 非负+上限校验
- [x] `aggregator.py`：`process_snapshot` 中 `current_minute` 改为单调递增条件赋值
- [x] 测试：`test_clock.py`（minute_key + is_data_driven_expired）、`test_config.py`（新配置项）、`test_watermark_flusher.py`（数据驱动 flush、fallback、防御性 pop、final flush、跨日重置、停滞检测）、`test_watermark_engine.py`（order loop 处理顺序、单调 watermark、stop final flush、跨日安全）、`test_flusher.py`（构造函数适配）
- [x] 配置：`config-watermark-test.ini`（`enable_time_fallback = false` 测试环境配置）

### 修改文件
| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `clock.py` | 修改 | 新增 minute_key_to_start_time + is_data_driven_expired + STALL_WARN_SECONDS |
| `config.py` | 修改 | RecoveryConfig 新增 2 字段；load_config 解析新 INI 参数 |
| `flusher.py` | 修改 | _step3 数据驱动重写；新增 _flush_minutes_internal / flush_all_remaining / 停滞检测；_step1 跨日 watermark 重置 |
| `engine.py` | 修改 | _order_loop 处理顺序重构；stop() 完整重写；_flush_expired_order_minutes 新增 watermark 参数 |
| `aggregator.py` | 修改 | process_snapshot current_minute 单调递增 |
| `test_clock.py` | 新增 | minute_key_to_start_time + is_data_driven_expired 测试 |
| `test_config.py` | 新增 | 新配置项测试 |
| `test_watermark_flusher.py` | 新增 | flusher 端 watermark 测试 |
| `test_watermark_engine.py` | 新增 | engine 端 watermark 测试 |
| `config-watermark-test.ini` | 新增 | 测试环境配置 |

### 设计决策记录
- flush 主判定：`current_minute`（数据进度）替代 `now_jst()`（真实时钟）
- OR 架构：数据驱动始终启用 + 真实时钟 fallback 可配置（`enable_time_fallback`，默认 true）
- `_flush_minutes_internal`：共享方法，`_step3` 和 `flush_all_remaining` 复用
- `stop()` 异常安全：flush_error 保留原始异常对象 + `from` 链式传播 + 资源独立 try-except
- 跨日：SharedState 零点基准条件赋值（防跨线程竞态），Order loop None + try-finally 清理旧 buffer
- 停滞检测：`time.monotonic()` wall-clock，单次告警 + 恢复 INFO 日志
- 配置校验：`data_flush_delay_minutes` 非负 ValueError，>10 WARNING

### Live 测试发现的问题（Phase 13 已全部修复）
| # | 问题 | 优先级 | Phase 13 修复方案 | 状态 |
|---|------|--------|-------------------|------|
| 1 | Snapshot carry-forward 包含未来时间戳 | P1 | Per-minute snapshot + writer 时间过滤 | ✅ 已修复 |
| 2 | 1530.csv 未生成 | P1 | Stall-triggered flush（`stall_flush_sec` 可配置） | ✅ 已修复 |
| 3 | 0913 前 name 缺失 | P2 | `code_refresh_sec=1` 配置优化 | ✅ 已验证 |

## Phase 13: Carry-Forward Fix + Stall-Triggered Flush `[complete]`

- [x] 设计文档 `docs/superpowers/specs/2026-05-25-carry-forward-future-data-fix.md`（含三 Agent 评审）
- [x] `aggregator.py`：新增 `_snapshot_at_minute_end`，`process_snapshot` 在 minute advance 前捕获快照
- [x] `flusher.py`：`_flush_minutes_internal` 使用分钟级快照；stall 检测后 flush 所有残余 ohlcv buffers
- [x] `replay.py`：`_flush_snapshot_minute` 弹出分钟级快照
- [x] `writer.py`：新增 `_minute_end_threshold()` + carry-forward 时间过滤（`rec.time >= minute_end` 跳过 + WARNING 日志）
- [x] `config.py`：`RecoveryConfig` 新增 `stall_flush_sec`（默认 300）
- [x] `engine.py`：`_order_loop` 新增独立 stall 检测，传递 `stall_flush_sec` 到 flusher
- [x] `clock.py`：`STALL_WARN_SECONDS` 标注 deprecated
- [x] 3 个 INI 配置新增 `stall_flush_sec`
- [x] 测试：TestPerMinuteSnapshot (6)、TestMinuteEndThreshold (3)、TestCarryForwardTimeFilter (4)、TestStallDetection (5)、config (2) — 共 176 passed

### 修改文件
| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `aggregator.py` | 修改 | 新增 `_snapshot_at_minute_end`；`process_snapshot` 分钟推进前捕获快照 |
| `flusher.py` | 修改 | 弹出分钟级快照；stall-triggered flush ohlcv buffers；新增 `stall_flush_sec` 参数 |
| `replay.py` | 修改 | `_flush_snapshot_minute` 弹出分钟级快照 |
| `writer.py` | 修改 | 新增 `_minute_end_threshold()`；carry-forward 时间过滤 |
| `config.py` | 修改 | `RecoveryConfig` 新增 `stall_flush_sec` |
| `engine.py` | 修改 | 传递 `stall_flush_sec`；`_order_loop` stall 检测 + flush order buffers |
| `clock.py` | 修改 | `STALL_WARN_SECONDS` 标注 deprecated |
| `config/*.ini` | 修改 | 3 个 INI 新增 `stall_flush_sec` |
| `test_aggregator.py` | 新增 | TestPerMinuteSnapshot (6 tests) |
| `test_writer.py` | 新增 | TestMinuteEndThreshold (3) + TestCarryForwardTimeFilter (4) |
| `test_watermark_flusher.py` | 新增 | stall flush 触发/不触发/恢复 (3 tests) |
| `test_config.py` | 修改 | `stall_flush_sec` 默认值 + 解析测试 |

### Live 验证结果
- 329 snapshot + 417 order 文件，1530 均正常生成
- 0 条 carry-forward 未来数据
- `stall_flush_sec=30` 测试环境配置，`stall_flush_sec=300` 生产配置

## Phase 14: Order 线程性能优化 `[complete]`

- [x] 设计文档 `docs/superpowers/specs/2026-05-27-order-write-performance-design.md`（3 Agent × 4 轮评审，0 CRITICAL/IMPORTANT，已收敛）
- [x] `engine.py` `_order_loop`：drain loop 连续读取 + 每 100 次保护检查
- [x] `engine.py` `__init__`：order tailer 使用 `config.input.order_chunk_size_bytes`
- [x] `config.py` `InputConfig`：新增 `order_chunk_size_bytes` 字段 + 解析
- [x] `writer.py` `write_order_file`：streaming write + 保留锁 + 去 fsync + 1MB buffer
- [x] `config/production.ini`：新增 `order_chunk_size_bytes = 524288`（已添加）
- [x] 新增测试：drain loop 行为、streaming write 输出一致性、配置解析（186 passed, +10 新测试）
- [x] 端到端验证：data_simulator(100x) + live + replay，70,809,005 条 order 记录 0 差异

### 设计要点
| 改动 | 说明 |
|------|------|
| Drain loop | 有数据时连续读取（不 sleep），无数据时走配置间隔 |
| 512KB chunk | `order_chunk_size_bytes=524288`，生产环境专用，默认 65KB 向后兼容 |
| Streaming write | 逐行 `f.write` 替代 `"\n".join()` 巨字符串，1MB I/O buffer |
| 去 fsync | Order 文件是派生数据可 replay 重生成，checkpoint 写入仍 fsync |
| 保留锁 | `_get_write_lock` 防御性保留（当前无竞态，`raw_order_buffers` 管线预留） |

### 性能预期（已验证）
| 指标 | 优化前 | 优化后（实测） |
|------|--------|---------------|
| 读取吞吐量 | 19.5 MB/min | 50-100 MB/min |
| 单次写入 50MB | 0.8-2.6s | 0.15-0.4s |
| Order 生成延迟 | 57+ min（1005） | **0 延迟**（全程无积压） |
| 端到端数据完整性 | — | **70,809,005 条记录，live vs replay 0 差异** |

### 端到端验证结果（2026-05-27）
- **输入数据**：`order.csv.20260525`（4.4GB, 70,809,006 行）+ snapshot + code
- **Live pipeline**：data_simulator(speed=100) → minute_bar → `test/live_output/`
- **Replay**：`test/replay_output/`
- **Order 结果**：417 分钟文件，70,809,005 条记录，live vs replay **0 差异**，vs 源数据 **0 丢失**
- **Snapshot 结果**：329 分钟文件，update_flag=Y 的 3 个边界分钟（1128/1129/1524）记录数不一致（drain loop 未覆盖 `_data_loop`）

### 遗留问题：Snapshot `_data_loop` 缺少 drain loop
- **现象**：1128/1129/1524 三个分钟的 update_flag=Y 记录数：live 远少于 replay
- **根因**：`_data_loop` 仍用 `for line in read_lines()` + 固定 sleep 模式。data_simulator 100x 速度下，watermark 在 1128/1129 数据未完全到达前就跳到 1130（lunch break），导致这些分钟被提前 flush
- **修复方案**：给 `_data_loop` 加 drain loop（与 `_order_loop` 同模式），~10 行改动

## Phase 15: Snapshot Drain Loop + Flusher Double-Flush Bug Fix `[complete]`

### Phase 15a: Snapshot Drain Loop `[complete]`

- [x] `engine.py` `_data_loop`：drain loop 重构（与 `_order_loop` 同模式）
- [x] 端到端验证：1128/1129 修复生效

### Phase 15b: Flusher Double-Flush Bug `[complete]`

- [x] 设计文档 `docs/superpowers/specs/2026-05-27-flusher-double-flush-bug-design.md`（4 轮 12 agent 评审，0 CRITICAL，已收敛）
- [x] `flusher.py` `_step3_minute_output`：分流 expired_keys 为 normal vs reflush
- [x] `flusher.py` 新增 `_reroute_buffer_to_late_queue`：buffer 数据路由到 late queue
- [x] `flusher.py` `flush_all_remaining`：增加 `_step4_handle_late_records()` 调用
- [x] 新增 11 个测试（202 passed，0 回归）
- [x] 端到端验证：329 snapshot 文件 update_flag=Y 全部一致，1524 从 781 行恢复到 44,696 行

### 设计要点

| 改动 | 说明 |
|------|------|
| `_step3_minute_output` 分流 | `already_flushed` 在同一锁块内检查，normal_keys → 正常 flush，reflush_keys → re-route |
| `_reroute_buffer_to_late_queue` | pop ohlcv（丢弃）+ pop raw_snapshot（路由到 late queue）+ pop _snapshot_at_minute_end（防内存泄漏） |
| `flush_all_remaining` 补充 | 增加 `_step4_handle_late_records()` 处理 shutdown 前 re-route 但未处理的 late records |
| 不改的文件 | `config.py`、`writer.py`、`aggregator.py`、`engine.py`、`replay.py` |

### 设计评审记录
- 4 轮 × 3 agent = 12 agent 审查
- 第 1 轮：根因确认 + 修复方案核心思路验证
- 第 2 轮：TOCTOU 合并锁块 + stall/cross-day 豁免 + kline 影响分析 + flush_all_remaining shutdown 路径修复
- 第 3 轮：记录重写入专项检查（确认无重复写入）+ order 数据独立性确认 + carry-forward 正确性
- 第 4 轮（最终）：数据完整性 ✅ + 可实施性 5/5 + 文档一致性 ✅

## Phase 16: Tickfile 生成功能 `[complete]`

### 设计文档
- `docs/superpowers/specs/2026-06-01-tickfile-generation-design.md`（Round 14, 15 轮 45-agent 审阅，0 Critical / 0 Major）
- 实施计划：`docs/superpowers/plans/2026-06-02-tickfile-generation.md`

### 概述
基于 snapshot + order 分钟数据生成 65 列 CSV tickfile（全天一个文件，每分钟 append）。支持 live 和 replay 两种模式。

### 全局实施步骤（按推荐顺序）
- [x] **Step 1**: `config.py` — `OutputConfig` 新增 `enable_tickfile: bool = False`
- [x] **Step 2**: `aggregator.py` — `SharedState.__init__` 新增 `latest_order_by_symbol`，`process_order` 增加 cache 更新并标记 TEST ONLY
- [x] **Step 3**: 新建 `tickfile.py` — 实现 `TICKFILE_HEADER`、`build_tickfile_row`、`select_tickfile_records`
- [x] **Step 4**: `writer.py` — 新增 `get_tickfile_path`、`write_tickfile_rows`（含末行截断修复）、`recover_tickfile_seqno`
- [x] **Step 5**: `flusher.py` — 修改 `__init__`、`_flush_minutes_internal`、`_write_minute_files`、`_step1_cross_day_check`
- [x] **Step 6**: `engine.py` — `_order_loop` 新增 SharedState 批处理写入（`LATE_CACHE_MARKER` object sentinel）
- [x] **Step 7**: `replay.py` — 按 Implementation Checklist 重构 SharedState 共享（`state` → `self._state`）

### 关键设计决策
| 决策 | 选择 | 理由 |
|------|------|------|
| 文件策略 | 全天一个文件，每分钟 append | 需求 a10,11 要求 |
| Seqno | per-minute 全局递增，ephemeral（不持久化） | crash 后 `recover_tickfile_seqno` 从文件恢复 |
| Order carry-forward | `latest_order_by_symbol` cache，无过期限制 | 需求 a9 规定 |
| Live order bridge | `_order_loop` batch-write 到 SharedState | 与 replay 模式一致的 SharedState bridge 模式 |
| Replay order 获取 | atomic pop `raw_order_buffers` | 防止 copy-then-pop 竞态窗口 |
| 跨日处理 | order cache 按日期过滤清空，snapshot cache 保留 | order 时效性强，snapshot 有参考价值 |
| 首次写入 | `atomic_write`（.tmp + rename） | 与现有 snapshot/order 一致 |
| 后续写入 | append + `f.flush()` + `os.fsync()` | 确保每分钟数据落盘 |
| NA 表示 | 字符串 `NA` | 需求文档要求 |
| INVARIANT | raw_records 仅含实时数据（等价 update_flag=Y） | 隐式实现需求 A9 规则 |

### 核心模块改动
| 模块 | 改动 |
|------|------|
| `config.py` | `OutputConfig.enable_tickfile` |
| `aggregator.py` | `SharedState.latest_order_by_symbol` + `process_order` TEST ONLY |
| `tickfile.py`（新建） | `TICKFILE_HEADER`（65列）、`build_tickfile_row`、`select_tickfile_records` |
| `writer.py` | `get_tickfile_path`、`write_tickfile_rows`、`recover_tickfile_seqno` |
| `flusher.py` | `_flush_minutes_internal`（2个lock scope均拷贝）、`_write_minute_files`、`_step1_cross_day_check` |
| `engine.py` | `_order_loop` SharedState batch-write + `LATE_CACHE_MARKER` |
| `replay.py` | SharedState 重构（state→self._state）+ `_flush_snapshot_minute` tickfile 生成 |

### 测试覆盖（71 个测试用例，实际实施）
- `tests/test_tickfile.py`（59 个）：TICKFILE_HEADER 3 + build_tickfile_row 45 + select_tickfile_records 12
- `tests/test_writer.py` 新增 13 个：get_tickfile_path 2 + write_tickfile_rows 6 + recover_tickfile_seqno 5
- 总测试 278 passed（1 pre-existing failure，与本改动无关）

### 已知限制（设计如此）
- ~~Replay tickfile order 字段可能依赖 carry-forward（order 线程系统性慢于 snapshot）~~ → Phase 17 修复
- Late record 不触发 tickfile 重新生成
- Crash recovery 可能产生重复行（不做在线去重）
- Order carry-forward 无过期限制
- `enable_order=False` + `enable_tickfile=True`：所有 order 字段输出 NA

## Phase 17: Tickfile 同步生成机制 `[complete]`

### 问题
Live 模式下，tickfile 生成绑定在 snapshot flush 上。由于 order 数据量是 snapshot 的 ~8 倍，order 线程系统性滞后于 snapshot 线程。导致 tickfile 写入时 `raw_order_buffers[该分钟]` 为空，只能用 `latest_order_by_symbol` carry-forward 数据（覆盖率 95.8% 但非当前分钟真实 order）。

### 根因
```
当前流程（有问题）：
  snapshot flush → pop raw_order_buffers[该分钟] → 立即写 tickfile
  （order 还没处理完该分钟 → raw_order_buffers 为空或部分 → 用 carry-forward）

期望流程：
  snapshot flush → 写 snapshot 文件，暂存 tickfile 所需 snapshot 数据到 _tickfile_pending
                   （不 pop raw_order_buffers）
  order flush    → 写 order 文件，检查 _tickfile_pending 是否有该分钟
                  → 有 → pop raw_order_buffers（此时数据完整）→ 生成 tickfile
  EOF 兜底       → 对 _tickfile_pending 残余分钟用 carry-forward 生成 tickfile
```

### 设计文档
- **Spec**: `docs/superpowers/specs/2026-06-03-tickfile-sync-design.md`（Round 8 Final，8 轮 24+ agent 审阅，最近 5 轮零 Critical，Status: Review Passed）
- **Implementation Plan**: 待创建（writing-plans skill）

### 核心设计

Tickfile 从"snapshot flush 同步写"改为"两线程 join 后写"。Tickfile 是 snapshot 分钟文件和 order 分钟文件的下游产物，两者都完成才生成。

#### 关键机制

| 机制 | 说明 |
|------|------|
| **`_tickfile_pending`** | Dict[str, dict] — 已 flush snapshot 但等 order 的 tickfile 数据。存在 SharedState 中，两个线程共享 |
| **`_drain_tickfile_triggers`** | Engine helper method — batch write 后更新 `order_current_minute` + catch-up scan + 触发 tickfile 生成 |
| **Deferred trigger** | `_flush_order_minute` 仅记录 minute 到 `_tickfile_trigger_pending`（order-thread-only list），不更新 `order_current_minute`。Drain 在 batch write 后执行 |
| **Catch-up scan** | drain 内扫描 `_tickfile_pending`，发现 `mk <= order_current_minute` 的 pending 条目也加入触发列表（填补 Case B gap） |
| **3 drain points** | (1) batch write 后 (2) 内层循环 `_flush_expired_order_minutes` 后 (3) 外层循环 `_flush_expired_order_minutes` 后 |
| **`>` vs `<=`** | Clock thread 用 `>` 触发（跳过 `==` 边界），order thread catch-up 用 `<=`（覆盖 `==`）|
| **Cross-day force-generate** | `_step1_cross_day_check` 先 force-generate 所有 pending tickfile，再 atomic cleanup（clear + orphaned raw_order_buffers）|
| **MAX_TICKFILE_PENDING_MINUTES=10** | Overflow protection，tick() 中检查，force-generate 最旧 pending |

#### 时序图
```
Case A: Snapshot 先完成（典型）
  Clock Thread:      flush snapshot X → store _tickfile_pending[X]
  Order Thread:      flush order X → _tickfile_trigger_pending.append(X)
  Order Thread:      batch write → raw_order_buffers[X] complete
  Order Thread:      _drain_tickfile_triggers:
                       order_current_minute = X, catch-up scan, _try_generate_tickfile(X) ✅

Case B: Order 先完成（罕见）
  Order Thread:      flush order X → _tickfile_trigger_pending.append(X)
  Order Thread:      batch write → _drain_tickfile_triggers → order_current_minute = X
                     → _try_generate_tickfile(X) → _tickfile_pending[X] NOT FOUND → no-op
  Clock Thread:      flush snapshot X → _tickfile_pending[X] = data
  Order Thread:      next drain → catch-up scan finds X <= order_current_minute → generate ✅

Case C: EOF 兜底
  Clock Thread:      flush_all_remaining → generate remaining pending with carry-forward ✅

Case D: Pending overflow（order 极端滞后）
  Clock Thread:      tick() overflow check → force-generate oldest pending with carry-forward ✅

Case E: Cross-day 兜底
  Clock Thread:      _step1_cross_day_check → force-generate all remaining pending → clear ✅
```

### 设计评审历史

8 轮审阅（24+ agent runs），完整闭环：

| Round | Critical | Major | 修复内容 |
|-------|----------|-------|---------|
| R1 | 7 | 10 | 基础设计修复（batch-write timing、cross-day pop） |
| R2 | 0 | — | 验证通过 |
| R3 | 0 | — | 3-agent 验证通过 |
| R4 | 3 | — | batch-write timing + cross-day pop |
| R5 | 5 | — | reroute pending + cross-day orphan + order_current_minute timing + expired drain + writer silent return |
| R6 | 0 | 7 (docs) | config scope + stale comment + writer paths + impl ordering + test gaps |
| R7 | 0 | 2 | third drain point + readlines I/O amplification note |
| **R8** | **0** | **1** | **re-insert replace vs extend** |

**最近 5 轮（R4-R8）均零 Critical**。所有 Major 已修复并经 Round 2 agents 验证 PASS。

### 25 INVARIANTS

| # | Invariant |
|---|-----------|
| 1 | `_tickfile_pending` 数据在 tickfile 生成后立即清除 |
| 2 | `raw_order_buffers[minute_key]` 仅在 tickfile 生成时 pop（enable_tickfile=True 时） |
| 3 | Order 线程对 `_tickfile_pending` 中的分钟继续写 `raw_order_buffers` |
| 4 | `_tickfile_seqno` 单调递增（同一日内），跨日重置为 0 |
| 5 | `_flushed_order_minutes` 保持 engine local |
| 6 | Tickfile 生成失败不影响 order/snapshot 文件写入 |
| 7 | `enable_tickfile=False` 时所有新代码不触发 |
| 8 | `order_current_minute` 仅在 `_drain_tickfile_triggers` 中更新（batch write 之后） |
| 9 | `order_current_minute` 跨日时双保险重置（flusher + engine） |
| 10 | `_tickfile_pending` 不超过 `MAX_TICKFILE_PENDING_MINUTES=10` |
| 11 | `flush_all_remaining` 后 `_tickfile_pending` 应为空（WARNING only） |
| 12 | IO 失败时 re-insert pending + raw_order_buffers（replace 模式） |
| 13 | `_tickfile_pending` entry 和 `flushed_snapshot_minutes.add()` 在同一 lock scope |
| 14 | 跨日清理前先 force-generate 所有残留 pending |
| 15 | Seqno gap 是 acceptable 的 |
| 16 | Lock ordering: `state.lock` 在 `checkpoint_lock` 之前 |
| 17 | `enable_tickfile=True` 要求 `enable_order=True`（ValueError） |
| 18 | Reroute 同时 pop `raw_order_buffers` 和 `_tickfile_pending` | → Phase 19 Fix-A 修改为：**reroute 仅 pop `raw_order_buffers`，不 pop `_tickfile_pending`**（INV-TF1） |
| 19 | Tickfile trigger 仅在 batch write 后执行（`_tickfile_trigger_pending` deferred list） |
| 20 | 跨日第一个 lock scope 中 `raw_order_buffers` pop 在 `enable_tickfile=True` 时跳过 |
| 21 | 跨日 cleanup lock scope 清除所有昨日 `raw_order_buffers` 孤立条目 |
| 22 | `_drain_tickfile_triggers` 在 batch write 后和每次 `_flush_expired_order_minutes` 后均被调用（三处 drain points） |
| 23 | drain catch-up scan 发现 `mk <= order_current_minute` 的 pending 也加入触发列表 |
| 24 | `write_tickfile_rows` 在不可恢复错误时必须 raise（非 silent return） |
| 25 | `_tickfile_trigger_pending` 仅 order thread 访问 |

### 实施步骤（全部完成）

#### Step 1: `aggregator.py` — SharedState 新增字段 `[complete]`
- [x] 新增 `_tickfile_pending: Dict[str, dict]` — 存储等 order 的 snapshot 数据
- [x] 新增 `_tickfile_seqno: int = 0` — 从 flusher 迁移到 SharedState
- [x] 新增 `order_current_minute: str = ""` — order 线程处理进度

#### Step 2: `writer.py` — 修复 silent return `[complete]`
- [x] line 334-336: corrupt header → raise IOError
- [x] line 291-296: all rows failed → raise IOError

#### Step 3: `flusher.py` — 解耦 tickfile 写入 `[complete]`
- [x] 移除 `self._tickfile_seqno` 实例变量（改用 `self._state._tickfile_seqno`）
- [x] Config validation: `if not enable_order: raise ValueError`
- [x] `recover_tickfile_seqno_lazy` 使用 `jst_now_yyyymmdd()`
- [x] `_write_minute_files` 移除 tickfile block（Section 4.2.3）
- [x] `_flush_minutes_internal` skip pop `raw_order_buffers`（Section 4.2.4）
- [x] `_flush_minutes_internal` 第二 lock scope store pending + 触发
- [x] 新增 `_try_generate_tickfile` 方法
- [x] Pending overflow protection（`MAX_TICKFILE_PENDING_MINUTES=10`）
- [x] Cross-day force-generate + orphaned cleanup
- [x] `flush_all_remaining` tickfile fallback（WARNING only）
- [x] `_reroute_buffer_to_late_queue` pop `_tickfile_pending`

#### Step 4: `engine.py` — Drain helper + 条件修改 `[complete]`
- [x] 新增 `_tickfile_trigger_pending: list` 字段（order-thread only）
- [x] 新增 `_drain_tickfile_triggers` helper method
- [x] `_flush_order_minute` 仅 append trigger（不更新 order_current_minute）
- [x] 3 处 drain call sites
- [x] `raw_order_buffers` 写条件修改（Section 4.3.2）
- [x] 跨日重置 `order_current_minute` + `_flushed_order_minutes.clear()`

#### Step 5: `replay.py` — 确认不改 `[complete]`
- [x] 验证 replay 行为不受影响

#### Step 6: 测试 `[complete]`
- [x] 新建 `tests/test_tickfile_sync.py`（31 个测试）
- [x] 279+ 现有测试全部通过
- [x] E2E live test + replay diff 对比

### 文件改动清单

| 文件 | 改动类型 | 改动量 |
|------|---------|--------|
| `aggregator.py` | 修改 | +7 行 |
| `writer.py` | 修改 | ~5 行 |
| `flusher.py` | 修改 | ~90 行 |
| `engine.py` | 修改 | ~48 行 |
| `replay.py` | 确认不改 | 0 |
| `test_tickfile_sync.py` | 新建 | ~490 行 |

### 上线前 Checklist
- [x] 31 unit tests PASS
- [x] 279+ regression tests PASS
- [x] E2E: bid/ask >99% 覆盖率（真实 order 非 carry-forward）
- [x] E2E: replay diff 一致
- [x] 日志无 pending >10 WARNING
- [x] Case A（order thread）为主要触发路径
- [x] write_tickfile_rows readlines() 优化 → Phase 18 已实施

### Live E2E 测试发现的问题（Phase 17 Live Test）

**Order Thread Stall（已在 Phase 18 修复）**
- **现象**：Live E2E（speed=100）中 order thread 在 `_drain_tickfile_triggers` 中被 tickfile IO 阻塞 3+ 分钟
- **根因**：catch-up scan 积累 ~58 pending 分钟 × `readlines()` 全文件读取 × `fsync()` + daily 文件锁竞争
- **修复**：Phase 18 Background Writer + Seek Optimization
  - 详见 [[tickfile-order-thread-stall]] 和 [[tickfile-bg-writer-design]]

## Phase 18: Tickfile Background Writer + Seek Optimization `[complete]`

### 设计文档
- **Spec**: `docs/superpowers/specs/2026-06-04-tickfile-bg-writer-design.md`（v11, 4 轮 12-agent review, 43 invariants）
- **Implementation Plan**: `docs/superpowers/plans/2026-06-04-tickfile-bg-writer.md`（14 tasks, 72 steps）

### 概述
替换同步 tickfile IO 为后台 writer thread + seek 优化，消除 order thread 被 tickfile IO 阻塞的问题。

### 核心机制

| 机制 | 说明 |
|------|------|
| **Background TickfileWriterThread** | Daemon 线程，串行消费 unbounded queue，sole tickfile IO |
| **Unbounded queue** | `queue.Queue(maxsize=0)`，仅存 minute_key 字符串（~12 bytes），enqueue 永不阻塞 |
| **Sentinel** | `_TICKFILE_SENTINEL_STOP = object()`（非 None），通知 writer 停止 |
| **Seek 优化** | `seek(-4KB, 2)` 替代 `readlines()`，单次 IO 从 ~200ms 降到 ~30ms |
| **skip_fsync** | Live Engine 跳过 fsync（tickfile=衍生数据），ReplayEngine 保留 |
| **Overflow 门控** | tick() overflow 只 force `order_watermark > minute_key` 的分钟 |
| **UpdateTime 语义** | 从 minute_key 派生（北京时间 UTC+8），carry-forward 行也按分钟递增 |
| **Cross-day** | pause → join(30s) → drain → prune locks → resume |
| **Health check** | 检测 writer 死亡 → 自动重启（1次/cross-day），3s drain timeout |
| **4-phase shutdown** | join workers → sentinel+join writer(60s) → drain queue → flush_all_remaining |
| **43 invariants** | N1-N43 全部实施并测试 |

### 实施步骤（全部完成）

- [x] **Task 1**: `writer.py` — Constants, RLock, `_prune_write_locks`
- [x] **Task 2**: `writer.py` — Seek-based tail check
- [x] **Task 3**: `writer.py` — `skip_fsync` parameter
- [x] **Task 4**: `replay.py` — Pass `skip_fsync=False`
- [x] **Tasks 5-6**: Engine queue infrastructure + writer loop + drain
- [x] **Tasks 7-9**: Overflow + cross-day + health check
- [x] **Tasks 10-11**: Start/stop lifecycle + `flush_all_remaining(skip_tickfile)`
- [x] **Tasks 12-13**: Remaining unit + regression + stress tests
- [x] **Task 14**: E2E live test (data_simulator speed=100)

### 文件改动清单

| 文件 | 改动类型 | 改动量 |
|------|---------|--------|
| `src/minute_bar/writer.py` | 修改 | ~50 行 |
| `src/minute_bar/engine.py` | 修改 | ~200 行 |
| `src/minute_bar/flusher.py` | 修改 | ~65 行 |
| `src/minute_bar/tickfile.py` | 修改 | ~10 行 |
| `src/minute_bar/replay.py` | 修改 | ~3 行 |
| `tests/test_tickfile_bg_writer.py` | 新建 | ~700 行 |
| `tests/test_writer.py` | 修改 | ~5 行 |

### 测试结果
- Phase 18 tests: **53 passed**
- Full suite: **363 passed**, 1 pre-existing failure (test_order_drain, unrelated)
- Zero regressions

### E2E Live Test（2026-06-08，data_simulator speed=100）

**验证结果：**
- ✅ Order thread 不被 tickfile IO 阻塞（持续从 0800 推进到 0920+）
- ✅ Tickfile 带正确 order 数据（18 分钟 85,548 行，67,586 行 Volume>0）
- ✅ Seqno 单调递增（1→19）
- ✅ UpdateTime 按分钟递增（carry-forward 行也正确）
- ✅ LocalTime 保持原始交易所时间
- ✅ 无 "0 orders" 错误生成

### E2E 发现并修复的 Bug（2 个）

| Bug | 根因 | 修复 |
|-----|------|------|
| Overflow 生成 0 orders | tick() overflow 未检查 `order_watermark` | 只 force `order_watermark > mk` 的分钟，新增 `eligible_keys` |
| UpdateTime 不递增 | carry-forward 行用旧 `rcvtime` | UpdateTime 从 `minute_key` 派生，LocalTime 保持不变 |

**已知限制（非 Bug）：**
- Order 处理速度受数据量影响：分钟 0900 有 685,145 records（日股开盘 rush），处理需 ~3.7 分钟
- 这是纯 CPU+IO 瓶颈，非 tickfile 阻塞导致

## Phase 19: E2E Fix — P0 Tickfile 缺失 + P1 Order 丢失 `[complete]`

### 设计文档
- **Spec**: `docs/superpowers/specs/2026-06-08-e2e-fix-design.md`（v5 final，5 轮审阅，7 Fix + 9 Implementation Notes）
- **实施方式**: Subagent-Driven Development（10 tasks）
- **Review**: 3-agent spec review 全部 ✅ 合规

### 问题概述

E2E 全天 Live 测试（data_simulator speed=100, date=20260528）发现：
- **P0**: Tickfile 缺失 24 分钟（329 有 order+snapshot 的分钟中仅 305 生成 tickfile）— `_reroute_buffer_to_late_queue` race condition
- **P1**: Order 丢失 13.4%（源 87.5M → 输出 75.8M）— `MAX_LATE_ORDER_RECORDS_PER_MINUTE=50000` 上限不足
- **P2**: Snapshot late queue 溢出（1 条丢失）— 待 Phase 20

### 修复清单（7 个 Fix）

| Fix | 文件 | 改动行数 | 说明 |
|-----|------|----------|------|
| **Fix-A** ⭐ | `flusher.py` | ~6 行 | 移除 `_reroute_buffer_to_late_queue` 中的 `_tickfile_pending.pop()`（核心 P0 修复） |
| **Fix-B** | `flusher.py` | ~15 行 | `_try_generate_tickfile` per-minute-key WARNING 日志 + `_tickfile_skip_warned_keys` |
| **Fix-C** | `engine.py` | ~50 行 | `stop()` 3-layer shutdown 完整性检查（pending/queue/内存比对） |
| **Fix-D** | `config.py` + `engine.py` + 7 ini | ~20 行 | `max_late_order_records_per_minute` 可配置（50K → 1M） |
| **Fix-E** | `engine.py` | ~20 行 | late order drop 计数 + per-drain 日志 + shutdown 汇总 |
| **Fix-F** | `aggregator.py` + `flusher.py` | ~20 行 | `_generated_tickfile_minutes` 去重 guard 防止重复写入 |
| **Fix-G** | `flusher.py` | ~8 行 | cross-day force-generation 失败 CRITICAL 日志 |

**总计**: ~139 行改动，4 个源文件 + 7 个 config ini

### 核心机制

#### Fix-A: reroute 不再丢弃 tickfile pending（INV-TF1）
```
Before: _reroute_buffer_to_late_queue pop both raw_order_buffers + _tickfile_pending
After:  _reroute_buffer_to_late_queue only pops raw_order_buffers, preserves _tickfile_pending

INV-TF1: _tickfile_pending[k] 的唯一合法移除路径:
  1. _try_generate_tickfile() → pop(k) — 正常消费
  2. _step1_cross_day_check() → .clear() — 跨日清空
  禁止路径: _reroute_buffer_to_late_queue() 不得 pop _tickfile_pending
```

#### Fix-F: `_generated_tickfile_minutes` 去重 guard
- `_try_generate_tickfile` 在 lock 内检查去重 → pop pending + seqno（同一 lock block）
- `.add(minute_key)` 在 write 成功后（Implementation Note #1）
- `not selected` 也标记为 generated（Implementation Note #6）
- IO error re-insert 检查 `_generated_tickfile_minutes`（防部分写入后重复）
- cross-day 时 `.clear()`（Implementation Note #5）

#### Fix-C: 3-layer shutdown check
```
Layer 1: _tickfile_pending 仍有 entries → WARNING
Layer 2: tickfile queue depth > 0 → WARNING
Layer 3: _generated_tickfile_minutes vs flushed_snapshot_minutes & flushed_order_minutes → MISSING/EXTRA/PASS
```

#### Fix-D: 可配置 late order cap
```python
# config.py RecoveryConfig:
max_late_order_records_per_minute: int = 1000000  # was hardcoded 50000
# 7 ini files: [recovery] max_late_order_records_per_minute = 1000000
```

### 9 条 Implementation Notes（全部验证通过）

| # | 内容 | 验证 |
|---|------|------|
| 1 | `.add()` 在 write 之后，不在 write 之前 | ✅ Agent 2 verified |
| 2 | Guard + pop + seqno 在同一 lock block | ✅ Agent 2 verified |
| 3 | `_tickfile_skip_warned_keys` 实例级（非 class 级） | ✅ Agent 1 verified |
| 4 | `_tickfile_skip_warned_keys.clear()` 在 cross-day | ✅ Agent 1 verified |
| 5 | `_generated_tickfile_minutes.clear()` 在 force-generate 之后 | ✅ Agent 1+2 verified |
| 6 | `not selected` 也标记为 generated | ✅ Agent 2 verified |
| 7 | `total_late_dropped` 无下划线前缀 | ✅ Agent 2 verified |
| 8 | `assert hasattr` for `flushed_order_minutes` | ✅ Agent 2 verified |
| 9 | Layer 3 用 `snaps & orders` 交集 | ✅ Agent 2 verified |

### 文件改动清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `src/minute_bar/config.py` | 修改 | RecoveryConfig 新增 `max_late_order_records_per_minute` + load_config parser |
| `src/minute_bar/aggregator.py` | 修改 | SharedState 新增 `_generated_tickfile_minutes` + `flushed_order_minutes` |
| `src/minute_bar/flusher.py` | 修改 | Fix-A（reroute 不 pop pending）+ Fix-B（skip logging）+ Fix-F（dedup guard）+ Fix-G（cross-day log/clear） |
| `src/minute_bar/engine.py` | 修改 | Fix-C（3-layer shutdown）+ Fix-D（configurable cap）+ Fix-E（drop logging）+ Step 4b（flushed_order_minutes） |
| `config/production.ini` | 修改 | `[recovery]` 新增 `max_late_order_records_per_minute` + full comment |
| `config/config.ini.example` | 修改 | 同上 |
| `config/test-*.ini` (5 files) | 修改 | `[recovery]` 新增 `max_late_order_records_per_minute` + short comment |
| `tests/test_tickfile_sync.py` | 修改 | 2 更新 + 12 新增测试（含 4 spec review 补充） |
| `tests/test_tickfile_bg_writer.py` | 修改 | 1 更新测试（reroute assertion flipped） |
| `tests/test_e2e_tickfile_completeness.py` | 新建 | E2E 自动化：tickfile 完整性 + order 完整性 |

### 测试结果
- **375 passed, 0 failed**（排除 pre-existing `test_order_drain` timing issue）
- E2E test file 已创建（需实际 E2E 数据才能运行）
- 3-agent spec review：Fix-A/B/G ✅, Fix-F/C/D/E ✅, Tests+Config ✅

### 修改文件
| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `src/minute_bar/config.py` | 修改 | RecoveryConfig 新增字段 + load_config parser |
| `src/minute_bar/aggregator.py` | 修改 | SharedState 新增 2 个 set 字段 |
| `src/minute_bar/flusher.py` | 修改 | Fix-A/B/F/G 共 4 处改动 |
| `src/minute_bar/engine.py` | 修改 | Fix-C/D/E + Step 4b 共 5 处改动 |
| `config/*.ini` (7 files) | 修改 | 新增 `max_late_order_records_per_minute` |
| `tests/test_tickfile_sync.py` | 修改 | 2 更新 + 12 新增 |
| `tests/test_tickfile_bg_writer.py` | 修改 | 1 更新 |
| `tests/test_e2e_tickfile_completeness.py` | 新建 | 2 个 E2E 自动化测试 |

### 上线前 Checklist
- [x] Fix-A 应用后 `_tickfile_pending` 仅被 `_try_generate_tickfile` pop
- [x] Fix-F `_generated_tickfile_minutes` 去重 guard 正常工作（`.add()` 在 write 后）
- [x] 2 个更新测试 + 15 个新测试通过
- [ ] E2E: tickfile 分钟数 == 有 order+snapshot 的分钟数（需 E2E 数据）
- [ ] E2E: order 输出记录数 == 源记录数（需 E2E 数据）
- [ ] shutdown 日志无 CHECK 3 FAIL
- [ ] `_tickfile_queue_skip_count` 合理
- [ ] 无 `Order late cap` WARNING（1M cap 足够覆盖 100x 测试）
- [ ] cross-day 无 CRITICAL 日志
- [x] `production.ini` 添加 `max_late_order_records_per_minute` 并标注内存风险

### 下轮计划（Phase 20+）

| 问题 | 计划 |
|------|------|
| Order 线程 late record 概念消除 | watermark-based flush 架构改造 |
| P2 snapshot late queue 溢出 | Phase 20 处理 |
| Replay vs Live tickfile 路径统一 | 统一 tickfile 生成路径 |
| Runtime tickfile 缺失检测 | runtime stale detection |

## Phase 20: Rust Order Acceleration `[complete]`

### 设计文档
- **Spec**: `docs/superpowers/specs/2026-06-10-rust-order-accel-design.md`（1686 行，15 轮 45+ agent review，0 Critical/Major）

### 概述
Order thread peak-minute performance（0900: 747K records）takes 147s > 60s requirement. Root cause: Python GIL contention. Solution: Rust/PyO3 native extension with GIL release during batch parsing.

### Phase 0: Prerequisites `[complete]`

- [x] **Fix `pyproject.toml`**: `build-backend` from non-existent `setuptools.backends._legacy:_Backend` to `setuptools.build_meta:__legacy__`（setuptools 82+ verified broken）
- [x] **Audit production data for empty symbols**: 87M lines, 0 empty symbols found — fix is safe
- [x] **Fix `parse_order_record` empty symbol check**: Add `if not fields[0].strip(): return None`（pre-existing bug: hot path accepts empty symbols, slow path rejects）
- [x] **Add `enable_order_accel` to InputConfig**: Default `False`, in `[input]` section（NOT `[output]`）
- [x] **Add startup log + fail-fast**: `Engine.__init__` end — 4 states（ENABLED / disabled-by-config / fail-fast RuntimeError / not-available）

### Phase 1: Rust Extension `[complete]`

- [x] Create `order_accel/`（`Cargo.toml` + `src/lib.rs` + `rust-toolchain.toml` + `.gitignore`）
- [x] API: `parse_order_batch(lines, encoding)` + `parse_order_batch_flat(lines, encoding)` + `is_available()`
- [x] 17 Rust unit tests — all pass
- [x] `setup.py` with `setuptools_rust.RustExtension`（graceful skip when not installed）
- [x] Build + verify import: `from minute_bar._order_accel import is_available; is_available()` → True
- [x] `csv_parser.py` Rust import fallback + `use_rust_accel()` + `decode_flat_batch()` + `set_rust_available()`

### Phase 2: Python Integration `[complete]`

- [x] **`_process_parsed_record`**（shared per-record function）:
  - Record-scoped: cross-day flush, late-order detection, buffer append, watermark update
  - Batch-scoped (stays OUTSIDE): `pending_shared_orders` state-lock, `drain_count`, periodic flush
- [x] **Rust/Python branch in `_order_loop`**:
  - Rust path: `encoding.lower() in ("utf-8", "utf8")` guard + `today_int` int comparison + `try/except` fallback
  - Python fallback: EXACT original code — `str(record.time)[:8]` string comparison
  - `today_int` guard: `RuntimeError` if `_get_target_date()` returns invalid value
- [x] **10 Python integration tests**: byte-identical CSV（10K+ records）, field parity, seqno after date check, CRLF, empty symbol, config guard
- [x] **DO NOT change `write_order_file`** — keep streaming write（spec Section 5.3）

### Phase 3: Validation `[complete]`

- [x] PyO3 microbenchmark（747K records, **release build**）:
  - Tuple return: **1.22s**（selected as primary）
  - Flat binary: 1.79s（backup）
  - Rust parse only（GIL-free）: 0.98s
  - **Debug build: 6.7s — NOT for production**
- [x] Full regression: 389 passed（1 pre-existing time-flaky test）

### Key Design Decisions (post-review, implementation-confirmed)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Tuple return as primary | Tuple（1.22s）> Flat（1.79s）in release | PyO3 optimized in release builds; flat binary overhead from Python decode |
| **Release build required** | Debug 6.7s → Release 1.22s（5.5x faster） | Debug is unoptimized; production MUST use release |
| `LATE_CACHE_MARKER` → `"__LATE__"` string | `_process_parsed_record` can't access caller's `object()` | String sentinel works across method boundaries |
| `today_int`（Rust）vs `str[:8]`（Python fallback） | Int comparison in Rust, exact original string in Python | Rust has 17-digit guard; Python preserves original semantics |
| `time_to_minute_key = str(time // 100_000)` | Verified equal to `str(time)[:12]` for 17-digit timestamps | Eliminates intermediate 17-char string allocation |
| `encoding.lower()` guard | ConfigParser doesn't lowercase values | "UTF-8" must still match |
| `PYO3_PYTHON` env var | Anaconda `Scripts/python.exe` doesn't exist | Must set `PYO3_PYTHON` to actual python.exe path |
| `gc.disable()` during batch loop | Amortizes ~10K gen0 scans | Process-wide effect; `try/finally` ensures re-enablement |

### File Changes

| File | Type | Description |
|------|------|-------------|
| `pyproject.toml` | Modified | Fix build-backend |
| `setup.py` | New | setuptools-rust RustExtension |
| `order_accel/Cargo.toml` | New | pyo3 0.23, panic=abort |
| `order_accel/src/lib.rs` | New | ~200 lines, 17 tests + flat binary |
| `order_accel/rust-toolchain.toml` | New | 1.84 |
| `order_accel/.gitignore` | New | target/, *.pyd, *.so |
| `src/minute_bar/csv_parser.py` | Modified | Empty symbol fix + Rust import + flat decoder |
| `src/minute_bar/config.py` | Modified | `enable_order_accel` in InputConfig |
| `src/minute_bar/engine.py` | Modified | `_process_parsed_record` + Rust/Python branch + startup log |
| `config/production.ini` | Modified | `enable_order_accel = false` |
| `tests/test_order_accel.py` | New | 10 integration tests |

### Pending（Phase 4）

- [ ] Engine-level integration test
- [ ] Corrupted .pyd rollback test
- [ ] Concurrent benchmark（order + snapshot threads, sustained load）
- [ ] E2E performance test（speed=100 tickfile live）
- [ ] Warmup self-test in `Engine.__init__`
- [ ] `enable_order_accel = true` in production INI

---

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| decimal 除法错误 | 1 | 除以 decimal 值而非 10**decimal，已修正 |
| code.csv 测试列数不足 | 1 | 测试提供 4 列但最少需 7 列，已修正 |
| CodeTable.load 只读一个 chunk | 1 | `read_lines()` 是生成器只读一个 chunk，改为 while 循环读完 |
| Replay 输出全天最终状态 | 1 | 先读完全天再输出导致 snapshot 全是 15:30 数据，改为逐分钟处理 |
| Replay 产生非目标日期输出 | 1 | snapshot.csv 开头包含上一交易日收盘数据，按日期过滤跳过 |
| CSV header 被当数据解析 | 1 | 检测 `symbol,` 开头行静默跳过，不再打 ERROR 日志 |
| SnapshotRecord 做 decimal 除法 | 1 | 用户要求保留原始值不做处理，改为 raw value + OHLCVAggregate 内部除 |
| Snapshot 输出缺少列 | 1 | 输出应保留全部原始列（含 lasttradeprice/sessionid/shortsellflag/rcvtime） |
| Snapshot 每分钟只保留最后一条 | 1 | 同一分钟内同一 symbol 可能有多次更新，改为保留全部记录 |
| Tickfile bid/ask 全 NA | 1 | `flusher._flush_minutes_internal` 调用 `_write_minute_files` 缺少 `latest_order_copy` 参数，已补传 |
| raw_order_buffers 内存泄漏 | 1 | order 落后 snapshot 时已 flush 分钟数据不被释放，写入时检查 `flushed_snapshot_minutes` 跳过 |
| code.csv 占位行覆盖 decimal | 1 | `_merge_symbol` 保留已有非零 limitup/limitdown/decimal，占位行 0 不覆盖 |
| Tickfile overflow 生成 0 orders | 1 | `tick()` overflow 未检查 `order_watermark`，新增 `eligible_keys` 过滤只 force order 已过的分钟 |
| Tickfile UpdateTime 不递增 | 1 | carry-forward 行用旧 `rcvtime`，UpdateTime 改从 `minute_key` 派生，LocalTime 保持不变 |

---

## 运行方式

### 实时模式
```bash
PYTHONPATH=src python main.py --config config/config.ini
```

### Replay 模式（离线回放历史数据）
```bash
PYTHONPATH=src python main.py --config config/config.ini --replay 20260519
```

### 数据模拟器（Live 端到端测试）
```bash
# 基础回放（100倍速，原始顺序）
PYTHONPATH=src python -m data_simulator --speed 100

# 最真实测试：保留乱序 + 半行写入 + late record
PYTHONPATH=src python -m data_simulator --speed 100 --split-line-prob 0.01 --late-prob 0.001

# 另一个终端启动 minute_bar live 模式
PYTHONPATH=src python main.py --config config/config.ini
```
