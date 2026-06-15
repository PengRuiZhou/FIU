# FIU Minute Bar Generator

日股分钟级行情数据生成器。读取 FIU 接收服务实时写入的 snapshot/order/code CSV 文件，按分钟生成全市场行情快照和 order 分钟文件。

## 功能

- **Live 模式**：多线程实时处理，数据驱动 watermark flush
- **Replay 模式**：离线回放历史数据，流式处理（峰值内存 ~120MB）
- **数据完整性**：迟到记录 append、per-minute snapshot、writer 时间过滤、double-flush 防护
- **异常安全**：stall-triggered flush、checkpoint 断点恢复、跨日自动重置

## 项目结构

```
src/
├── minute_bar/           # 主程序（15 个模块）
│   ├── engine.py         # Live 多线程引擎（drain loop + 独立 order 线程）
│   ├── replay.py         # Replay 离线引擎（流式 + 并行）
│   ├── flusher.py        # Watermark flush + stall 检测 + double-flush 防护
│   ├── aggregator.py     # SharedState + per-minute snapshot
│   ├── writer.py         # 原子写入 + streaming write + carry-forward 时间过滤
│   ├── clock.py          # 时间函数 + data-driven watermark
│   ├── config.py         # 配置解析
│   ├── csv_parser.py     # CSV 解析（snapshot/order/code）
│   ├── file_tailer.py    # 增量文件读取
│   ├── checkpoint.py     # 断点管理
│   ├── code_table.py     # 码表管理
│   ├── models.py         # 数据结构
│   └── validator.py      # 数据校验
└── data_simulator/       # 测试工具（加速回放 + late record 注入）
config/                   # 配置文件
tests/                    # 207 个测试
deploy/                   # 生产部署脚本
```

## 输出文件

每分钟生成 2 个文件（snapshot + order），按日期归档：

```
output/
├── snapshot/2026/20260522/snapshot_minute_20260522_0930.csv
└── order/2026/20260522/order_minute_20260522_0930.csv
```

**Snapshot 文件**：每个 symbol 一行，包含 `update_flag`（Y=本分钟有交易 / N=carry-forward）

**Order 文件**：保留该分钟内所有 order 记录

## 快速开始

### 依赖

Python >= 3.8，无需第三方库。

### Live 模式

```bash
PYTHONPATH=src python main.py --config config/production.ini
```

### Replay 模式

```bash
PYTHONPATH=src python main.py --config config/production.ini --replay 20260522
```

### 测试

```bash
python -m pytest tests/ -v
```

### 数据模拟器（端到端测试）

```bash
# 终端 1：启动 minute_bar
PYTHONPATH=src python main.py --config config/test-order-live.ini

# 终端 2：启动模拟器（100 倍速）
PYTHONPATH=src python -m data_simulator --speed 100 --file-types order,snapshot,code \
    --date 20260525 --source-dir input --output-dir test/output
```

## 生产部署

```bash
cd /path/to/fiu_minute_bar && bash deploy/setup.sh
bash deploy/start.sh
bash deploy/stop.sh    # SIGTERM 优雅关闭，flush 最后分钟数据
```

详见 [deploy/](deploy/) 和 [config/production.ini](config/production.ini)。

### 核心配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `data_flush_delay_minutes` | 1 | 数据推进后延迟几分钟 flush |
| `enable_time_fallback` | true | 真实时钟兜底（data-driven 为主，fallback 兜底） |
| `stall_flush_sec` | 300 | Watermark 停滞多少秒后触发 flush |
| `code_refresh_sec` | 30 | Code table 刷新间隔 |
| `order_chunk_size_bytes` | 65536 | Order 文件读取 chunk 大小（生产建议 524288） |

## 架构

```
┌─ data-thread ──────┐   ┌─ clock-thread ──────┐   ┌─ order-thread ─────┐
│ FileTailer (drain)  │   │ Flusher.tick()       │   │ FileTailer (drain)  │
│ → parse             │   │  step1 cross-day     │   │ → parse             │
│ → validate          │   │  step3 minute output │   │ → late detect       │
│ → aggregate         │   │    (normal + reflush) │   │ → buffer            │
│ → SharedState       │   │  step4 late records   │   │ → record-driven     │
│                     │   │  step5 checkpoint     │   │   / watermark flush │
└───────┬─────────────┘   └────────┬─────────────┘   └────────┬────────────┘
        │                          │                           │
        └──────────────────────────┼───────────────────────────┘
                                   ▼
                             SharedState
                    (RLock + latest_snapshot
                     + ohlcv_buffers
                     + per-minute snapshot)
```

- **Data-driven watermark**：flush 时机由 `current_minute`（数据进度）决定，非真实时钟
- **Per-minute snapshot**：分钟推进前捕获快照，carry-forward 不含未来数据
- **Stall-triggered flush**：watermark 停滞超过 `stall_flush_sec` 自动 flush 残余 buffer
- **Double-flush 防护**：已 flush 分钟的竞态窗口数据路由到 late queue（append 而非覆盖）
- **Drain loop**：data/order 线程有数据时连续读取，无数据时走配置间隔
- **Order 独立线程**：独立 FileTailer + 本地 buffer + 独立 stall 检测 + streaming write

## 测试

207 个测试覆盖全部模块，包括：

- 数据驱动 watermark flush（触发/不触发/fallback/跨日）
- Per-minute snapshot 捕获（时序正确性/边界情况）
- Carry-forward 时间过滤（未来数据跳过/合法数据保留）
- Stall-triggered flush（ohlcv + order）
- Double-flush 防护（re-route to late queue + shutdown late record 处理）
- 迟到记录处理（snapshot + order）
- Drain loop 行为（snapshot + order）
- Streaming write（输出一致性/锁/无 tmp 残留）
- 端到端集成测试

## 许可

内部项目
