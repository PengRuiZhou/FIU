# Data Simulator Design

模拟 FIU 数据接收程序的写入行为，用于测试 minute_bar 的 live 模式。

## 目标

从 `input/` 读取历史 CSV 文件（order/snapshot/code），默认按源文件原始顺序以加速模式逐步追加写入到 `test/output/` 目录。minute_bar 的 `--input-dir` 指向 `test/output/`，实现端到端 live 功能测试。

## 架构

```
src/data_simulator/
  __init__.py
  simulator.py    # 核心逻辑：读取源文件，按原始顺序或时间排序，加速追加写入
  __main__.py     # 入口：python -m data_simulator
```

## 数据流

```
input/order.csv.20260522  ──读取──>  simulator  ──追加写入──>  test/output/order.csv.20260522
input/snapshot.csv.XXXX   ──读取──>  simulator  ──追加写入──>  test/output/snapshot.csv.XXXX
input/code.csv.XXXX       ──读取──>  simulator  ──追加写入──>  test/output/code.csv.XXXX
                                                              ↑
                                    minute_bar --input-dir=test/output  ← 读取
```

## 核心流程

1. 扫描 `--source-dir` 下匹配 `{order,snapshot,code}.csv.YYYYMMDD` 的文件
2. code 文件默认 preload：启动时一次性写入目标目录
3. order / snapshot 各自启动独立线程
4. 默认按源文件原始顺序播放（`--order-mode original`），保留真实乱序；可选 `--order-mode time` 按时间戳排序
5. 所有线程共享 global_start_time，每条记录的回放时间基于 `record_time - global_min_time`
6. 可选模拟半行写入（`--split-line-prob`）
7. 可选注入 late records（`--late-prob`）
8. 批量写入 + 定期 flush（`--batch-size` / `--flush-interval-ms`）
9. KeyboardInterrupt 时停止所有线程并 flush 文件句柄

## 时间基准设计

所有线程共享同一个回放起点，保证 order/snapshot/code 之间的相对时间关系真实：

- global_min_time 只从 order / snapshot 的 time 字段（第 2 列）计算
- code preload 不参与时间基准
- code-mode=stream 时使用 code 的 rcvtime 字段；若无 rcvtime 则按固定间隔写入

```python
global_min_time = min(order首行time, snapshot首行time)
real_start = time.monotonic()

# 每条记录
target_elapsed = (record_ts - global_min_time) / speed
actual_elapsed = time.monotonic() - real_start
sleep_time = target_elapsed - actual_elapsed
if sleep_time > 0:
    time.sleep(sleep_time)  # 上限 0.5s
# sleep_time < 0 时立即写入（original 模式下时间倒退是预期行为）
```

## CLI 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--source-dir` | `input/` | 历史数据目录 |
| `--output-dir` | `test/output/` | 模拟写入目标目录 |
| `--speed` | `100` | 加速倍数 |
| `--date` | 自动检测 | 回放日期（YYYYMMDD） |
| `--file-types` | `order,snapshot,code` | 回放哪些文件类型 |
| `--order-mode` | `original` | `original` = 源文件原始顺序（默认），`time` = 按时间戳排序 |
| `--code-mode` | `preload` | `preload` = 启动时一次性写入（默认），`stream` = 按行追加 |
| `--split-line-prob` | `0.0` | 每行被拆成两段写入的概率（0-1） |
| `--split-line-delay-ms` | `50` | 半行写入时两段之间的延迟（毫秒） |
| `--late-prob` | `0.0` | 每行被延迟写入的概率（0-1） |
| `--late-delay-sec` | `10` | late record 延迟秒数（真实秒数，不受 speed 影响） |
| `--batch-size` | `1000` | 累计多少行后 flush |
| `--flush-interval-ms` | `100` | 超过多少毫秒未 flush 则强制 flush |
| `--fsync` | `false` | 是否在 flush 后调用 os.fsync（很慢，按需开启） |
| `--clean` | `true` | 启动前是否清空目标目录中的同名 CSV 文件 |

## 实现细节

- 纯标准库，无外部依赖
- 每个文件类型一个守护线程，主线程等待 KeyboardInterrupt
- 首行立即写入（不等待），后续行按共享时间基准 sleep
- sleep 间隔上限 0.5s / 下限 0s（相邻行时间差为 0 时不 sleep，批量写入）
- 跳过源文件的 header 行（以 `symbol,` 开头的行）
- 目标目录不存在时自动创建
- `--clean` 时只删除匹配 `{order,snapshot,code}.csv.YYYYMMDD` 的文件，打印被删除的文件列表，不递归删除整个目录
- 半行写入：命中 split-line-prob 时先 flush 当前 batch buffer，写前半段（不加换行）并 flush，sleep 指定延迟，写后半段 + "\n" 并 flush。绕过 batch 机制确保半行被真正分开写入
- late record：按概率选中某行后暂存到延迟队列，由独立线程在延迟时间到达后写入。退出时必须 drain 队列中所有待写 records，确保模拟器自身不丢数据
- original 模式下 record_time 可能倒退，sleep_time < 0 时直接写入，不等待，用于保留真实乱序
- 写入后调用 `f.flush()`，`--fsync` 时额外调用 `os.fsync(f.fileno())`
- 停止流程：设置 stop_event → 正常线程停止读取 → late writer drain 已入队 records → flush 所有文件句柄 → 退出

## 使用方式

```bash
# 基础回放（100倍速，原始顺序）
python -m data_simulator --speed 100

# 带半行写入 + late record 注入
python -m data_simulator --speed 100 --split-line-prob 0.01 --late-prob 0.001

# 另一个终端启动 minute_bar live 模式
python -m minute_bar --input-dir test/output --mode live
```
