# Data-Driven Watermark Flush 设计文档

## 概述

将 Live 模式的分钟级 flush 判定机制从**真实时钟驱动**改为**数据进度驱动**，以真实时钟作为 fallback 兜底。

---

## 1. 背景

### 1.1 当前架构

Live 模式使用 3 线程架构：

- **data-thread**：读取 snapshot.csv / code.csv，解析后写入 `SharedState` 的 buffer
- **clock-thread**：每秒 tick，通过 `ClockWatermarkFlusher` 检查并 flush 过期分钟
- **order-thread**：独立读取 order.csv，自主管理 buffer 和 flush

`is_expired()` 是 flush 的核心判定函数，使用真实 JST 时钟：

```python
def is_expired(minute_key: str, delay_sec: int) -> bool:
    end_time = minute_key_to_end_time(minute_key)  # minute_end = minute_key + 1min
    return end_time + timedelta(seconds=delay_sec) <= now_jst()
```

调用点：
1. `flusher.py` `_step3_minute_output()` — snapshot/ohlcv buffer flush
2. `engine.py` `_flush_expired_order_minutes()` — order buffer watermark flush
3. `engine.py` `_enforce_max_pending()` — 内存保护强制 flush（`is_expired` 仅用于日志）

### 1.2 已有基础设施

- `SharedState.current_minute`：data-thread 每处理一条 snapshot 更新，表示数据处理进度
- `_order_loop` 局部变量 `current_minute`：order-thread 的数据处理进度
- Late record 处理机制（Phase 10）：flush 后到达的迟到记录 append 到已有文件
- Checkpoint 安全机制：late record 未落盘前不推进 offset

---

## 2. 问题描述

### 2.1 核心问题：真实时钟与数据进度不匹配

`is_expired()` 用 `now_jst()`（真实时钟）对比 `minute_key`（数据时间）。当两者不同步时，flush 时机错误。

**生产环境问题：**

真实时钟 `output_delay_sec` 是固定值（默认 5s）。当网络延迟波动时：
- 如果网络快于预期：0900 数据在 09:00:03 全部到达，但 flush 等到 09:01:05 才触发 → 无意义延迟
- 如果网络慢于预期：0900 数据仍在传输中（09:01:05 只到了 80%），flush 触发 → 20% 数据走 late record append → 额外 IO

固定时钟延迟无法自适应数据到达速率。

**测试环境问题（speed=100 加速回放）：**

真实时间（如 10:10 JST）远大于数据时间（0800 JST），所有分钟在 buffer 出现的瞬间就被判为过期：

```
T+0.0s  Data thread 读 chunk 1 (~500 symbols of 0800) → buffer[0800] 有 500 symbols
T+1.0s  Clock thread tick:
          is_expired(0800): now_jst(10:10) >> 0800+5s → TRUE → FLUSH!
          → 0800 只包含 500/4449 symbols ← 质量极差
T+1.2s  Data thread 读 chunk 2 (~500 more symbols of 0800)
          → 0800 已在 flushed_snapshot_minutes → LATE RECORD!
...      剩余 3949 symbols 全部变成 late record → 大量无效 IO
```

实测结果（05-22 测试，speed=100）：
- Snapshot 0800 文件只有 ~500 个 symbol（应 4496）
- 持续 ~2500 条/秒 late record flush
- Order 每分钟只输出数百条（应数万条）

### 2.2 问题根因

flush 判定依赖 `now_jst()` 真实时钟，但真实时钟不知道数据进度。正确的语义应该是：**当数据已经推进到下一分钟时，上一分钟就可以 flush 了**。这是一个数据进度信号，不是时间信号。

---

## 3. 设计目标

1. **生产环境正确性**（首要目标）：flush 时机由数据实际到达进度决定，自适应网络延迟波动
2. **测试环境可用性**：支持任意加速倍率的数据回放，无需同步 speed 参数
3. **向后兼容**：真实时钟作为 fallback 兜底，数据线程卡死时仍能安全 flush
4. **最小改动**：不改变 late record 处理、checkpoint 安全、跨日处理等已有机制

---

## 4. 设计方案

### 4.1 核心思路

用 data-thread / order-thread 的 `current_minute`（数据实际处理进度）替代 `now_jst()` 作为 flush 主判定依据。真实时钟仅作 fallback。

```
Clock thread tick:
  with state.lock:                          ← current_minute 读取需 lock 保护
    data_watermark = state.current_minute    ← 数据线程实际处理进度

  # 主逻辑：数据驱动判定（始终启用）
  for minute_key in ohlcv_buffers:
    if is_data_driven_expired(minute_key, data_watermark, delay_minutes):
      flush(minute_key)

  # Fallback：真实时钟兜底（仅 enable_time_fallback=true 时启用）
  if enable_time_fallback:
    for minute_key in ohlcv_buffers:
      if is_expired(minute_key, output_delay_sec):
        flush(minute_key)
```

两个条件是 OR 关系：数据驱动判定始终启用，真实时钟 fallback 由 `enable_time_fallback` 控制是否参与。

### 4.2 新增函数 `is_data_driven_expired`

按 minute_key 的**起始时间**比较，语义直观：

```python
def minute_key_to_start_time(minute_key: str) -> datetime:
    """Convert minute_key "YYYYMMDDHHMM" to the start datetime of that minute.

    同时校验格式和语义有效性（日期、小时 0-23、分钟 0-59）。
    """
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST)

def is_data_driven_expired(minute_key: str, data_watermark: str, delay_minutes: int) -> bool:
    """Check if minute_key should be flushed based on data progress watermark.

    Semantics: watermark 达到 minute_key + delay_minutes 时 flush。
    - delay_minutes=1: watermark 到 0901 时 flush 0900
    - delay_minutes=2: watermark 到 0902 时 flush 0900
    - delay_minutes=0: watermark 到 0900 时就 flush 0900（不建议生产使用）

    Args:
        minute_key: 要检查的分钟，格式 "YYYYMMDDHHMM"
        data_watermark: 数据处理进度，state.current_minute，格式 "YYYYMMDDHHMM"
        delay_minutes: 数据进度需要领先多少分钟后 flush
    """
    if not data_watermark:
        return False
    watermark_dt = minute_key_to_start_time(data_watermark)
    threshold = minute_key_to_start_time(minute_key) + timedelta(minutes=delay_minutes)
    return watermark_dt >= threshold
```

**验证：**

| minute_key | data_watermark | delay_minutes | watermark_dt | threshold | 结果 |
|------------|----------------|---------------|--------------|-----------|------|
| 0900 | 0901 | 1 | 09:01 | 09:00+1=09:01 | True（flush） |
| 0900 | 0900 | 1 | 09:00 | 09:01 | False（同分钟） |
| 0900 | 0905 | 1 | 09:05 | 09:01 | True（跳分钟） |
| 0900 | "" | 1 | - | - | False（无数据） |
| 0900 | 0902 | 2 | 09:02 | 09:00+2=09:02 | True（2分钟窗口） |
| 0900 | 0901 | 2 | 09:01 | 09:02 | False（窗口未满） |
| 0900 | 0900 | 0 | 09:00 | 09:00 | True（立即flush，不推荐） |
| 202605202359 | 202605210000 | 1 | 05-21 00:00 | 05-20 23:59+1=05-21 00:00 | True（跨日） |

> 注：上表中 `0900`、`0901` 等为简写格式，实际代码中使用 12 位 `YYYYMMDDHHMM` 格式（如 `"202605210900"`）。

### 4.3 配置项

`RecoveryConfig` 新增：

```python
data_flush_delay_minutes: int = 1   # 数据驱动 flush 等待窗口（分钟）
enable_time_fallback: bool = True   # 是否启用真实时钟 fallback
```

INI 配置：

生产环境：
```ini
[recovery]
output_delay_sec = 5               # fallback 超时（数据线程卡死时兜底）
data_flush_delay_minutes = 1       # 数据进度领先多少分钟后 flush
enable_time_fallback = true        # 生产环境必须开启 fallback
```

测试环境（data simulator 加速回放）：
```ini
[recovery]
output_delay_sec = 5               # 值不重要，fallback 已关闭
data_flush_delay_minutes = 1
enable_time_fallback = false       # 关闭真实时钟，避免 speed=100 时误触发
```

配置项含义：

`data_flush_delay_minutes`：
- `0`：watermark 到达同一分钟时立即 flush（不建议生产使用）
- `1`（默认）：watermark 推进到下一分钟时 flush 上一分钟
- `2`：watermark 推进 2 分钟后才 flush（更多缓冲，内存占用稍高）

`enable_time_fallback`：
- `true`（默认）：真实时钟 fallback 启用。数据线程卡死时，`is_expired(minute_key, output_delay_sec)` 作为兜底保证分钟不会永远积压在内存中
- `false`：关闭真实时钟 fallback。仅数据驱动判定触发 flush。适用于 data simulator 加速测试，避免真实时钟远超数据时间导致过早 flush

`output_delay_sec` 保持不变，仅在 `enable_time_fallback=true` 时生效。

### 4.4 场景验证

#### 场景 1：生产环境（实时数据，网络延迟正常）

```
09:00:00  data-thread 处理 0900 snapshot 数据，current_minute=0900
09:00:30  clock-thread tick:
            is_data_driven_expired(0900, "0900", 1) → False（还在同一分钟）
            is_expired(0900, 5) → False（时间未到）
09:01:03  data-thread 开始收到 0901 数据，current_minute=0901
09:01:03  clock-thread tick:
            is_data_driven_expired(0900, "0901", 1) → True → FLUSH 0900
            （此时 0900 buffer 已包含绝大部分数据）
09:01:07  少量 0900 迟到记录到达（网络延迟）
            → late record append 到 0900 文件（正常路径）
```

比固定 5s 延迟更准确：flush 时机由数据到达决定，不受时钟偏差影响。

#### 场景 2：生产环境（网络延迟较大）

```
09:00:00  data-thread 处理 0900 数据
09:01:05  is_data_driven_expired(0900, "0900", 1) → False
          is_expired(0900, 5) → True（fallback 触发）→ FLUSH 0900
          （网络慢，数据还没到 0901，但真实时钟超时兜底）
09:01:20  0900 迟到数据到达 → late record append
09:01:30  0901 数据到达，current_minute=0901
          正常处理 0901
```

Fallback 保证数据不会永远积压在内存中。

#### 场景 3：测试环境（speed=100，enable_time_fallback=false）

```
T+0.0s  data-thread 快速读完 0800 块（~1.8s），current_minute=0800
T+1.0s  clock-thread tick:
          is_data_driven_expired(0800, "0800", 1) → False（同分钟）
          enable_time_fallback=false → 跳过真实时钟判定
          → 不 flush，等待数据推进
T+1.8s  data-thread 读完 0800 块，开始读 0830 数据，current_minute=0830
T+2.0s  clock-thread tick:
          is_data_driven_expired(0800, "0830", 1) → True（0830 >= 0800+1）
          → FLUSH 0800（此时包含全部 4449 symbols）
```

关闭 fallback 后，真实时钟不会误触发，flush 完全由数据进度驱动。

#### 场景 4：异常场景（data-thread 卡死，enable_time_fallback=true）

```
09:00:00  data-thread 处理 0900 数据后卡死，current_minute 停在 0900
09:01:05  clock-thread tick:
          is_data_driven_expired(0900, "0900", 1) → False（卡住）
          is_expired(0900, 5) → True → FLUSH 0900（fallback 兜底）
09:01:06  之后所有新分钟只靠 fallback flush
```

安全兜底，不会永远卡住。

#### 场景 5：异常场景（data-thread 卡死，enable_time_fallback=false）

```
09:00:00  data-thread 处理 0900 数据后卡死，current_minute 停在 0900
09:01:05  clock-thread tick:
          is_data_driven_expired(0900, "0900", 1) → False（卡住）
          enable_time_fallback=false → 跳过真实时钟判定
          → 0900 永远不会 flush（需要外部干预重启）
```

**风险**：关闭 fallback 后，数据线程卡死会导致分钟永远不 flush。这是测试环境的可接受折衷——测试人员可以观察到数据停止输出并重启。生产环境必须开启 fallback。

#### 场景 6：数据流结束 — 最后一分钟无法被数据驱动 flush（关键）

```
交易日最后一分钟 1530，所有数据到达后 current_minute 停在 "202605211530"。
没有 1531 的数据来推进 watermark。

is_data_driven_expired("202605211530", "202605211530", 1)
→ watermark=15:30, threshold=15:31
→ 15:30 >= 15:31 → False → 不 flush
```

**生产环境（enable_time_fallback=true）不受影响**：真实时钟在 15:31:05 触发 fallback flush。

**测试环境（enable_time_fallback=false）会丢失最后一分钟**：数据驱动无法触发，无真实时钟兜底。

**同样的问题出现在**：程序在最后一分钟后很快被 stop、手动停止服务时 buffer 中仍有数据、回放测试中数据文件结束但 live engine 没有真实下一分钟。

**解决方案**：`Engine.stop()` 必须执行 final flush，将所有残留 snapshot/kline/order buffers 无条件写出（见 7.10）。

#### 场景 7：同一分钟内数据分多批到达，watermark 不变

```
09:00:00  data-thread 处理 chunk1 of 0900 (symbols 1-1500), current_minute="...0900"
09:00:05  data-thread 处理 chunk2 of 0900 (symbols 1501-3000), current_minute="...0900"（不变）
09:00:10  data-thread 处理 chunk3 of 0900 (symbols 3001-4449), current_minute="...0900"（不变）
09:00:11  clock-thread tick:
            is_data_driven_expired("0900", "0900", 1) → False（同分钟）
            → 不 flush，等待 0901 数据到达
```

data-thread 在同一分钟内多次调用 `process_snapshot`，每次都写入不同 symbol 的数据，但 `current_minute` 保持 `"...0900"` 不变（单调递增约束：`"0900" > "0900"` 为 False）。clock-thread 不会过早触发 flush，等待 0901 数据到达后才 flush 0900 的全部数据。

#### 场景 8：跳分钟 + 迟到数据到达

```
data 文件中 0900 之后直接是 0905（0901-0904 无数据）。

T+0.0s  data-thread 处理 0900 块, current_minute="...0900"
T+1.0s  clock-thread tick:
          is_data_driven_expired("0900", "0900", 1) → False
          → 不 flush
T+2.0s  data-thread 处理 0905 块, current_minute="...0905"
T+3.0s  clock-thread tick:
          is_data_driven_expired("0900", "0905", 1) → True（0905 >= 0900+1=0901）
          → FLUSH 0900（包含全部数据）
          0901-0904 从未在 buffer 中出现 → 不生成空文件 ✓
```

迟到数据到达（0905 数据之后才收到 0902 迟到 record）：

```
T+3.0s  0900 已 flush，0901-0904 从未有 buffer
T+4.0s  data-thread 收到 0902 迟到 record
          → minute_key="0902" 不在 flushed_snapshot_minutes 中（0902 从未 flush 过）
          → 创建 buffer["0902"]，写入数据
          → current_minute 单调约束："0902" < "0905" → watermark 不变
T+5.0s  clock-thread tick:
          is_data_driven_expired("0902", "0905", 1) → True（0905 >= 0903）
          → FLUSH 0902（仅包含迟到的那部分数据）
```

**预期行为**：跳分钟的迟到数据会创建新 buffer 并在下一 tick 被数据驱动 flush。输出文件只包含迟到的数据，质量较低，但保证数据不丢。这是预期行为而非 bug。

**Order 侧对应行为**：

```
T+4.0s  order-thread 收到 0902 迟到 order record
          → minute_key="0902" 不在 _flushed_order_minutes 中（0902 从未被 flush）
          → 创建 buffers["0902"]，写入 record
          → current_minute="0905"，"0902" < "0905" → watermark 不变
T+5.0s  _flush_expired_order_minutes 调用：
          is_data_driven_expired("0902", "0905", 1) → True
          → FLUSH 0902 order buffer（仅包含迟到的 order）
```

Order 侧行为与 snapshot 侧一致：迟到数据创建新 buffer，由 watermark-driven flush 写出。在本场景中，0902 从未出现过 buffer，不在 `_flushed_order_minutes` 中，因此必定走创建新 buffer 路径。一般性规则：若迟到 record 所属分钟已被 flush（在 `_flushed_order_minutes` 中），则走 late append 路径；若从未 flush，则作为新 buffer 等待后续 flush。

#### 场景 9：数据线程暂停后恢复，watermark 跳跃

```
09:00:00  data-thread 正常运行, current_minute = "...0905"
09:00:30  data-thread 因网络问题暂停 2 分钟
          buffer 中有 0905, 0906（data-thread 暂停前的残留）
          clock-thread tick:
            is_data_driven_expired("0905", "0905", 1) → False（同分钟）
            is_expired("0905", 5) → False / True（取决于真实时钟）
            → 如果 fallback=true，可能在 09:06:05 由真实时钟 flush 0905
            → 如果 fallback=false，0905 等待 watermark 推进
09:02:30  data-thread 恢复，快速读到 0910, current_minute 跳到 "...0910"
09:02:31  clock-thread tick:
          is_data_driven_expired("0905", "0910", 1) → True → FLUSH 0905
          is_data_driven_expired("0906", "0910", 1) → True → FLUSH 0906
          （如果 fallback 已 flush 过 0905，buffer 已被 pop，不存在，不会重复 flush）
          0907-0909 从未有 buffer → 不生成空文件 ✓
```

**预期行为**：watermark 跳跃导致多个积压分钟同时 flush，短时间内 IO 增加。如果 `enable_time_fallback=true`，部分分钟可能已被真实时钟 flush，pop 时 buffer 不存在（跳过），不会重复 flush。这是因为 `expired_keys` 来自 `ohlcv_buffers.keys()` 的当前遍历，已被 pop 的 key 不在遍历范围内，天然去重。同一 tick 内 OR 条件筛选也只产生去重后的列表，不存在同一分钟被 flush 两次的风险。这是正确的恢复行为——积压数据被快速写出，不影响后续数据流。

---

## 5. 改动清单

### 5.1 `clock.py`

新增 `minute_key_to_start_time()` 和 `is_data_driven_expired()` 函数：

```python
def minute_key_to_start_time(minute_key: str) -> datetime:
    """Convert minute_key "YYYYMMDDHHMM" to the start datetime of that minute.

    同时校验格式和语义有效性（日期、小时 0-23、分钟 0-59）。
    """
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST)

def is_data_driven_expired(minute_key: str, data_watermark: str, delay_minutes: int) -> bool:
    if not data_watermark:
        return False
    watermark_dt = minute_key_to_start_time(data_watermark)
    threshold = minute_key_to_start_time(minute_key) + timedelta(minutes=delay_minutes)
    return watermark_dt >= threshold
```

不修改现有 `is_expired()`，保留作为 fallback。

**性能优化建议**：`is_data_driven_expired` 每次调用对 `data_watermark` 执行一次 `minute_key_to_start_time` 转换。当 `_step3_minute_output` 对多个 buffer key 逐个调用时，watermark 转换重复执行。可在调用方缓存 `watermark_dt`，传入批量判定函数。当前实现优先保持 API 简洁性（纯函数、无状态），性能影响可忽略（单次 tick 通常 flush 少量分钟）。如果未来 buffer 规模显著增大，可重构为批量接口。

### 5.2 `config.py`

`RecoveryConfig` 新增字段：

```python
@dataclass
class RecoveryConfig:
    checkpoint_file: str = "checkpoint.json"
    output_delay_sec: int = 5
    code_refresh_sec: int = 30
    data_flush_delay_minutes: int = 1    # 新增
    enable_time_fallback: bool = True    # 新增
```

**参数校验**：`data_flush_delay_minutes` 必须为非负整数。负值会导致 threshold 早于 minute_key 起始时间，使 flush 条件恒真（提前 flush 正在写入的分钟）。在 `Engine.__init__` 中校验：

```python
if config.recovery.data_flush_delay_minutes < 0:
    raise ValueError(
        f"data_flush_delay_minutes must be >= 0, got {config.recovery.data_flush_delay_minutes}"
    )
if config.recovery.data_flush_delay_minutes > 10:
    logger.warning(
        "data_flush_delay_minutes=%d is unusually large; minutes may accumulate in buffer for extended time",
        config.recovery.data_flush_delay_minutes,
    )
```

`data_flush_delay_minutes=0` 合法但不建议生产使用（数据到达下一分钟即立即 flush，无缓冲窗口），仅用于测试。`enable_time_fallback` 不支持运行时变更，修改需重启进程。

`load_config` 新增读取：

```python
cfg.recovery.data_flush_delay_minutes = s.getint("data_flush_delay_minutes", cfg.recovery.data_flush_delay_minutes)
cfg.recovery.enable_time_fallback = s.getboolean("enable_time_fallback", cfg.recovery.enable_time_fallback)
```

### 5.3 `flusher.py`

#### 构造函数

新增 `data_flush_delay_minutes` 和 `enable_time_fallback` 参数：

```python
class ClockWatermarkFlusher:
    def __init__(self, ..., data_flush_delay_minutes: int = 1, enable_time_fallback: bool = True):
        ...
        self._data_flush_delay_minutes = data_flush_delay_minutes
        self._enable_time_fallback = enable_time_fallback
```

#### `_step3_minute_output`

核心逻辑委托给 `_flush_minutes_internal`（见 5.4 节），此处只负责过期判定和触发来源日志分类。完整伪代码见 Section 5.4。

`data_watermark` 在 lock 内读取，与 buffer 状态原子一致。`enable_time_fallback=false` 时，真实时钟判定完全跳过。实际的 buffer pop、snapshot copy、文件写入、状态更新由 `_flush_minutes_internal` 完成（lock 内 pop + lock 外 IO + lock 内更新状态），保证 flush 期间 data-thread 不受阻塞。

**Lock 释放窗口安全性**：`_step3_minute_output` 在 lock 内筛选 `expired_keys` 后释放 lock，再调用 `_flush_minutes_internal`。此窗口内 data-thread 可能向 buffer 添加新数据，但不会删除已有 key。`_flush_minutes_internal` 使用 `pop(k, None)` 防御性弹出，不存在 double-pop 风险。窗口内新增的数据会被本次 `pop` 一并取出——这有利于数据完整性（flush 更多数据而非更少），不会丢数据。`ohlcv_data` 使用 `.get(minute_key)` 防御性取值，跳过 pop 时被跳过的 key，不存在 KeyError 风险。

#### `_step1_cross_day_check` 跨日重置清空 watermark

跨日重置块（当前代码 `flusher.py` 第 109-118 行）中，增加 `current_minute` 清空：

```python
with self._state.lock:
    self._state.output_minutes.clear()
    self._state.last_totalvol_by_symbol.clear()
    self._state.last_totalamount_by_symbol.clear()
    self._state.last_output_date = current_date
    self._state.last_output_minute = ""
    self._state.first_data_received = False
    self._state.flushed_snapshot_minutes.clear()
    self._state.late_snapshot_count = 0
    self._state.late_snapshot_minutes.clear()
    # 条件赋值：仅在 watermark 低于零点基准时才设置，避免覆盖 data-thread 已推进的新日期值
    new_day_base = current_date + "0000"
    if not self._state.current_minute or self._state.current_minute < new_day_base:
        self._state.current_minute = new_day_base
```

**理由**：跨日重置执行时，`current_date` 已是新日期（从被 data-thread 更新后的 `current_minute` 中提取）。将 watermark 重置为新日期安全基准值，需使用条件赋值避免覆盖 data-thread 可能已设置的更新值（如 `"202605220900"`）。条件 `current_minute < new_day_base` 保证：如果 watermark 已被 data-thread 推进到 `"202605220900"`（大于 `"202605220000"`），则不覆盖。零点基准值不会误触发 flush（`"202605220000" < "202605220900"`），同时避免了空水印导致 `is_data_driven_expired` 返回 False 的问题（尤其在 `enable_time_fallback=false` 的测试环境中）。

**跨日 flush 为什么不改用 `_flush_minutes_internal`**：跨日 flush（当前代码 `flusher.py` 第 79-93 行）使用 `_write_minute_files` 直接写入旧日期的残留 buffer，然后跨日重置块清空 `output_minutes`、`flushed_snapshot_minutes` 等按天跟踪的状态集合。如果改用 `_flush_minutes_internal`，它会向刚被清空的集合中 `add` 新条目，与跨日重置的语义矛盾。此外，跨日 flush 发生在 `_step1` 中，此时 clock-thread 正持有执行权，不涉及跨线程竞态。保持直接写入方式是正确且简洁的设计。

### 5.4 `engine.py`

#### 构造函数

传入 `data_flush_delay_minutes` 和 `enable_time_fallback`：

```python
self._flusher = ClockWatermarkFlusher(
    ...,
    data_flush_delay_minutes=config.recovery.data_flush_delay_minutes,
    enable_time_fallback=config.recovery.enable_time_fallback,
)
self._data_flush_delay_minutes = config.recovery.data_flush_delay_minutes
self._enable_time_fallback = config.recovery.enable_time_fallback
```

启动时校验配置并输出告警（`Engine.__init__` 末尾或 `start` 方法开头）：

```python
if not config.recovery.enable_time_fallback:
    logger.warning(
        "enable_time_fallback is DISABLED — only for test environments; "
        "if data thread stalls, buffers may not flush automatically."
    )
```

#### `_flush_expired_order_minutes`

新增 `order_watermark` 参数，双判定：

```python
def _flush_expired_order_minutes(
    self,
    buffers: Dict[str, "_OrderMinuteBuffer"],
    output_dir: str,
    output_delay_sec: int,
    order_watermark: str,
) -> None:
    expired_keys = [
        k for k in buffers
        if is_data_driven_expired(k, order_watermark, self._data_flush_delay_minutes)
           or (self._enable_time_fallback and is_expired(k, output_delay_sec))
    ]
    for minute_key in sorted(expired_keys):
        self._flush_order_minute(buffers, minute_key, output_dir)
```

#### `_order_loop` 处理顺序重构

当前代码的 `_order_loop` 需要三处修改：

**修改 1：Record-driven flush 条件从 `!=` 改为 `>`（关键）**

当前代码（`engine.py` 第 372 行）：
```python
if current_minute is not None and minute_key != current_minute:
    self._flush_order_minute(buffers, current_minute, output_dir)
```

在 `current_minute` 单调递增后，乱序记录（如 `0930` 在 `0931` 之后到达）会触发 `"0930" != "0931"` → True → 错误 flush `current_minute="0931"`（正在收集中的分钟）。

修改为：
```python
if current_minute is not None and minute_key > current_minute:
    self._flush_order_minute(buffers, current_minute, output_dir)
```

Record-driven flush 只在数据时间**向前推进**时触发。乱序/迟到 record 不得触发当前分钟 flush，交给 watermark-driven `_flush_expired_order_minutes` 处理。

**修改 2：`current_minute` 和 `current_date` 单调递增更新**

当前代码（`engine.py` 第 405 行）：
```python
current_minute = minute_key
current_date = record_date
```

修改为：
```python
if current_minute is None or minute_key > current_minute:
    current_minute = minute_key
    current_date = record_date
```

**修改 3：处理顺序调整 — seqno 构建 → late 检测 → record-driven flush → buffer 写入**

确保 seqno 分配和 record 构建在 late 检测之前完成（late record 需要 `OrderRecord` 含 seqno），late 检测在 buffer 写入和 minute boundary flush 之前。以下是完整的 `_order_loop` 循环体内处理顺序：

```python
# Step 0: 解析
parsed = parse_order_line(line)
if parsed is None:
    continue

record_date = str(parsed.time)[:8]
minute_key = time_to_minute_key(parsed.time)

# Step 0.5: 跨日判断 — 先 flush 旧日残留 buffer 并重置日内状态，
# 然后让当前 record（新日第一条有效数据）继续正常处理（分配 seqno + 写入新日 buffer）
if current_date is not None and record_date != current_date:
    try:
        self._flush_all_order_buffers(buffers, output_dir)
    except Exception:
        logger.exception("Cross-day order flush failed, resetting date anyway")
    finally:
        # 无论 flush 是否成功，清空旧日期 buffer 防止残留数据干扰新日处理
        old_keys = [k for k in buffers if k[:8] != record_date]
        for k in old_keys:
            buffers.pop(k, None)
    current_date = record_date
    current_minute = None    # 跨日重置 watermark

if current_date is None:
    current_date = record_date

# Step 1: seqno 分配 + record 构建（必须在 late 检测之前）
seqno += 1
record = build_order_record(parsed, seqno)

# Step 2: Late order detection（跳过 Step 3/4/5）
if minute_key in self._flushed_order_minutes:
    count = late_order_per_minute.get(minute_key, 0)
    if count >= MAX_LATE_ORDER_RECORDS_PER_MINUTE:
        continue
    late_order_per_minute[minute_key] = count + 1
    # ... append late record (使用已构建的 record) ...
    continue

# Step 3: Record-driven flush: only on forward progress
if current_minute is not None and minute_key > current_minute:
    self._flush_order_minute(buffers, current_minute, output_dir)

# Step 4: Buffer write
buf = buffers.get(minute_key)
if buf is None:
    buf = _OrderMinuteBuffer()
    buffers[minute_key] = buf
buf.records.append(record)
buf.line_end_offset = self._order_tailer.line_offset

# Step 5: Monotonic watermark update
if current_minute is None or minute_key > current_minute:
    current_minute = minute_key
    current_date = record_date
```

#### `_order_loop` 跨日重置说明

跨日处理已集成到上述 Step 0.5 中。`current_minute` 重置为 `None`（而非零点基准字符串），因为 order 的 `current_minute` 是 `_order_loop` 局部变量，不存在跨线程覆盖的竞态风险。`None or ""` 传入 `is_data_driven_expired` 时，空水印安全返回 False。这与 SharedState 侧的零点基准策略（`current_date + "0000"`）不同，因为 SharedState 的 `current_minute` 被 data-thread 和 clock-thread 跨线程读写，需要零点基准防止覆盖。

**跨日 flush 失败场景**：Step 0.5 使用 try-except 包裹 `_flush_all_order_buffers`，flush 失败时记录异常日志但不阻止 `current_date` 更新。这确保即使旧日 flush 部分失败，`current_date` 和 `current_minute` 仍会正确更新为新日期，避免后续新日数据重复触发跨日 flush（对已空的 buffer）。已 flush 的分钟不会被重复处理（buffer 已 pop），未 flush 的分钟由 `finally` 块中的 `_flush_all_order_buffers` 兜底处理。

#### `_order_loop` watermark-driven flush 调用处

传入 order 的 `current_minute`：

```python
# Watermark-driven: flush expired minutes
self._flush_expired_order_minutes(
    buffers, output_dir, output_delay_sec, current_minute or ""
)
```

#### `_enforce_max_pending`

无需修改核心逻辑。`_enforce_max_pending` 的 flush 触发条件是 `len(buffers) > MAX_PENDING_ORDER_MINUTES`（基于 buffer 数量的硬限制，默认 3），与 flush 判定策略（数据驱动 / 真实时钟）无关。这是独立的内存保护机制，确保极端情况下 buffer 不会无限增长。

**数据驱动模式下的行为说明**：跳分钟场景中（如 buffer 包含 0900、0902、0905，watermark 停留在 0905），buffer 数量为 3，恰好等于阈值，`_enforce_max_pending` 不会触发。当 buffer 数量超过 3 时，强制 flush 最早的分钟（`min(buffers)`）。`MAX_PENDING_ORDER_MINUTES=3` 基于正常交易间隔（每 1 分钟）的假设；在跳分钟场景中，buffer 计数可能逻辑上较低但实际较大。当前默认值对日本股票市场足够。`is_expired` 仅用于日志中标记分钟是否已过期，可更新日志信息以包含数据驱动状态（如 `current_minute` 值）便于排查。

#### `stop()` — Final flush

当前 `stop()` 只在 order 侧有 final flush（`_order_loop` finally 中的 `_flush_all_order_buffers`）。snapshot/kline 侧没有。

数据驱动 watermark 要求 watermark 推进到下一分钟才能 flush 上一分钟。最后一分钟（交易日收盘、数据文件结束）没有下一分钟数据推进 watermark，因此**数据驱动无法 flush 最后一分钟**。

生产环境 `enable_time_fallback=true` 时真实时钟兜底，但测试环境 `enable_time_fallback=false` 时会丢失。

`stop()` 必须在所有线程 join 并确认停止后，无条件 flush 所有残留 snapshot/kline buffers：

```python
def stop(self) -> None:
    self._running = False
    join_errors = []
    flush_error = None

    try:
        # Join 工作线程
        if self._order_thread:
            self._order_thread.join(timeout=10)
            if self._order_thread.is_alive():
                join_errors.append("order")
        if self._data_thread:
            self._data_thread.join(timeout=5)
            if self._data_thread.is_alive():
                join_errors.append("data")
        if self._clock_thread:
            self._clock_thread.join(timeout=5)
            if self._clock_thread.is_alive():
                join_errors.append("clock")

        if join_errors:
            logger.critical(
                "Threads still alive after join timeout: %s; skip final flush",
                join_errors,
            )
            # Dump 存活线程栈帧，辅助定位死锁/阻塞点
            for name, thread in [
                ("order", self._order_thread),
                ("data", self._data_thread),
                ("clock", self._clock_thread),
            ]:
                if thread and thread.is_alive():
                    import sys
                    frame = sys._current_frames().get(thread.ident)
                    if frame:
                        logger.error("Thread %s stack:\n%s", name, ''.join(
                            f"  File \"{fn}\", line {ln}, in {func}\n    {line.strip()}\n"
                            for fn, ln, func, line in traceback.extract_stack(frame)
                        ))
        else:
            # Final flush: 无条件 flush 所有残留 snapshot/kline buffers
            # 不判断 is_data_driven_expired，不判断 is_expired
            # 只要 buffer 有数据就 flush
            try:
                self._flusher.flush_all_remaining()
            except Exception as e:
                logger.exception("Final flush failed")
                flush_error = e   # 保留原始异常对象，避免丢失具体失败分钟列表

    except Exception as e:
        logger.exception("Engine stop failed unexpectedly")
        flush_error = e   # 保留原始异常对象
    finally:
        # 资源释放必须始终执行，每个资源独立 try-except 防止链式中断
        for name, resource in [
            ("snapshot_tailer", self._snapshot_tailer),
            ("order_tailer", self._order_tailer),
            ("code_table", self._code_table),
        ]:
            try:
                if resource is not None:
                    resource.close()
            except Exception:
                logger.exception("Failed to close %s", name)
        logger.info("Engine stopped")

    if join_errors or flush_error:
        raise RuntimeError(
            f"Engine stop errors: join_timeout={join_errors}, flush={flush_error}"
        ) from flush_error
```

`flusher.py` 抽取共享内部方法 `_flush_minutes_internal`，`_step3_minute_output` 和 `flush_all_remaining` 均复用：

```python
def _flush_minutes_internal(
    self, minute_keys: list, *, is_final: bool = False
) -> None:
    """Flush specified minutes from buffers to files.

    本方法负责：
    1. 在 SharedState.lock 内从 ohlcv_buffers/raw_snapshot_buffers/raw_order_buffers 中 pop 数据；
    2. 拷贝 latest_snapshot；
    3. lock 外写 snapshot/kline 文件；
    4. 写成功后在 lock 内更新 output_minutes / flushed_snapshot_minutes / last_output_minute / volume baseline；
    5. 调用方根据场景决定是否及何时写 checkpoint。

    Args:
        minute_keys: 要 flush 的分钟列表（调用方只做筛选，由本方法负责 pop）。
            调用方通过遍历 ohlcv_buffers.keys() 筛选，保证 key 在筛选时刻存在。
            pop 使用防御性处理（跳过已不存在的 key），避免并发或边界条件下 KeyError。
        is_final: True 时为 shutdown final flush，异常不终止进程，尽量多 flush 并收集错误。
    """
    with self._state.lock:
        ohlcv_data = {}
        for k in minute_keys:
            v = self._state.ohlcv_buffers.pop(k, None)
            if v is not None:
                ohlcv_data[k] = v
            else:
                logger.debug("minute_key %s not found in ohlcv_buffers during flush, skipping", k)
        raw_data = {}
        for k in minute_keys:
            v = self._state.raw_snapshot_buffers.pop(k, None)
            if v is not None:
                raw_data[k] = v
        # 注意：当前 Live 模式下 raw_order_buffers 不被填充（order 由独立线程处理），
        # 此处保留 pop 用于向后兼容和防御性编程。
        order_data = {}
        for k in minute_keys:
            v = self._state.raw_order_buffers.pop(k, None)
            if v is not None:
                order_data[k] = v
        snapshot_copy = dict(self._state.latest_snapshot)

    errors = []
    for minute_key in minute_keys:
        data = ohlcv_data.get(minute_key)
        if data is None:
            continue   # defensive pop 跳过的 key，直接跳过
        raw = raw_data.get(minute_key, {})
        orders = order_data.get(minute_key, [])
        try:
            self._write_minute_files(minute_key, snapshot_copy, data, raw, orders)
        except Exception:
            if is_final:
                logger.exception("Final flush failed for minute=%s", minute_key)
                errors.append(minute_key)
                continue   # 尽可能多 flush
            else:
                logger.fatal("Output failed for minute=%s", minute_key)
                raise SystemExit(1)

        # 成功后更新状态（final flush 和正常 flush 都需要，保证 checkpoint 一致）
        with self._state.lock:
            self._state.output_minutes.add(minute_key)
            self._state.last_output_minute = minute_key
            self._state.flushed_snapshot_minutes.add(minute_key)
            for sym, agg in data.items():
                self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
                self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
        if is_final:
            logger.info("Final flush: minute=%s (%d symbols)", minute_key, len(data))

    if is_final:
        total = len(ohlcv_data)
        if errors:
            logger.error("Final flush summary: %d/%d minutes failed: %s", len(errors), total, errors)
            raise RuntimeError(
                f"Final flush failed for {len(errors)}/{total} minutes: {errors}"
            )
        else:
            logger.info("Final flush summary: %d minutes flushed successfully", total)
```

`_step3_minute_output` 调用 `_flush_minutes_internal`：

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
            # 日志分类在 lock 内完成，避免释放 lock 后重复计算
            data_driven_keys = [
                k for k in expired_keys
                if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
            ]
            fallback_keys = [k for k in expired_keys if k not in data_driven_keys]
    except ValueError:
        logger.exception("Invalid minute_key or watermark format, skipping tick (data_watermark=%s)", locals().get('data_watermark'))
        return
    if expired_keys:
        if data_driven_keys:
            logger.info("Data-driven flush: %d minutes (watermark=%s)", len(data_driven_keys), data_watermark)
        if fallback_keys:
            logger.warning("Time-fallback flush: %d minutes (data progress lagging)", len(fallback_keys))
        self._flush_minutes_internal(expired_keys, is_final=False)
```

`flush_all_remaining()` 调用 `_flush_minutes_internal`：

```python
def flush_all_remaining(self) -> None:
    """Flush all remaining buffers on shutdown.

    PRECONDITION: All worker threads (data, clock, order) must have been
    joined AND confirmed stopped before calling this method. NOT thread-safe.
    No watermark or time checks — unconditional flush of all remaining data.
    """
    with self._state.lock:
        remaining_keys = sorted(self._state.ohlcv_buffers.keys())

    # 分离 flush 和 checkpoint 异常，避免 checkpoint raise 覆盖 flush 错误信息
    flush_error = None
    if remaining_keys:
        try:
            self._flush_minutes_internal(remaining_keys, is_final=True)
        except Exception as e:
            flush_error = e  # 捕获但不立即 raise，先写 checkpoint

    # 无论 final flush 是否有部分失败，都尝试写 checkpoint。
    # 已成功 flush 的分钟状态已在 _flush_minutes_internal 中更新，
    # checkpoint 必须持久化这些进度，避免重启后重复处理。
    try:
        self._write_checkpoint()
    except Exception:
        logger.exception("Failed to write checkpoint after final flush")
        if flush_error:
            logger.error("Also had flush errors (checkpoint error takes priority): %s", flush_error)
        raise
    # checkpoint 成功后，如果有 flush 失败，re-raise flush 错误（优先级高于 checkpoint）
    if flush_error:
        raise flush_error
```

Order 侧已有 final flush（`_order_loop` finally 中的 `_flush_all_order_buffers`），无需额外改动。但 finally 块需改为独立 try-except 模式，防止一个 close 失败阻断另一个：

```python
# _order_loop finally 块
finally:
    try:
        self._flush_all_order_buffers(buffers, output_dir)
    except Exception:
        logger.exception("Order final flush failed")
    try:
        self._order_tailer.close()
    except Exception:
        logger.exception("Failed to close order tailer")
```

**资源 close 幂等性**：`_order_loop` finally 关闭 `_order_tailer`，`stop()` finally 也关闭同一资源。`close()` 方法必须幂等（重复调用不报错）。如果底层 tailer 的 `close()` 非幂等，应在 `stop()` finally 中跳过已关闭的 tailer（如 `if self._order_tailer is not None` 检查后设 `self._order_tailer = None`）。

### 5.5 `aggregator.py` — current_minute 单调性

`process_snapshot` 中 `current_minute` 更新改为单调递增（当前代码第 134 行）：

```python
# 当前代码
self.current_minute = minute_key

# 修改为
if not self.current_minute or minute_key > self.current_minute:
    self.current_minute = minute_key
```

`YYYYMMDDHHMM` 格式的字符串字典序与时间序一致，`>` 比较正确。初始值 `self.current_minute = ""`（空字符串），任何非空 `minute_key > ""` 为 True，第一次赋值正确通过。

### 5.6 不改动的组件

| 组件 | 原因 |
|------|------|
| `flusher.py` `_step4_handle_late_records` | late record append 机制完整保留 |
| `flusher.py` `_step5_write_checkpoint` | checkpoint 安全逻辑不受影响 |
| `is_expired()` | 保留作为 fallback，签名不变 |
| `replay.py` | Replay 模式使用独立的延迟 flush 策略，不使用 watermark |
| `writer.py` | 写入逻辑不变 |
| `checkpoint.py` | checkpoint 格式不变 |

注：`flusher.py` 内部有改动的函数包括 `_step1_cross_day_check`（重置 `current_minute` 为新日期零点基准）、`_step3_minute_output`（数据驱动判定，委托 `_flush_minutes_internal`）、`__init__`（新参数）、新增 `_flush_minutes_internal()`（共享 flush 逻辑）、新增 `flush_all_remaining()`（shutdown final flush，委托 `_flush_minutes_internal(is_final=True)`）。`_step4` 和 `_step5` 不改动。

---

## 6. 测试计划

### 6.1 单元测试

1. **`is_data_driven_expired` 函数测试**：
   - 基本场景：watermark 领先 → True，落后 → False
   - 边界值：watermark == threshold → True
   - 空水印：`data_watermark=""` → False
   - 跳分钟：watermark 跨越多分钟 → True
   - delay_minutes=0：数据刚到下一分钟就 flush
   - delay_minutes=2：需要 2 分钟窗口
   - 跨日期不同 HHMM：watermark 日期 > minute_key 日期
   - 跨日期相同 HHMM：`minute_key="202605190930"`, `watermark="202605200930"`, delay=1 → True
   - 午夜边界：`minute_key="202605202359"`, `watermark="202605210000"`, delay=1 → True

2. **flusher 集成测试**（mock `is_expired` 和 `is_data_driven_expired`）：
   - 数据驱动触发 flush：设 `current_minute` 领先
   - Fallback 触发 flush：数据未推进但真实时钟超时
   - 两者都不触发：不 flush

3. **order flush 测试**：
   - `_flush_expired_order_minutes` 接收 `order_watermark` 参数
   - 数据驱动和 fallback 双路径验证

4. **`current_minute` 单调性测试**：
   - `SharedState.process_snapshot`：输入 `"202605210905"` → `"202605210903"` → `"202605210906"`，验证 `current_minute` 不回退（始终为最大值 `"202605210906"`）
   - `_order_loop` 模拟：`current_minute="202605210931"` 收到 `minute_key="202605210930"` record，不回退

5. **order record-driven flush 条件测试**：
   - `current_minute="202605210931"`，收到 `minute_key="202605210930"` record → 不触发 `"202605210931"` flush
   - `current_minute="202605210930"`，收到 `minute_key="202605210931"` record → 触发 `"202605210930"` flush

6. **跨日重置测试**：
   - 跨日后 `SharedState.current_minute` 被设为新日期零点基准值（如 `"202605220000"`）
   - 如果 data-thread 已将 `current_minute` 推进到 `"202605220900"`，条件赋值不覆盖
   - 新日期第一条数据到达后 `current_minute` 从零点基准正确推进到实际数据时间

7. **fallback 配置测试**：
   - `enable_time_fallback=false` 启动时输出 WARNING 日志

8. **config 加载测试**：
   - `RecoveryConfig` 默认值：`data_flush_delay_minutes=1`，`enable_time_fallback=True`
   - `load_config` 正确解析 INI 中新字段
   - INI 中不包含新字段时使用默认值（向后兼容性）

9. **`_order_loop` 处理顺序测试（修改 3）**：
   - `flushed_order_minutes` 包含 `"202605210930"`，`current_minute="202605210931"`，收到 `minute_key="202605210930"` record
   - 验证：先完成 seqno + build_order_record，再走 late 路径（continue），不触发 record-driven flush，不更新 watermark，不创建新 buffer

10. **Final flush 测试**：
    - `Engine.stop()` 时 `ohlcv_buffers` 中仍有残留分钟 → 验证所有 buffer 被无条件 flush
    - `_flush_minutes_internal(is_final=True)` 不判断任何过期条件，直接写出
    - Final flush 成功后 `output_minutes`、`flushed_snapshot_minutes`、`last_output_minute` 等状态正确更新
    - Final flush 失败时 `continue` 尝试后续分钟，最终 raise `RuntimeError`
    - `stop()` 中工作线程仍 alive 时记录 CRITICAL 日志，不执行 final flush
    - Order 侧已有 final flush（`_order_loop` finally），验证其正常工作

11. **`_flush_minutes_internal` 共享方法契约测试**：
    - `is_final=False`：单个分钟写入失败时 `raise SystemExit(1)`
    - `is_final=True`：单个分钟写入失败时 `continue`，收集 errors，最终 raise `RuntimeError`
    - 防御性 pop：`ohlcv_buffers` 中 key 不存在时跳过（不 KeyError），记录 WARNING

12. **`stop()` 异常安全测试**：
    - Final flush 抛出 `RuntimeError` 后，`finally` 块中的三个 close 仍被调用
    - 工作线程 alive 导致跳过 final flush 时，`finally` 块中的 close 仍被调用
    - 单个 close() 失败不阻止其他 close() 执行
    - 错误分类：`join_timeout`、`flush_error` 分别记录，最终 RuntimeError 携带分类信息

13. **`flush_all_remaining` checkpoint 保证测试**：
    - 部分分钟 flush 成功、部分失败后，checkpoint 仍被尝试写入
    - Checkpoint 中 `output_minutes` 包含已成功 flush 的分钟，不包含失败的分钟
    - Checkpoint 写入失败时，flush 错误信息不被覆盖（两个错误独立记录）

14. **`raw_order_buffers` 空路径测试**：
    - 验证 `_flush_minutes_internal` 在 `raw_order_buffers` 为空时不产生空文件、不触发异常

	- 注：当前 Live 模式下 `raw_order_buffers` 始终为空（order 由独立线程处理），此为防御性测试

15. **跨日 + enable_time_fallback=false + 残留 buffer 测试**：
	- `ohlcv_buffers` 中有旧日期（如 `20260521`）的残留数据，`enable_time_fallback=false`
	- 触发跨日重置（`current_minute` 推进到 `20260522`），验证旧日期 buffer 在跨日 flush 中被处理（不受数据驱动判定影响）
	- 验证跨日后 `current_minute` 被设为零点基准 `"202605220000"`，不会误触发新日期数据 flush

16. **Watermark 停滞检测测试**：
	- `data_watermark` 连续 N 次 tick 不变（N = `STALL_WARN_THRESHOLD`），验证 logger.error 被调用
	- `data_watermark` 推进后 stall 计数器重置为 0
	- 停滞告警只在到达阈值时触发一次，不会每个 tick 重复告警

### 6.2 端到端测试

使用 data_simulator + live 模式（speed=100）验证：
- 所有分钟输出数据完整（symbol 数量与 replay 模式一致）
- late record 数量远少于修复前
- 无数据丢失
- 必须使用 `enable_time_fallback=false` 配置，验证真实时钟不会提前触发 flush
- 额外验证 `enable_time_fallback=true` + `output_delay_sec=3600` 配置也能正确工作

---

## 7. 实现约束

### 7.1 数据水印必须单调前进

`current_minute` 是数据驱动 flush 的核心依据，必须保证单调递进，不能被迟到记录拉回：

```python
# SharedState.process_snapshot 中
minute_key = time_to_minute_key(record.time)
# current_minute 只在 minute_key 更大时更新（初始值 "" 时 minute_key > "" 恒为 True）
if not self.current_minute or minute_key > self.current_minute:
    self.current_minute = minute_key
```

```python
# _order_loop 中
minute_key = time_to_minute_key(parsed.time)
if current_minute is None or minute_key > current_minute:
    current_minute = minute_key
```

违反单调性会导致：watermark 回退 → 已 flush 的分钟被判为未过期 → buffer 重建 → 重复 flush 或数据错乱。

**并发访问约束**：`SharedState.current_minute` 的所有读操作必须在 `self._state.lock` 保护下进行。当前 clock-thread 通过 `_step3_minute_output` 在 lock 内读取 `data_watermark`，满足此约束。如果未来有新的读取路径（如监控线程），必须加锁保护。`_order_loop` 的 `current_minute` 是局部变量，不与任何其他线程共享，无需加锁。

### 7.2 Order record-driven flush 只在数据前进时触发

`_order_loop` 中的 record-driven minute boundary flush 条件必须从 `minute_key != current_minute` 改为 `minute_key > current_minute`。

```python
# 正确：只在数据前进时 flush 前一分钟
if current_minute is not None and minute_key > current_minute:
    self._flush_order_minute(buffers, current_minute, output_dir)

# 错误：乱序记录会触发当前分钟过早 flush
# if current_minute is not None and minute_key != current_minute:
```

反例：`current_minute="0931"`，乱序 `minute_key="0930"` 到达时：
- `"0930" != "0931"` → True → flush `"0931"`（正在收集中，数据不完整）
- `"0930" > "0931"` → False → 不触发，`"0930"` 由 watermark-driven flush 处理

乱序 record 的 flush 完全交给 `_flush_expired_order_minutes`（数据驱动 + fallback 双判定）。

### 7.3 迟到记录检测必须在 buffer 写入之前

处理顺序固定为：

```python
minute_key = time_to_minute_key(record.time)

if minute_key in flushed_minutes:
    handle_late_record(record)       # 先检测 late
else:
    append_to_active_buffer(record)  # 再写入 buffer
    update_current_minute_if_forward(minute_key)
```

不能先放 buffer 再判断 late，否则已 flush 的分钟可能重新创建 buffer，后续被重复处理。

### 7.4 late append 失败不得推进 checkpoint

late append 失败时，必须：
- 不推进 checkpoint
- 不推进 committed_offset
- 不继续运行（raise 或 fatal）

否则会出现：record 已读取 → append 失败 → checkpoint 推进 → 重启后跳过该 record → 静默丢数据。

这一约束已在现有 `_step5_write_checkpoint` 中实现（late queue 非空则跳过 checkpoint），本方案不改变此行为。

### 7.5 Snapshot late append 后的 latest_snapshot 更新

late snapshot append 后，如果 record 比当前 `latest_snapshot` 新，则更新。影响范围：

- 后续尚未输出分钟的 carry-forward 使用新状态
- 已输出分钟不回写
- K 线不自动重算

业务口径：late append 保证真实 record 不丢；不保证已输出 kline / carry-forward 被历史修正。

### 7.6 默认配置

在"源文件总体按时间顺序、仅少量 late records"的前提下：
- `data_flush_delay_minutes=1` 是合理的默认值
- 如果后续发现 snapshot late > 0.5% 或 order late > 0.1%，再考虑调为 2
- 第一版不过度保守

### 7.7 测试环境必须关闭 time fallback

生产环境 fallback 是兜底，测试环境 fallback 是干扰源。data simulator 加速测试必须使用：

```ini
enable_time_fallback = false
```

### 7.8 跨日重置必须重置 data watermark 为新日期零点基准

`_step1_cross_day_check` 跨日重置时，`current_minute` 必须设为新日期零点基准值（如 `current_date + "0000"`），而非空字符串 `""`。

设为零点基准而非清空为 `""` 的原因：
- 避免竞态：data-thread 可能已将 `current_minute` 设为新日期值（如 `"202605220900"`），如果 clock-thread 将其清空为 `""`，会导致 watermark 丢失。零点基准值 `"202605220000"` 不会大于新日期真实数据时间，不会误触发 flush
- `enable_time_fallback=false` 的测试环境：空水印导致 `is_data_driven_expired` 返回 False，新日期的 flush 被延迟到第一条数据到达后；零点基准值同理，但语义更清晰——表示"新日期已开始，等待数据推进"
- 监控一致性：零点基准值明确表示"跨日重置已完成，新日期进行中"，比空字符串更利于日志分析

### 7.9 `enable_time_fallback=false` 启动时必须输出 WARNING

生产环境误设此值会导致 data-thread 卡死时永远不 flush。启动时必须输出 WARNING：

```python
if not config.recovery.enable_time_fallback:
    logger.warning(
        "enable_time_fallback is DISABLED — only for test environments; "
        "if data thread stalls, buffers may not flush automatically."
    )
```

`enable_time_fallback=false` 仅允许用于 data simulator / accelerated live test。生产环境如关闭该配置，建议由部署检查阻止上线。

### 7.10 Engine stop/shutdown 必须 final flush 所有残留 buffers

数据驱动 watermark 的语义是"watermark 推进到下一分钟时 flush 上一分钟"。当数据流结束（交易日收盘、数据文件末尾、程序停止）时，最后一分钟没有下一分钟数据来推进 watermark，因此**数据驱动判定无法 flush 最后一分钟**。

生产环境 `enable_time_fallback=true` 时，真实时钟 fallback 会在 `output_delay_sec` 后兜底。但以下场景需要 final flush：

1. `enable_time_fallback=false` 的测试环境 — 最后一分钟无法被任何机制 flush
2. 程序在最后一分钟后很快被 stop — 没等到 `output_delay_sec`
3. 手动停止服务时 buffer 中仍有数据
4. 回放测试中数据文件结束但 live engine 没有真实下一分钟

**约束**：`Engine.stop()` 在所有线程 join 后，必须无条件 flush 所有残留 snapshot/kline/order buffers。不判断 `is_data_driven_expired`，不判断 `is_expired`，只要 buffer 有数据就 flush。

**线程安全前提**：`flush_all_remaining()` 不是线程安全的。调用前必须确认所有工作线程（data、clock、order）已停止（通过 `is_alive()` 检查）。如果任何线程在 join timeout 后仍 alive，`stop()` 必须 raise `RuntimeError` 而非继续 final flush，避免并发操作 buffer。

**状态一致性**：final flush 的每一分钟成功写出后，必须同步更新 `output_minutes`、`flushed_snapshot_minutes`、`last_output_minute`、`last_totalvol_by_symbol`、`last_totalamount_by_symbol` 等状态字段，保证 `_write_checkpoint()` 记录的状态与实际输出一致。

**错误处理**：final flush 中某个分钟写入失败时，应 `continue` 尝试后续分钟（尽可能多救回数据），记录失败分钟列表，最终 raise `RuntimeError`。不能 `break` 放弃后续所有分钟，也不能假装 stop 成功。

**已有机制**：Order 侧已在 `_order_loop` finally 中有 `_flush_all_order_buffers`。Snapshot/kline 侧使用 `flusher.flush_all_remaining()`（内部委托 `_flush_minutes_internal(is_final=True)`）。

### 7.11 Watermark 停滞检测

数据驱动模式下，`current_minute` 停滞意味着数据源不再推进。无论 `enable_time_fallback` 是否开启，watermark 停滞都应产生告警（说明数据源可能异常）。

在 clock-thread 的 `_step3_minute_output` 开头增加停滞检测：

```python
def _step3_minute_output(self) -> None:
    with self._state.lock:
        data_watermark = self._state.current_minute
        # ...expired_keys 筛选...

    # Watermark 停滞检测（基于 wall-clock 时间，不受 tick 间隔变化影响）
    now = time.monotonic()
    if data_watermark == self._last_watermark:
        if self._last_watermark_advance_ts is not None:
            stalled_sec = now - self._last_watermark_advance_ts
            if not self._stall_warned and stalled_sec >= STALL_WARN_SECONDS:  # e.g., 300
                logger.error(
                    "Watermark stalled at %s for %.0f seconds — data thread may be dead or data source stopped",
                    data_watermark, stalled_sec,
                )
                self._stall_warned = True
    else:
        if self._last_watermark is not None and self._stall_warned:
            logger.info(
                "Watermark recovered: %s -> %s (was stalled for %.0f seconds)",
                self._last_watermark, data_watermark,
                now - self._last_watermark_advance_ts if self._last_watermark_advance_ts else 0,
            )
        self._last_watermark = data_watermark
        self._last_watermark_advance_ts = now
        self._stall_warned = False
```

`_last_watermark`、`_last_watermark_advance_ts`、`_stall_warned` 是 `ClockWatermarkFlusher` 实例变量（仅 clock-thread 访问，无需 lock）。使用 `time.monotonic()` 而非 tick 计数，避免 tick 间隔因时段变化（盘前 1000ms、盘中 200ms、午休 5000ms）导致停滞检测时间不稳定。`STALL_WARN_SECONDS` 建议为 300（5 分钟）。`_stall_warned` 标志确保只触发一次告警，watermark 恢复推进后输出 INFO 日志并重置标志。

---

## 8. 设计决策记录

| 事项 | 决策 |
|------|------|
| flush 时机 | 保留等待窗口（可配置 `data_flush_delay_minutes`），默认 1 分钟 |
| `is_data_driven_expired` 比较语义 | 按 minute_key 起始时间比较，`watermark >= minute_key + delay_minutes` |
| 真实时钟 fallback 可控性 | 新增 `enable_time_fallback`，生产=true，测试=false |
| `output_delay_sec` 在数据驱动模式下的角色 | 复用为 fallback 超时，仅在 `enable_time_fallback=true` 时生效 |
| order 的 `current_minute` 跟踪方式 | 独立水印，使用 `_order_loop` 局部变量 |
| fallback 超时时间 | 沿用 `output_delay_sec` 默认 5s |
| 数据水印单调性 | current_minute 只在 minute_key > current_minute 时更新（阻塞项） |
| order record-driven flush 条件 | 从 `minute_key != current_minute` 改为 `minute_key > current_minute`（阻塞项） |
| 迟到检测顺序 | late 判断在 buffer 写入之前，order loop 中 late 检测在 record-driven flush 之前 |
| checkpoint 安全 | late append 失败不推进 checkpoint（已有机制保持） |
| kline 回写 | late append 不回写已输出 kline/carry-forward |
| 跨日重置 | `current_minute` 设为新日期零点基准（`current_date + "0000"`），避免覆盖 data-thread 已设置的新日期值 |
| 启动校验 | `enable_time_fallback=false` 时输出 WARNING，提示仅用于测试环境 |
| Final flush | `Engine.stop()` 在确认所有工作线程停止后，无条件 flush 所有残留 buffers，状态同步更新，失败 continue + 最终 FATAL |
| Final flush 线程安全 | `flush_all_remaining()` 非线程安全，调用前必须确认工作线程已停止；仍 alive 时 raise RuntimeError |
| Final flush 状态一致性 | 每分钟成功写出后更新 output_minutes / flushed_snapshot_minutes / last_output_minute，保证 checkpoint 一致 |
| Order loop 处理顺序 | seqno + build_order_record → late 检测 → record-driven flush → buffer 写入 → watermark 更新 |
| 共享 flush 方法 | 抽取 `_flush_minutes_internal(minute_keys, is_final)` 供 `_step3_minute_output` 和 `flush_all_remaining` 复用 |
| Order 跨日重置 | `_order_loop` 跨日时同步清空 `current_minute=None`，`current_date` 随 watermark 单调更新 |
| `_enforce_max_pending` 不纳入数据驱动 | 内存保护基于 buffer 数量硬限制，独立于 flush 判定策略，不需修改 |
| `stop()` 异常安全 | try-finally 保护资源释放（tailer/code_table close），flush 失败不影响资源清理 |
| `flush_all_remaining` checkpoint 保证 | try-finally 确保部分失败后仍写 checkpoint，持久化已成功 flush 的分钟进度 |
| 跨日条件赋值 | `current_minute` 仅在低于零点基准时设置，避免覆盖 data-thread 已推进的新日期值 |
| Flusher vs Order 跨日策略 | Flusher 用零点基准字符串（跨线程竞态保护），Order 用 None（局部变量无竞态），文档明确解释差异 |
| `stop()` 错误信息保留 | `flush_error` 保留原始异常对象（非字符串），避免丢失具体失败分钟列表；raise 时使用 `from` 链式异常 |
| `_flush_minutes_internal` 防御性 pop | 所有 buffer（ohlcv/raw_snapshot/raw_order）统一使用 `pop(k, None)` + `.get()` 防御 |
| Flush 触发来源日志 | `_step3_minute_output` 按触发原因分组：data-driven 用 INFO，fallback 用 WARNING |
| Watermark 停滞检测 | clock-thread 连续 N 次 tick watermark 不变时 ERROR 告警，推进后重置计数器 |
| `data_flush_delay_minutes` 校验 | 非负整数校验，负值 ValueError；0 合法但不建议生产使用 |
| 跨日 flush 不改用 `_flush_minutes_internal` | 跨日 flush 后立即清空按天状态集，改用共享方法会矛盾；保持直接写入 |
| `_order_loop` finally 异常安全 | flush 和 tailer close 各自独立 try-except，防链式中断 |
| 跨日 flush 失败仍更新 `current_date` | try-except 包裹 flush，确保 `current_date`/`current_minute` 总是更新，避免重复触发跨日 |
| minute_key 格式校验 | `minute_key_to_start_time` 使用 `datetime.strptime` 一步完成格式+语义校验（含 HHMM 范围） |
| `stop()` 错误信息类型 | `flush_error` 始终为 Exception 对象（非字符串），`from flush_error` 保留异常链 |
| `flush_all_remaining` 异常捕获 | `except Exception`（非 RuntimeError），防止 SystemExit 绕过 checkpoint |
| `_step3` 日志分类 | 在 lock 内构建 data_driven_keys / fallback_keys，避免释放 lock 后重复计算 |
| `_step3` ValueError 防御 | catch `minute_key_to_start_time` 的 ValueError，跳过 tick 而非中断 clock-thread |
| Watermark 停滞检测 | 基于 `time.monotonic()` wall-clock 时间（非 tick 计数），`_stall_warned` 标志保证单次告警+恢复日志 |
| 跨日 flush 旧 buffer 清理 | `try-finally` 确保无论 flush 是否成功，旧日期 buffer 从 dict 中移除 |
| 资源 close 幂等性 | `_order_loop` finally 和 `stop()` finally 可能 close 同一资源，`close()` 必须幂等 |
| Final flush 汇总日志 | 循环结束后输出成功/失败数汇总，便于运维确认 shutdown 状态 |
| stop() join 超时 dump 线程栈 | `sys._current_frames()` 输出存活线程调用栈，辅助定位死锁/阻塞 |
| `data_flush_delay_minutes` 上限 | 大于 10 时 WARNING 提醒，防止 buffer 无限积压 |
| 防御性 pop 日志级别 | `ohlcv_buffers` 中 key 不存在时 DEBUG（非 WARNING），避免正常场景日志嘈杂 |

---

## 9. 升级注意事项

### 9.1 Flush 时序变化

升级后 snapshot/kline/order 文件的生成时机会比旧版本**更早**：

- **旧版**：固定延迟 `output_delay_sec`（默认 5s）后 flush。例如 0900 数据在 09:01:05 才输出文件
- **新版**：数据驱动 flush 在 watermark 推进到下一分钟时触发。例如 0901 数据到达（如 09:01:03）即 flush 0900，比旧版提前约 2-3 秒

这是预期优化——flush 时机由数据实际到达进度决定，而非固定时钟延迟。但如果下游系统依赖"固定 5 秒后才有文件"的时序假设，需要同步调整。

如需保持旧版延迟行为，可增大 `data_flush_delay_minutes` 或依赖 `enable_time_fallback=true` + `output_delay_sec` 配置。

### 9.2 配置向后兼容

新增配置项 `data_flush_delay_minutes` 和 `enable_time_fallback` 使用 dataclass 默认值作为 INI fallback。现有 INI 文件无需修改即可升级——默认值保证与旧行为兼容：

- `enable_time_fallback=true`（默认）：保留真实时钟判定
- `data_flush_delay_minutes=1`（默认）：新增数据驱动判定，flush 时机只会提前不会延后

### 9.3 函数签名变更

- `ClockWatermarkFlusher.__init__`：新增参数 `data_flush_delay_minutes`、`enable_time_fallback`（均有默认值）
- `Engine._flush_expired_order_minutes`：新增参数 `order_watermark`（无默认值，必须传参）。行为变更：内部同时使用数据驱动和 fallback 双判定（OR 关系），而非仅依赖真实时钟
- `ClockWatermarkFlusher`：新增公共方法 `flush_all_remaining()`（shutdown 时由 `Engine.stop()` 调用）
- `ClockWatermarkFlusher`：新增内部方法 `_flush_minutes_internal(minute_keys, is_final)`，`_step3_minute_output` 重构为委托调用
- `aggregator.py` `process_snapshot`：`current_minute` 更新改为单调递增条件赋值（`if not self.current_minute or minute_key > self.current_minute`）
- `flusher.py` `_step1_cross_day_check`：跨日重置块新增 `current_minute` 条件赋值为零点基准值
- `ClockWatermarkFlusher`：新增实例变量 `_last_watermark`、`_last_watermark_advance_ts`、`_stall_warned`（watermark 停滞检测用）
- `clock.py`：新增模块级常量 `STALL_WARN_SECONDS = 300`（watermark 停滞告警阈值）

所有调用点在改动清单中已列出。

### 9.4 内部行为变更

**`_step3_minute_output` 重构**：旧版是完整方法（筛选 → pop → IO → 状态更新全流程），新版拆分为 `_step3_minute_output`（仅筛选过期分钟）+ `_flush_minutes_internal`（pop + IO + 状态更新）。如果有自定义子类覆盖了 `_step3_minute_output`，升级后行为会完全不同，需要重新实现。

**`_order_loop` 处理顺序变化**：
- 旧版：解析 → 跨日 → record-driven flush（`!=`）→ seqno → late 检测 → buffer 写入 → watermark 更新
- 新版：解析 → 跨日 → seqno + build_order_record → late 检测 → record-driven flush（`>`）→ buffer 写入 → watermark 更新
- 关键差异：seqno 分配提前到 late 检测之前（late record 也消耗 seqno）；flush 条件从 `!=` 改为 `>`（乱序 record 不触发当前分钟 flush）
- **`!=` 改为 `>` 的行为变化影响**：旧版 `!=` 条件下，乱序 record（`minute_key < current_minute`）也会触发当前分钟 flush；新版 `>` 条件下，乱序 record 不触发 flush，完全依赖 watermark-driven flush 处理。这意味着乱序到达的 order record 可能延迟当前分钟的 flush 时间点，但不会丢数据（watermark 推进后会统一 flush）。此变更减少了不必要的 flush 调用，提升了批量处理效率。

**`Engine.stop()` 行为变化**：新增 final flush 逻辑（flush 所有残留 snapshot/kline buffers）+ 线程 alive 检查 + try-finally 资源释放。旧版 `stop()` 只 join 线程后关闭 tailer，不 flush 残留 snapshot/kline。
