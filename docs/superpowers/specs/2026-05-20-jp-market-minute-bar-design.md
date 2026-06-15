# FIU 日股分钟级行情数据生成器 — 设计文档

## 概述

读取FIU接收服务实时写入的 snapshot.csv / code.csv 文件，每分钟生成全市场行情快照和 OHLCV K 线文件。采用纯时钟驱动触发输出，数据线程与输出线程解耦。

---

## 1. 数据源

### 1.1 文件格式

FIU 接收服务按日写入三种 CSV 文件：

- `snapshot.csv.YYYYMMDD` — 快照数据（主要数据源）
- `order.csv.YYYYMMDD` — 挂单数据（本期不参与输出）
- `code.csv.YYYYMMDD` — 码表（股票元信息）

**重要**：CSV header 列数不完整，必须按列位置索引，不依赖 header。

### 1.2 snapshot.csv 列映射（实际 17-21 列）

| 位置 | 字段 | 类型 | 必要 | 说明 |
|------|------|------|------|------|
| 0 | symbol | string | 是 | 证券代码 |
| 1 | time | int64 | 是 | 交易所时间，17位 YYYYMMDDHHMMSSMMM，JST (UTC+9) |
| 2 | preclose | int64 | 是 | 昨收价（原始值） |
| 3 | lastprice | int64 | 是 | 最新价（原始值） |
| 4 | open | int64 | 是 | 开盘价（日内累计） |
| 5 | high | int64 | 是 | 最高价（日内累计） |
| 6 | low | int64 | 是 | 最低价（日内累计） |
| 7 | close | int64 | 是 | 收盘价 |
| 8 | lasttradeprice | int64 | 是 | 最新成交价 |
| 9 | lasttradeqty | int64 | 是 | 最新成交量 |
| 10 | totalvol | int64 | 是 | 成交总量（累计） |
| 11 | totalamount | int64 | 是 | 成交总额（累计，原始值） |
| 12 | sessionid | int32 | 是 | 时段ID |
| 13 | tradetype | string | 否 | 成交类型 |
| 14 | status | string | 是 | T=可交易，P=停牌 |
| 15 | direction | int64 | 是 | 1=主买，0=中性，-1=主卖 |
| 16 | pflag | string | 是 | Y/N，是否用于画分时图 |
| 17 | decimal | int32 | 否 | 小数位（默认2） |
| 18 | vwap | int64 | 否 | 平均价（默认0） |
| 19 | shortsellflag | int32 | 否 | 沽空标识（默认0） |
| 20 | rcvtime | int64 | 否 | 接收时间，17位 YYYYMMDDHHMMSSMMM，CST (UTC+8)，默认系统时间 |

### 1.3 order.csv 列映射（实际 6-8 列）

本期不参与输出，仅预留接口。

| 位置 | 字段 | 必要 |
|------|------|------|
| 0 | symbol | 是 |
| 1 | time | 是 |
| 2 | bidprice | 是 |
| 3 | bidsize | 是 |
| 4 | askprice | 是 |
| 5 | asksize | 是 |
| 6 | decimal | 否 |
| 7 | rcvtime | 否 |

### 1.4 code.csv 列映射（实际 7-17 列）

| 位置 | 字段 | 必要 | 说明 |
|------|------|------|------|
| 0 | symbol | 是 | |
| 1 | market | 是 | 1=Tokyo, 6=Fukuoka, 8=Sapporo |
| 2 | marketdesc | 否 | |
| 3 | name | 是 | 股票名称 |
| 4 | money | 否 | 交易币种 |
| 5 | type | 否 | 证券类型 |
| 6 | subtype | 否 | 证券子类型 |
| 7 | issueclass | 否 | 公司分类 |
| 8 | industrycode | 否 | 行业代码 |
| 9 | isincode | 否 | ISIN码 |
| 10 | lotsize | 否 | 每手股数 |
| 11 | limitup | 否 | 价格上限（原始值） |
| 12 | limitdown | 否 | 价格下限（原始值） |
| 13 | decimal | 否 | 小数位 |
| 14 | rcvtime | 否 | |
| 15 | businessday | 否 | 业务日期 |
| 16 | baseprice | 否 | 基准价（原始值） |

### 1.5 列数校验规则

| 文件 | 最小必要列数 | 最大列数 | <最小 | >最大 |
|------|-------------|---------|-------|-------|
| snapshot | 17 | 21 | 丢弃(ERROR) | 丢弃(ERROR) |
| order | 6 | 8 | 丢弃(ERROR) | 丢弃(ERROR) |
| code | 7 | 17 | 丢弃(ERROR) | 丢弃(ERROR) |

缺失的可选列填默认值：decimal=2, vwap=0, shortsellflag=0, rcvtime=当前系统时间戳。

### 1.6 时区

| 字段 | 时区 | 用途 |
|------|------|------|
| time | JST (UTC+9) | 切分钟、输出文件名 |
| rcvtime | CST (UTC+8) | 排序 |
| 时钟驱动检查 | JST | |
| 程序内部时钟 | 部署服务器本地时区 | |

---

## 2. 架构

### 2.1 流水线架构

```
FileTailer(snapshot/code/order)
    ↓ bytes chunk
BinaryLineAssembler
    ↓ 完整行(bytes)
CsvRowParser(position-based)
    ↓ parsed fields
Validator
    ↓ valid record
MinuteAggregator
    ↓ aggregated data
ClockWatermarkFlusher
    ↓ expired minutes
AtomicWriter
    ↓
CheckpointManager
```

每个模块职责单一、可独立测试：
- **FileTailer**: 按 bytes offset 轮询读取文件增量，输出 raw bytes chunk
- **BinaryLineAssembler**: 按 `b"\n"` 切完整行，处理截断拼接，checkpoint offset 只推进到最后一个完整 `\n`
- **CsvRowParser**: 按列位置索引解析，不依赖 header
- **Validator**: 字段校验、错误分级
- **MinuteAggregator**: 更新 latest_snapshot、ohlcv_buffer、seqno
- **ClockWatermarkFlusher**: 纯时钟驱动，检查 watermark 过期分钟
- **AtomicWriter**: .tmp + rename 写入
- **CheckpointManager**: 管理 checkpoint 的读写和恢复

### 2.2 线程模型

```
┌──────────────────────┐     ┌─────────────────────────┐
│  Data Thread         │     │    Clock Thread          │
│  (FileTailer+Parser) │     │    (每秒检查)             │
│                      │     │                           │
│  FileTailer(rb模式)  │     │  Step 1: 跨日检查         │
│  BinaryLineAssembler │     │  Step 2: 首次数据检查     │
│  CsvRowParser        │     │  Step 3: watermark输出    │
│  Validator           │     │     +断流追赶             │
│  MinuteAggregator    │     │                           │
│  更新状态(加锁) ─────┼─────┼──→ 读取+删除buffer       │
│                      │     │     输出文件(锁外)        │
│                      │     │     写checkpoint(锁外)    │
└──────────────────────┘     └─────────────────────────┘
         │                            │
    ┌────┴─────┐              ┌───────┴───────┐
    │  共享状态  │              │    输出文件     │
    │ (buffer   │              │  CSV/Parquet   │
    │  lock)    │              └───────────────┘
    └──────────┘
```

### 2.3 共享状态与锁

```python
buffer_lock = threading.RLock()  # 保护以下共享状态

# 受锁保护的状态：
ohlcv_buffers: dict[str, dict[str, OHLCVAggregate]]
latest_snapshot: dict[str, SnapshotRecord]
last_totalvol_by_symbol: dict[str, int]       # 上一个已输出分钟结束时的totalvol
last_totalamount_by_symbol: dict[str, float]  # 上一个已输出分钟结束时的totalamount
seqno: int
current_minute: str
first_data_received: bool
last_output_date: str

# 仅时钟线程访问（无需锁）：
last_output_minute: str
output_minutes: set[str]
```

数据线程操作（持锁）：

```python
with buffer_lock:
    seqno += 1
    # 构造 immutable SnapshotRecord，seqno 在创建时传入（frozen=True 不可后赋值）
    record = build_snapshot_record(parsed_row, seqno=seqno)
    if minute_key not in ohlcv_buffers:
        ohlcv_buffers[minute_key] = {}
    update_ohlcv(ohlcv_buffers[minute_key], symbol, record,
                 last_totalvol_by_symbol.get(symbol, 0),
                 last_totalamount_by_symbol.get(symbol, 0.0))
    # agg.seqno = seqno 在 update_ohlcv 内完成
    latest_snapshot[symbol] = record
    current_minute = minute_key
    if not first_data_received:
        first_data_received = True
```

时钟线程操作（持锁获取数据，锁外做IO）：

```python
# Step 3: 获取过期分钟
with buffer_lock:
    expired = {k: ohlcv_buffers.pop(k) for k in sorted(ohlcv_buffers) if is_expired(k)}
    snapshot_copy = dict(latest_snapshot)

# 锁外写文件（不阻塞数据线程）
for minute_key, data in expired.items():
    try:
        write_snapshot_file(minute_key, snapshot_copy, data)
        write_kline_file(minute_key, snapshot_copy, data)
    except IOError as e:
        log FATAL: "output failed for minute=%s: %s" % (minute_key, e)
        raise SystemExit(1)
    # 每成功输出一个 minute，立即更新状态并原子写 checkpoint
    output_minutes.add(minute_key)
    last_output_minute = minute_key
    with buffer_lock:
        # 只更新有实际 OHLCVAggregate 的 symbol 的基线
        # carry-forward (update_flag=N) 的 symbol 不更新基线
        for sym, agg in data.items():
            last_totalvol_by_symbol[sym] = agg.end_totalvol
            last_totalamount_by_symbol[sym] = agg.end_totalamount
    write_checkpoint()
```

### 2.4 输出触发：纯时钟驱动 + Watermark

数据到达时**不触发任何输出**，只更新内存状态。

Watermark 定义：
```
expired(minute_key) = minute_key_end_time + output_delay_sec <= current_jst_time
```
其中 `minute_key_end_time` 由 minute_key 中的时间推算（如 `"202605200930"` → JST 09:31:00），`current_jst_time` 是系统时钟转为 JST。数据归属分钟由 `time` 字段决定。

时钟线程每秒执行以下检查：

```
Step 1 - 跨日检查：
  if last_output_date == "":
    last_output_date = extract_date(current_minute)
    return

  if extract_date(current_minute) != last_output_date:
    with buffer_lock:
      pending = {k: ohlcv_buffers.pop(k) for k in list(ohlcv_buffers) if is_yesterday(k, last_output_date)}
      snapshot_copy = dict(latest_snapshot)
    for minute_key, data in pending.items():
      write_files(minute_key, snapshot_copy, data)
    output_minutes.clear()
    last_output_date = extract_date(current_minute)
    last_output_minute = ""
    first_data_received = false

Step 2 - 首次数据检查：
  if not first_data_received:
    return

Step 3 - 分钟输出（含断流追赶）：
  1. 加锁 pop 已过期 minute buffer，并浅拷贝 latest_snapshot（frozen=True 安全）
  2. 锁外逐分钟写 snapshot/kline 文件（同第 2.3 节时钟线程操作）
  3. 任一输出失败则 FATAL 退出，不推进 checkpoint
  4. 每成功输出一个 minute 后：
     - output_minutes.add(minute_key)
     - last_output_minute = minute_key
     - 仅对有 OHLCVAggregate 的 symbol 更新 last_totalvol/last_totalamount（carry-forward 不更新）
     - 原子写 checkpoint
```

### 2.5 交易时段定义

**分钟口径说明**：`minute_key "0930"` 代表 `[09:30:00.000, 09:30:59.999]` 这一分钟。日股上午盘交易时间 JST 09:00-11:30，其中 `11:30` 表示 `[11:30:00, 11:30:59]`。需与下游确认：上午盘最后有效分钟是 `1129` 还是包含 `1130`。默认配置为包含 `1130`，可通过配置调整。

```python
def is_trading_minute(minute_key: str) -> bool:
    hhmm = minute_key[-4:]  # "0930", "1500" 等
    return ("0900" <= hhmm <= "1130") or ("1230" <= hhmm <= "1500")
```

| 时段 | JST 时间 | 轮询频率 | 输出行为 |
|------|---------|---------|---------|
| 盘前 | 08:00-09:00 | 1s | 不输出 |
| 上午盘 | 09:00-11:30 | 200ms | 正常输出 |
| 午休 | 11:30-12:30 | 5s | 正常输出（处理11:30前残留） |
| 下午盘 | 12:30-15:00 | 200ms | 正常输出（含15:00这一分钟） |
| 盘后 | 15:00-15:30 | 1s | 正常输出（处理15:00残留） |
| 深夜 | 15:30-08:00 | 5s | 不输出 |

---

## 3. 核心数据结构

```python
@dataclass(frozen=True)
class SnapshotRecord:
    symbol: str
    seqno: int            # 该symbol最后更新时的seqno（用于carry-forward输出）
    time: int             # 17位, JST
    rcvtime: int          # 17位, CST
    preclose: float       # 已除以decimal
    lastprice: float
    open: float           # 日内累计
    high: float           # 日内累计
    low: float            # 日内累计
    close: float
    lasttradeprice: float
    lasttradeqty: int
    totalvol: int
    totalamount: float    # 已除以decimal
    sessionid: int
    tradetype: str
    status: str           # T/P
    direction: int        # 1/0/-1
    pflag: str            # Y/N
    decimal: int
    vwap: float
    shortsellflag: int

@dataclass
class OHLCVAggregate:
    symbol: str
    open: float            # 分钟内第一条 lastprice
    high: float            # 分钟内所有 lastprice 最大值
    low: float             # 分钟内所有 lastprice 最小值
    close: float           # 分钟内最后一条 lastprice
    volume: int            # 基于上一分钟结束totalvol的差值
    amount: float          # 基于上一分钟结束totalamount的差值
    count: int             # 分钟内更新次数
    start_totalvol: int    # 参考基线（上一分钟结束时的totalvol）
    start_totalamount: float
    end_totalvol: int      # 当前分钟末尾totalvol
    end_totalamount: float # 当前分钟末尾totalamount
    any_lasttradeqty_positive: bool  # 本分钟内任意一条记录的 lasttradeqty > 0
    seqno: int             # 该symbol最后一次更新的seqno
    decimal: int           # 用于定点格式化输出

@dataclass
class FileState:
    """每个被轮询文件的独立状态"""
    offset: int            # bytes offset（rb模式）
    pending_line: bytes    # 未完成的尾部bytes
    date: str              # 文件日期 YYYYMMDD
```

---

## 4. OHLCV 计算规则

### 4.1 价格字段

| 字段 | 取值 | 说明 |
|------|------|------|
| Open | 分钟内第一条 lastprice | 非 snapshot.open（那是日内累计） |
| High | 分钟内所有 lastprice 最大值 | 非 snapshot.high |
| Low | 分钟内所有 lastprice 最小值 | 非 snapshot.low |
| Close | 分钟内最后一条 lastprice | |

### 4.2 Volume / Amount — 基于上一分钟结束累计值差分

维护跨分钟状态：`last_totalvol_by_symbol[symbol]` 记录上一个已输出分钟结束时的 totalvol。

```python
if symbol in last_totalvol_by_symbol:
    base_vol = last_totalvol_by_symbol[symbol]
    base_amt = last_totalamount_by_symbol[symbol]
else:
    # 首次出现的 symbol，基线取决于配置
    if first_seen_volume_base == "start_totalvol":
        base_vol = agg.start_totalvol
        base_amt = agg.start_totalamount
    else:  # "zero"
        base_vol = 0
        base_amt = 0.0

volume = agg.end_totalvol - base_vol
amount = agg.end_totalamount - base_amt

if volume < 0:
    log WARNING: "negative delta: symbol=%s, minute=%s, base_vol=%d, end_vol=%d" % (...)
    volume = 0
if amount < 0:
    log WARNING: "negative delta: symbol=%s, minute=%s, base_amt=%f, end_amt=%f" % (...)
    amount = 0.0
```

**边界处理**：
- 首次出现的 symbol：基线可配置为 `start_totalvol`（只算分钟内增量）或 `zero`（包含开盘前累计量）。默认 `start_totalvol`
- 如果差值为负（跨日重置、数据修正），输出 0 + WARNING
- 输出后更新基线：`last_totalvol_by_symbol[symbol] = agg.end_totalvol`（使用 end_totalvol 而非 start+volume，因为 volume 可能被修正为 0）

### 4.3 trade_flag

```python
if update_flag == "N":
    trade_flag = "N"   # 无更新的 symbol 必然无成交
else:
    trade_flag = "Y" if (volume > 0 or any_lasttradeqty_positive) else "N"
```

- `any_lasttradeqty_positive`：本分钟内该 symbol 的任意一条记录 `lasttradeqty > 0`
- update_flag=N 时 trade_flag 必然为 N
- update_flag=Y 但 volume=0 且无 lasttradeqty>0 记录时 trade_flag=N（仅有报价更新无成交）

### 4.4 单条数据边界

Open = High = Low = Close = lastprice, Volume = 0, count = 1, update_flag = "Y", trade_flag 视 lasttradeqty 而定。

---

## 5. 序列号 (seqno)

- **语义**：接收序号。每从文件成功解析一行 snapshot 数据，全局 seqno += 1
- **作用范围**：仅用于检测**本聚合器内部处理连续性**。不能证明上游（FIU接收服务、文件写入）没有丢数据。上游缺失需要结合 FIU 日志、文件行数、send buffer 监控、数据延迟监控共同判断
- **持久化**：写入 checkpoint，重启后继续递增
- **输出**：快照文件中每行携带该 symbol 最后更新时的 seqno；K线文件中每行携带该 symbol 在该分钟内最后更新的 seqno

---

## 6. 全市场输出规则

**snapshot_minute 和 kline_minute 都输出全市场 symbol**，即使某些 symbol 本分钟没有更新。

### 6.1 snapshot_minute 输出规则

遍历 `latest_snapshot` 中所有 symbol：

- **本分钟有更新**（symbol 在 ohlcv_buffer 中）：update_flag = "Y"，使用最新 snapshot 数据
- **本分钟无更新**：update_flag = "N"，沿用 latest_snapshot 中的数据

name 字段来自 code_table 关联。关联不到时 name 填空字符串，不丢弃数据。

### 6.2 kline_minute 输出规则

遍历 `latest_snapshot` 中所有 symbol：

**本分钟有更新**（symbol 在 ohlcv_buffer 中）：
- open = 分钟内第一条 lastprice
- high = 分钟内 lastprice 最大值
- low = 分钟内 lastprice 最小值
- close = 分钟内最后一条 lastprice
- volume = end_totalvol - 上一分钟结束 totalvol（差分）
- amount = end_totalamount - 上一分钟结束 totalamount（差分）
- count = 本分钟更新次数
- update_flag = "Y"
- trade_flag 根据 volume/lasttradeqty 判断

**本分钟无更新**（symbol 不在 ohlcv_buffer 中）：
- open = high = low = close = latest_snapshot.lastprice（延续补齐）
- volume = 0
- amount = 0
- count = 0
- update_flag = "N"
- trade_flag = "N"
- seqno = latest_snapshot[symbol].seqno

这样每分钟的 snapshot 和 kline 都是完整的全市场截面。下游可通过 update_flag 和 count 区分真实更新与系统补齐。

---

## 7. decimal 处理

- 以 snapshot.csv 中的 decimal 为准
- code.csv 的 decimal 用于辅助校验（获取 limitup/limitdown 的正确值），不一致时记录 WARNING
- 同一 symbol 同一交易日内 decimal 应一致，变化时记录 WARNING，以最新值为准
- 输出价格使用定点格式：`f"{value:.{decimal}f}"`，避免浮点误差

需要除以 decimal 的字段：
- snapshot: preclose, lastprice, open, high, low, close, lasttradeprice, vwap
- **totalamount**: 使用同一 decimal。此缩放规则需用实际 FIU 样本与交易所/供应商口径确认
- code: limitup, limitdown, baseprice

---

## 8. 边界条件处理

### 8.1 首次启动（盘中）

1. `first_data_received = false`, `last_output_minute = ""`, `last_output_date = ""`
2. 数据到达 → `first_data_received = true`
3. Step 1: `last_output_date` 为空 → 初始化为当前日期，return
4. Step 2: 已有数据，跳过
5. Step 3: `last_output_minute` 为空 → 输出所有超时分钟
6. INFO 日志：`"First data received at minute=%s, skipping all prior minutes for today"`

### 8.2 收盘（JST 15:00）

- 15:00 这一分钟属于交易时段（需与下游确认口径），默认正常输出
- 时钟在 15:01:05 检测到 15:00 数据超时，输出
- 盘后缓冲期（15:00-15:30）仍会输出残留分钟

### 8.3 跨日

- 时钟线程 Step 1 统一处理，数据线程不做跨日判断
- 跨日完整流程：
  1. 输出上一日所有 pending 分钟（加锁获取后锁外写文件）
  2. 关闭旧日期文件（snapshot.csv.旧日期、code.csv.旧日期）
  3. 清空 output_minutes
  4. 重置 last_totalvol_by_symbol、last_totalamount_by_symbol（新交易日起始）
  5. 初始化新日期 FileState（offset=0, pending_line=b"", date=新日期）
  6. 打开新日期文件（snapshot.csv.新日期、code.csv.新日期）
  7. last_output_date = 新日期, last_output_minute = "", first_data_received = false

### 8.4 午休（JST 11:30-12:30）

- 11:30:05 输出 11:29 分钟数据（11:30 是否输出取决于交易时段口径配置）
- 午休期间轮询频率降为 5s
- 12:30 开始正常输出

### 8.5 断流恢复

- 积压的旧分钟数据通过 Step 3 遍历自动追赶输出
- 不需要额外机制

### 8.6 程序重启恢复

从 checkpoint.json 恢复：
1. 恢复各文件 offset 和 pending_line → 继续读取 CSV
2. 恢复 current_minute → 判断重启后第一条数据是否与之前同一分钟
3. 恢复 output_minutes → 跳过已输出的分钟
4. 恢复 last_seqno → 继续递增
5. 恢复 latest_snapshot → 读取最近一个 snapshot_minute 文件重建全市场状态
6. 恢复 last_totalvol_by_symbol / last_totalamount_by_symbol → 从 checkpoint 中完整恢复，确保 volume 差分基线正确

---

## 9. Checkpoint

### 9.1 格式

```json
{
  "version": 3,
  "date": "20260520",
  "last_seqno": 98765,
  "output_minutes": ["202605200930", "202605200931"],
  "last_output_minute": "202605200932",
  "current_minute": "202605200933",
  "last_output_date": "20260520",
  "first_data_received": true,
  "last_update_time": "2026-05-20T09:33:15+09:00",
  "files": {
    "snapshot": {
      "offset": 12345678,
      "pending_line_base64": ""
    },
    "code": {
      "offset": 234567,
      "pending_line_base64": ""
    },
    "order": {
      "offset": 0,
      "pending_line_base64": ""
    }
  },
  "last_totalvol_by_symbol": {
    "1301": 78300,
    "1305": 74450
  },
  "last_totalamount_by_symbol": {
    "1301": 350251000.00,
    "1305": 308198040.00
  }
}
```

- 所有时间键使用 `YYYYMMDDHHMM` 格式（12位）
- `pending_line_base64`: 未完成的尾部 bytes（base64 编码），重启后恢复到 BinaryLineAssembler
- 每个文件独立维护 offset，支持 snapshot/code/order 各自的轮询进度
- `last_totalvol_by_symbol` / `last_totalamount_by_symbol` **完整保存所有已出现 symbol 的末尾值**，确保重启后 volume 差分基线正确。日股约 4000 只 symbol，JSON 大小可接受

### 9.2 写入顺序

```
1. 写 snapshot_minute_{date}_{HHMM}.csv.tmp → rename
2. 写 kline_minute_{date}_{HHMM}.csv.tmp → rename
3. 写 checkpoint.json.tmp → rename
```

保证崩溃恢复的幂等性：
- 步骤 1 或 2 崩溃：checkpoint 未更新，重启后重新输出该分钟
- 步骤 3 崩溃：数据文件已落盘，重启后可能重复输出，下游通过 seqno 去重

### 9.3 output_minutes 生命周期

- 一个交易日，跨日时清空
- 用途：监控（向下游报告已输出哪些分钟）
- 内部去重依赖 buffer key 的存在性

---

## 10. 输出文件

### 10.1 目录与命名

- 目录：`{output_dir}/{type}/{YYYY}/{YYYYMMDD}/`
- 快照：`{output_dir}/snapshot/{YYYY}/{YYYYMMDD}/snapshot_minute_{YYYYMMDD}_{HHMM}.csv`
- K线：`{output_dir}/kline/{YYYY}/{YYYYMMDD}/kline_minute_{YYYYMMDD}_{HHMM}.csv`
- 挂单：`{output_dir}/order/{YYYY}/{YYYYMMDD}/order_minute_{YYYYMMDD}_{HHMM}.csv`
- 示例：`output/snapshot/2026/20260520/snapshot_minute_20260520_0930.csv`

### 10.2 快照文件格式

```csv
seqno,symbol,name,time,preclose,lastprice,open,high,low,close,totalvol,totalamount,status,direction,pflag,vwap,decimal,update_flag
150,1301,極洋,20260520093000999,4435.00,4500.00,...,Y
148,1305,iFTPX年1,20260520093000999,...,N
```

### 10.3 K线文件格式

```csv
seqno,symbol,name,open,high,low,close,volume,amount,count,trade_flag,update_flag
150,1301,極洋,4455.00,4510.00,4435.00,4500.00,1500,675000.00,23,Y,Y
148,1305,iFTPX年1,4123.00,4154.00,4110.00,4123.00,0,0.00,0,N,N
```

`update_flag` 必须保留，用于区分两种 volume=0 的不同含义：
- update_flag=Y, volume=0, count>0：本分钟有报价更新但无成交
- update_flag=N, volume=0, count=0：本分钟完全没有更新，由系统延续补齐

### 10.4 原子写入

所有输出文件和 checkpoint 使用 `.tmp` + `rename` 策略。

---

## 11. 文件读取策略

### 11.1 BinaryLineAssembler（核心读取机制）

**使用 `rb` 模式读取文件，按 bytes 操作，不使用文本模式的 readline()。**

```
1. 以 rb 模式打开 snapshot.csv.YYYYMMDD
2. seek 到 checkpoint 记录的 offset
3. 读取一个 chunk（如 64KB）
4. 如果有 pending_line，将 chunk 拼接到 pending_line 后
5. 按 b"\n" 切分完整行
6. 最后一个 \n 之后的不完整 bytes 存入 pending_line
7. checkpoint offset 只推进到最后一个完整 \n 之后的位置
8. 每条完整行 decode(encoding) 后交给 CsvRowParser
```

原因：checkpoint 是 byte offset，文本模式的 readline() 在编码转换、换行符处理、中文 name 等场景下可能导致 offset 不一致。使用 bytes 模式可以精确定位和恢复。

### 11.2 多文件 FileTailer

每个文件类型维护独立的 `FileState`：

```python
file_states: dict[str, FileState] = {
    "snapshot": FileState(offset=0, pending_line=b"", date=""),
    "code": FileState(offset=0, pending_line=b"", date=""),
    "order": FileState(offset=0, pending_line=b"", date=""),
}
```

轮询时按文件类型分别读取，各自维护独立的 offset 和 pending_line。

### 11.3 轮询频率

| 时段 (JST) | 频率 |
|------------|------|
| 08:00-09:00 | 1s |
| 09:00-11:30 | 200ms |
| 11:30-12:30 | 5s |
| 12:30-15:30 | 200ms |
| 15:30-08:00 | 5s |

### 11.4 code.csv 处理

- 启动时全量加载到 `code_table`
- 运行时每 30 秒检查文件变化（比较文件大小 vs code FileState.offset），有新增行则追加解析
- 只加载最新一行（同一 symbol 多行时取最后一行）
- code.csv 使用独立的 FileState，offset 持久化到 checkpoint

---

## 12. 重启恢复 latest_snapshot

盘中重启后内存中的 `latest_snapshot` 为空，会影响：
1. 快照文件无法输出全市场最新状态
2. `update_flag=N` 的 symbol 无法沿用上次状态

**恢复策略**（优先级从高到低）：

1. **优先读取最近一个已完成的 snapshot_minute 文件**（由 checkpoint 的 `last_output_minute` 定位），恢复 `latest_snapshot` 和 `last_totalvol_by_symbol` / `last_totalamount_by_symbol`。这是全量 CSV 文件（全市场 symbol），加载一次即可恢复完整状态。**只读取正式文件 `snapshot_minute_YYYYMMDD_HHMM.csv`，不读取 `.tmp` 文件**。如果正式文件存在但行数异常（空文件、列数不对），fallback 到 checkpoint 或空状态并打 ERROR
2. **如果 snapshot_minute 文件不存在**（被清理或首次启动），再使用 checkpoint 中的 `last_totalvol_by_symbol` / `last_totalamount_by_symbol`
3. **如果两者都不存在**，则从空状态启动

注意：`SnapshotRecord` 使用 `frozen=True`（不可变），因此 `snapshot_copy = dict(latest_snapshot)` 是安全的浅拷贝，无需 deepcopy。

---

## 13. 数据校验

### 13.1 字段校验规则

| 字段 | 规则 | 处理 |
|------|------|------|
| symbol | 非空 | 为空 → 丢弃整行 (ERROR) |
| time | 17位数字 | 格式非法 → 丢弃 (ERROR) |
| lastprice | 非负数值 | 为空 → 视为0，WARNING |
| totalvol | 非负整数 | 非递增 → WARNING，接受 |
| decimal | 0-6 整数 | 超范围 → 视为0，WARNING |
| status | T 或 P | 其他值 → WARNING，接受 |
| 价格字段 | ≤limitup 且 ≥limitdown | 超范围 → WARNING，接受 |

### 13.2 错误分级

| 级别 | 含义 | 处理 |
|------|------|------|
| WARN | 数据异常但不影响流程 | 记录日志，继续 |
| ERROR | 行级错误 | 丢弃该行，记录日志 |
| FATAL | 文件级错误 | 暂停处理，等待人工介入 |

### 13.3 错误日志

- 路径：`errors/{YYYYMMDD}_errors.log`
- 日志轮转：单文件最大 100MB，保留 5 个备份
- WARNING 日志包含具体数值：`"negative delta: symbol=%s, minute=%s, start_vol=%d, end_vol=%d"`

---

## 14. 监控

| 指标 | 说明 |
|------|------|
| 数据延迟 | 最新 time 与当前 JST 时间的差值 |
| 断流检测 | 超过 N 秒无新数据时 WARNING |
| 处理速度 | 每秒处理的记录数 |
| 输出完整性 | 每分钟快照中的 symbol 数量 vs code_table 总数 |
| 上游健康 | 结合 FIU 日志 send buffer、文件行数增长判断上游状态 |

---

## 15. 配置参数

```ini
[input]
csv_dir = /path/to/log
poll_interval_ms = 200
idle_poll_interval_ms = 5000
buffer_poll_interval_ms = 1000
chunk_size_bytes = 65536
file_encoding = utf-8

[output]
output_dir = /path/to/output
format = csv              # csv | parquet | both
enable_kline = true
enable_full_snapshot = true  # false时仅输出delta snapshot
enable_full_kline = true    # 当前业务要求全市场输出，生产环境不建议关闭

[aggregation]
first_seen_volume_base = start_totalvol  # start_totalvol | zero
# start_totalvol: 首次出现symbol的volume=分钟内差值
# zero: 首次出现symbol的volume=分钟末尾totalvol（包含开盘前累计）

[timezone]
exchange_tz = Asia/Tokyo
local_tz = Asia/Shanghai

[session]
morning_open = 0900       # JST
morning_close = 1130      # JST (含1130这一分钟)
afternoon_open = 1230     # JST
afternoon_close = 1500    # JST (含1500这一分钟)
pre_market_start = 0800   # JST
post_market_end = 1530    # JST

[recovery]
checkpoint_file = checkpoint.json
output_delay_sec = 5
code_refresh_sec = 30

[logging]
error_log_dir = errors/
max_file_size_mb = 100
max_backup_count = 5
log_level = INFO
```

---

## 16. 扩展预留

- **order.csv**：`OrderFileReader` 接口已预留，未来可接入分钟级买卖盘输出
- **Parquet 输出**：输出格式配置支持 parquet，实现时需攒齐所有记录后一次性写入
- **多市场**：当前仅日股，但列映射表和交易时段配置化设计支持扩展到其他市场
- **delta snapshot/kline**：当 `enable_full_snapshot = false` 或 `enable_full_kline = false` 时仅输出本分钟有更新的 symbol（当前业务要求全市场输出，生产环境不建议关闭）

---

## 17. 守护与自动重启策略

程序遇到 FATAL 时必须立即退出，禁止继续推进 checkpoint。生产环境通过 systemd/supervisord 守护。

### 17.1 退出码分类

| 退出码 | 含义 | 是否允许自动重启 |
|--------|------|----------------|
| 1 | 可恢复 FATAL（输出失败、临时 IO 异常、.tmp 残留） | 允许 |
| 2 | 配置错误 | 不允许 |
| 3 | checkpoint 损坏 | 不允许 |
| 4 | 输入文件结构严重异常（file size < offset、schema 长期不符） | 不允许 |

程序内部应根据 FATAL 类型使用对应退出码退出（`sys.exit(1/2/3/4)`），确保 systemd 能正确区分可恢复与不可恢复故障。不可恢复 FATAL 不应统一返回 exit code 1。

### 17.2 自动重启规则

- 可恢复 FATAL（exit code 1）允许守护进程自动重启，重启后从 checkpoint 恢复
- 不可恢复 FATAL（exit code 2/3/4）应告警并等待人工处理，不无限重启
- 自动重启必须设置 backoff 和最大重启次数
- 每次 FATAL 需记录错误类型、minute_key、file offset、checkpoint 状态

### 17.3 systemd 服务配置示例

```ini
[Unit]
Description=FIU Minute Bar Generator
After=network.target

[Service]
Type=simple
User=prod
WorkingDirectory=/home/prod/fiu_minute_generator
ExecStart=/home/prod/miniconda3/envs/py312/bin/python main.py --config config.ini

Restart=on-failure
RestartPreventExitStatus=2 3 4
RestartSec=30

StartLimitInterval=600
StartLimitBurst=5

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

- 10 分钟内最多重启 5 次，超过后停止并等待人工介入
- 使用 `journal` 输出（兼容 CentOS 7），业务日志由程序自身通过 loguru/logging 写文件
- 如部署在较新 systemd 环境（Rocky 8/9+），可改用 `StartLimitIntervalSec` 和 `StandardOutput=append:/path`
- 生产环境应结合监控系统对 exit code 2/3/4、连续重启失败、StartLimit 触发进行告警

---

## 18. 开发验收测试

正式开发后，至少要验证以下 case：

**核心主链路：**

1. 文件最后一行不完整，下一轮 append 后能正确拼接
2. 程序盘中启动，能从当前分钟开始输出
3. 某 symbol 本分钟无更新，kline 仍输出 update_flag=N
4. 某 symbol 本分钟有更新但 volume=0，输出 update_flag=Y、count>0
5. 程序输出 snapshot 成功但 kline 失败时 FATAL，不继续推进 checkpoint
6. 重启后从最近 snapshot_minute 恢复 latest_snapshot 和成交量基线
7. 多个过期分钟积压时能逐分钟追赶输出，每分钟输出后立即写 checkpoint
8. 跨日后 FileState offset 清零，pending_line 清空，成交量基线重置
9. code.csv 新增或更新 symbol name 后，下一个输出文件能反映
10. first_seen_volume_base=start_totalvol 与 zero 两种口径输出符合预期

**生产边界与异常：**

11. snapshot.csv 长时间无新增时触发断流 WARNING，恢复 append 后继续处理，不误生成空分钟文件
12. 非交易时段（08:30、11:45、15:40）出现 snapshot 数据时按规则处理，不影响正常交易分钟输出
13. 11:29、11:30、12:30、14:59、15:00 等交易边界分钟输出符合配置，不多不少
14. 最近 snapshot_minute 恢复文件为空、缺列、仅有 .tmp 或不存在时，正确 fallback 到 checkpoint 或空状态并打 ERROR/WARNING
15. decimal 缺失（默认2）、超范围（视为0+WARNING）、盘中变化（WARNING+以最新为准）、与 code.csv 不一致（WARNING）
16. totalvol/totalamount 负差值时输出 0、记录 negative delta WARNING、后续基线更新为 end_totalvol、下一分钟不连锁错误
17. code.csv 启动时不存在或延迟出现时程序不中断，先输出 name=""，code.csv 出现后自动加载补全 name
18. checkpoint.json 不存在时允许空状态启动；内容损坏或版本不匹配时按配置拒绝启动或人工确认后重置，禁止静默使用异常 checkpoint

**性能与一致性（生产上线验收）：**

19. 大文件/高频 append 压测：模拟全市场高频 snapshot 持续 append 1-2 小时，验证处理速度稳定、CPU/内存可控、每分钟输出不超过预期延迟、checkpoint 写入不膨胀变慢
20. 输出文件原子性与幂等恢复：人为中断程序（snapshot 写完 kline 未写完 / snapshot+kline 写完 checkpoint 未写完 / .tmp 残留），验证重启后不读取 .tmp、可重新生成同一分钟正式文件、不出现半个正式文件、下游看到的正式文件始终完整
21. 输出完整性与 symbol 数量校验：full snapshot/full kline 行数等于 latest_snapshot symbol 数、code_table 有但 latest_snapshot 未出现的 symbol 不强行输出、latest_snapshot 有但 code_table 未匹配的 symbol name=""

**数据质量与鲁棒性：**

22. CSV 行格式异常：snapshot.csv 中出现空行、字段非法、列数不足或超出时，异常行被丢弃并记录 ERROR，不影响后续正常行解析，offset 正常推进
23. 文件截断/轮转异常：运行中发现 snapshot.csv 文件 size 小于 checkpoint offset 时能检测到，按策略 FATAL 或重新从 0 读取，不静默 seek 到错误位置
24. 同一 symbol 数据乱序或重复：同一 symbol 出现 time 倒退、重复 time、重复行时记录 WARNING，不导致 OHLCV 崩溃，close 按当前处理顺序最后一条，seqno 保持连续；已输出分钟不自动回写，需修正时走离线补算流程
