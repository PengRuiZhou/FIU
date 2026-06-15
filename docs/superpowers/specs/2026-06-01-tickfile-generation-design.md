# Tickfile Generation Design Spec

> **Version**: Round 20 (2026-06-02) | **Status**: Ready for Implementation

## 1. Overview

基于 minute_bar 引擎实时生成的 snapshot 和 order 分钟数据，在 flush 管线中原位生成 tickfile 格式的 CSV 文件。支持 live 和 replay 两种模式。

## 2. 输入

每次分钟 flush 时，`_write_minute_files` 已有以下数据：

| 数据 | 来源 | 内容 |
|---|---|---|
| `snapshot_copy` | `SharedState.latest_snapshot` 或 `_snapshot_at_minute_end` | 该分钟所有 symbol 的最新 snapshot（含 carry-forward） |
| `ohlcv_data` | `SharedState.ohlcv_buffers[minute_key]` | 该分钟有实际成交的 symbol 集合（tickfile selection 不直接使用，仅 `raw_records` 判断即可） |
| `raw_records` | `SharedState.raw_snapshot_buffers[minute_key]` | 该分钟每个 symbol 的原始 snapshot 记录列表 |
| `order_records` | `SharedState.raw_order_buffers[minute_key]` | 该分钟所有原始 order 记录列表 |
| `code_table` | `CodeTable._table` | 每 30s 刷新的证券基础信息（含 limitup/limitdown） |
| `latest_order_by_symbol` | 新增于 `SharedState` | 每个 symbol 最近的 order 记录（用于 carry-forward） |

**数据类型说明**：`SnapshotRecord` 中价格和金额字段（preclose、lastprice、open 等）存储原始整数值 as Python `float`（如 `443500.0` 表示 decimal=2 下真实价格 4435.00）；`time` 和 `rcvtime` 字段存储为 Python `int`（17 位时间戳，如 `20260602090013000`）。**Division by `10^decimal` must always be applied at output time (for price/amount fields only).** `OrderRecord` 同理。

## 3. 输出

### 3.1 文件路径

```
output/tickfile/{YYYY}/{YYYYMMDD}/tickfile_{YYYYMMDD}.csv
```

全天一个文件，每分钟 flush 时追加写入（append mode）。输出文件使用 `encoding='utf-8', newline=''`（与现有 snapshot/order 文件一致）。

**首分钟写入策略**：对 daily tickfile 的第一次写入，使用 `atomic_write` 模式（写 `.tmp` 后 `os.replace`），包含 header + 首分钟数据行。后续分钟的写入使用 append mode + `f.flush()` + `os.fsync()`，确保每分钟数据落盘。

### 3.2 列定义（65 列）

```
InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,
OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,
SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,
BidPrice1,BidVolume1,AskPrice1,AskVolume1,
BidPrice2,BidVolume2,AskPrice2,AskVolume2,
BidPrice3,BidVolume3,AskPrice3,AskVolume3,
BidPrice4,BidVolume4,AskPrice4,AskVolume4,
BidPrice5,BidVolume5,AskPrice5,AskVolume5,
BidPrice6,BidVolume6,AskPrice6,AskVolume6,
BidPrice7,BidVolume7,AskPrice7,AskVolume7,
BidPrice8,BidVolume8,AskPrice8,AskVolume8,
BidPrice9,BidVolume9,AskPrice9,AskVolume9,
BidPrice10,BidVolume10,AskPrice10,AskVolume10,
ActionDay,Type,Seqno,LocalTime,
IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume
```

**列数验证**：`TICKFILE_HEADER.split(',')` 必须等于 65，单元测试中显式断言。

**完整常量定义**（可直接 copy）：
```python
TICKFILE_HEADER = "InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,BidPrice1,BidVolume1,AskPrice1,AskVolume1,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,BidPrice6,BidVolume6,AskPrice6,AskVolume6,BidPrice7,BidVolume7,AskPrice7,AskVolume7,BidPrice8,BidVolume8,AskPrice8,AskVolume8,BidPrice9,BidVolume9,AskPrice9,AskVolume9,BidPrice10,BidVolume10,AskPrice10,AskVolume10,ActionDay,Type,Seqno,LocalTime,IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume"
```

### 3.3 字段映射表

| # | 输出列 | 来源 | 转换规则 |
| *`#` 为 0-based 列索引，对应 CSV 解析时 `line.split(',')[N]`* | | | |
|---|---|---|---|
| 0 | InstrumentID | symbol（并集） | 直接使用 `select_tickfile_records` 返回元组的 `symbol` 字段（snapshot 和 order 的 symbol 始终一致） |
| 1 | TradingDay | snapshot.time (JST) | 取前 8 位 YYYYMMDD；无 snapshot 时从 order.time (JST) 取 |
| 2 | LastPrice | snapshot.lastprice | `lastprice / 10^decimal`；**0→NA**（未交易的 symbol） |
| 3 | PreSettlementPrice | 无 | NA |
| 4 | PreClosePrice | snapshot.preclose | `preclose / 10^decimal`；**0→NA**（除零保护：IntraDailyReturn 在 PreClosePrice=0 时也输出 NA） |
| 5 | PreOpenInterest | 无 | NA |
| 6 | OpenPrice | snapshot.open | `open / 10^decimal`，0→NA |
| 7 | HighestPrice | snapshot.high | `high / 10^decimal`，0→NA |
| 8 | LowestPrice | snapshot.low | `low / 10^decimal`，0→NA |
| 9 | Volume | snapshot.totalvol | 输出时 `int(snapshot.totalvol)` 转为 int 格式（defensive cast：`totalvol` 在 `SnapshotRecord` 中已为 `int`，`int()` 确保未来类型变更时无 `.0` 后缀），累计成交量；snapshot=None 时输出 NA |
| 10 | Turnover | snapshot.totalamount | **`totalamount / 10^decimal`**（累计成交额，需 decimal 转换）。`totalamount` 在 `SnapshotRecord` 中类型为 `float`（非 `int`），除法结果自然为 `float`，无需额外 `int()` cast，输出保留 Python float 表示（如 `3502510.0`）。`totalamount=0` 输出 `0.0`（与 Volume 一致：零成交额是合法值，不做 0→NA）。`snapshot=None` 时输出 NA（此时 decimal 不适用） |
| 11 | OpenInterest | 无 | NA |
| 12 | ClosePrice | snapshot.close | `close / 10^decimal`，0→NA |
| 13 | SettlementPrice | 无 | NA |
| 14 | UpperLimitPrice | code_table (30s 刷新) | `code.limitup / 10^code.decimal`；code 表无此 symbol 或 limitup=0→NA |
| 15 | LowerLimitPrice | code_table (30s 刷新) | `code.limitdown / 10^code.decimal`；code 表无此 symbol 或 limitdown=0→NA |
| 16 | UpdateTime | snapshot.rcvtime (CST) | `str(rcvtime)[:8] + " " + str(rcvtime)[8:10] + ":" + str(rcvtime)[10:12] + ":00"`；无 snapshot 时从 order.rcvtime (CST) 取。**秒数有意丢弃**（始终 `:00`），因为 tickfile 按分钟粒度输出。**长度防护**：`len(str(rcvtime)) < 12` 时输出 NA 并 log WARNING（异常 rcvtime 值。正常值为 17 位 YYYYMMDDHHMMSSMMM，< 12 意味着连小时和分钟都不完整） |
| 17 | BidPrice1 | order (carry-forward) | `bidprice / 10^decimal`；**0→NA**（无报价） |
| 18 | BidVolume1 | order (carry-forward) | 直接使用，输出时 `int(round(bidsize))` 转为 int 格式；order=None 时输出 NA。**前提**：`ParsedOrder.bidsize` 原始类型为 `int`，经 `build_order_record` 转为 `float`。JP equity 单档挂单量不会超过 2^53（~9×10^15），`int→float→int(round())` 链路在业务值域内精度无损 |
| 19 | AskPrice1 | order (carry-forward) | `askprice / 10^decimal`；**0→NA**（无报价） |
| 20 | AskVolume1 | order (carry-forward) | 直接使用，输出时 `int(round(asksize))` 转为 int 格式；order=None 时输出 NA。同 BidVolume1 精度前提 |
| 21 | BidPrice2 | 无 | NA |
| 22 | BidVolume2 | 无 | NA |
| 23 | AskPrice2 | 无 | NA |
| 24 | AskVolume2 | 无 | NA |
| 25 | BidPrice3 | 无 | NA |
| 26 | BidVolume3 | 无 | NA |
| 27 | AskPrice3 | 无 | NA |
| 28 | AskVolume3 | 无 | NA |
| 29 | BidPrice4 | 无 | NA |
| 30 | BidVolume4 | 无 | NA |
| 31 | AskPrice4 | 无 | NA |
| 32 | AskVolume4 | 无 | NA |
| 33 | BidPrice5 | 无 | NA |
| 34 | BidVolume5 | 无 | NA |
| 35 | AskPrice5 | 无 | NA |
| 36 | AskVolume5 | 无 | NA |
| 37 | BidPrice6 | 无 | NA |
| 38 | BidVolume6 | 无 | NA |
| 39 | AskPrice6 | 无 | NA |
| 40 | AskVolume6 | 无 | NA |
| 41 | BidPrice7 | 无 | NA |
| 42 | BidVolume7 | 无 | NA |
| 43 | AskPrice7 | 无 | NA |
| 44 | AskVolume7 | 无 | NA |
| 45 | BidPrice8 | 无 | NA |
| 46 | BidVolume8 | 无 | NA |
| 47 | AskPrice8 | 无 | NA |
| 48 | AskVolume8 | 无 | NA |
| 49 | BidPrice9 | 无 | NA |
| 50 | BidVolume9 | 无 | NA |
| 51 | AskPrice9 | 无 | NA |
| 52 | AskVolume9 | 无 | NA |
| 53 | BidPrice10 | 无 | NA |
| 54 | BidVolume10 | 无 | NA |
| 55 | AskPrice10 | 无 | NA |
| 56 | AskVolume10 | 无 | NA |

> **2-10 档 NA 说明**：BidPrice2~10、BidVolume2~10、AskPrice2~10、AskVolume2~10 均无数据源（order 仅提供一档买卖盘），price 和 volume 字段统一输出 NA。这与 1 档的 `0→NA` 规则（仅限 price 字段）不同：1 档的 volume=0 是合法值输出 `0`，而 2-10 档的 volume 输出 NA 是因为"无数据源"而非"值为 0"。

| 57 | ActionDay | snapshot.time (JST) | 取前 8 位 YYYYMMDD；无 snapshot 时从 order.time (JST) 取 |
| 58 | Type | 固定值 | 69（下游系统约定标识值，对应 ASCII 字符 'E' = Equity，输出为整数 69 而非字符 'E'） |
| 59 | Seqno | 分钟顺序 | 同分钟所有 symbol 共享，按分钟全局递增 |
| 60 | LocalTime | snapshot.time (JST) | 使用 `clock.py` 的 `parse_17digit_time` 转换为 `datetime`，**必须使用** `.strftime('%Y-%m-%d %H:%M:%S.%f')`（不要用 `str()` 或 `isoformat()`，它们会包含 `+09:00` 时区后缀）；毫秒（3 位）→ 微秒（6 位）补零；无 snapshot 时从 order.time 取 |
| 61 | IntraDailyReturn | 计算 | `(LastPrice - PreClosePrice) / PreClosePrice`（使用已转换的 float 值，与下游使用 float 价格值计算 IntraDailyReturn 的场景一致），double 精度，输出时保留 Python 默认浮点表示（如 `0.0`、`0.002254...`，不做额外格式化），preClose=0→NA；**注意**：结果为 `-0.0` 时归一化为 `0.0`（使用 `result = result + 0.0`，利用 IEEE 754 下 `-0.0 + 0.0 == 0.0`） |
| 62 | IsShortRestricted | snapshot.shortsellflag | 1→"Y"，0→"N"；其他值→NA；无 snapshot→NA。**注意**：`ParsedSnapshot.shortsellflag` 默认值为 0（csv_parser 中字段缺失时），因此缺失时输出 "N" 而非 NA（JP equity 始终包含此字段，其他市场可能需关注） |
| 63 | OpenAuctionVolume | 无 | NA |
| 64 | CloseAuctionVolume | 无 | NA |

**NaN 表示**：输出字符串 `NA`（需求要求"缺失数据填NA"）。CSV 中表现为 `NA` 字面量，例如 `,,` 不会出现，而是 `,NA,`。映射表中"NaN"指 Python 内部逻辑值（`float('nan')`），在 CSV 输出时统一转为字符串 `NA`。

**Decimal 规则**：
- 所有 snapshot 价格字段统一使用 `snapshot.decimal`
- Order 的 bid/ask 价格使用 `order.decimal`（carry-forward 时保留原始 order 的 decimal）
- Code table 的 limitup/limitdown 使用 `code_entry.decimal`。`ParsedCode.limitup`/`limitdown` 为 `int` 类型，`int / 10^int` 在 Python 中产生 `float`（64-bit double），精度与 `float / 10^int` 一致，无需特殊处理
- **如果 `snapshot.decimal != code_entry.decimal`**：log WARNING，limitup/limitdown 仍用 `code_entry.decimal` 转换（保证绝对值正确），但这意味着同一行中 price 与 limit 可能量级不同，下游需知悉
- **如果 `snapshot.decimal != order.decimal`**：log WARNING（限流：同一 symbol 对每分钟最多 1 条），snapshot 和 order 各自使用自己的 decimal 转换（不做统一化）。这在理论上不应发生（同一 symbol 的 decimal 由交易所固定），但 carry-forward order 可能来自不同 decimal 版本
- `decimal=0` 时 `10^0=1`，即直接使用原始值，不做缩放
- **`decimal <= 0` 防护**：如果 `decimal < 0`（数据异常），使用 `10^0=1`（不做缩放），与现有 `OHLCVAggregate.update()` 中 `10 ** record.decimal if record.decimal > 0 else 1` 的行为一致
- **0→NA 规则与 decimal 无关**：无论 decimal 值如何，原始整数值为 0 的价格字段（LastPrice、OpenPrice、HighPrice、LowPrice、ClosePrice、PreClosePrice、BidPrice1、AskPrice1）均输出 NA。原始值 0 代表"无报价/未交易"，而非真实价格 0
- **Volume 字段不做 0→NA 转换**：Volume（totalvol）、BidVolume1（bidsize）、AskVolume1（asksize）等量字段直接使用原始值。原始值 0 是合法值（代表无成交量/无挂单量），输出 `0` 而非 NA

**时间字段说明**：
- `TradingDay`/`ActionDay` 使用 JST 日期（从 `time` 字段），`UpdateTime` 使用 CST（从 `rcvtime` 字段）。JST 与 CST 差 1 小时，因此同一行中 TradingDay 的日期可能与 UpdateTime 的日期不同（JST 跨日时）。这是设计如此，TradingDay 代表交易所交易日，UpdateTime 代表系统接收时间。
- `totalamount` (Turnover) 为累计成交额，原始整数存储，需 `/10^decimal` 转换为真实金额（与现有 kline writer 中 amount 字段的 decimal 处理一致）。注意 tickfile Turnover 使用累计 `totalamount / 10^decimal`，与 kline amount（增量 `totalamount delta / 10^decimal`）语义不同但转换规则一致。

## 4. 数据选取规则

对每个分钟的每个 symbol，分别从 snapshot 和 order 中选取最佳记录后拼接。

### 4.1 Snapshot 选取

1. 如果 `raw_records[symbol]` 非空：选其中 **time 最早** 的记录（这些是该分钟内实际到达的 snapshot 数据）。**Tie-breaking**：当多条记录 `time` 相同时，取 `rcvtime` 最小者（与 order 选取的 `(time, rcvtime)` 元组比较模式一致）
2. 否则，使用 `snapshot_copy[symbol]`（carry-forward：前一分钟的最新 snapshot 状态）
3. 如果 snapshot_copy 中也没有该 symbol：snap = None

> **注意**：`raw_records` 中的记录包含该分钟内所有 snapshot 更新（含无交易的心跳更新）。选取最早记录是需求指定的行为——捕获该分钟的"快照起点"状态。

> **INVARIANT — update_flag 等价性**：`raw_records[symbol]` 仅包含当前分钟的实时交易所数据（等价于 `update_flag=Y`）。此不变量由 `SharedState.process_snapshot` 保证——仅实时数据进入 `raw_snapshot_buffers`，carry-forward 数据进入 `latest_snapshot`/`_snapshot_at_minute_end`。需求 A9 的 "优先 Y → fallback N" 规则通过两步选择（raw_records 优先 → snapshot_copy fallback）隐式实现。**如果未来修改 `raw_records` 的填充逻辑包含 carry-forward 数据，此等价性将破坏，选择逻辑需重新审视。**

### 4.2 Order 选取

1. 从 `order_records` 中按 symbol 分组，每组选 **time 最早** 的记录
2. 如果当前分钟没有该 symbol 的 order，使用 `latest_order_by_symbol[symbol]`（carry-forward，保留原始 time 和 decimal）
3. 如果 carry-forward 也没有：order = None

> **Order carry-forward 无过期限制**：如果一个 symbol 连续 30 分钟没有 order 数据，第 30 分钟仍使用 30 分钟前的 order。这是需求规定的行为。下游可根据 order.time 与当前分钟时间的差距判断数据新鲜度（tickfile 中 order 的时间未被修改）。

### 4.3 输出条件

- **Symbol 并集构建**：`all_symbols = set(raw_records.keys()) | set(snapshot_copy.keys()) | set(order_by_symbol.keys()) | set(latest_order_by_symbol.keys())`，对每个 symbol 按规则选取 snapshot 和 order 后过滤掉双 None 的组合
- snapshot 和 order **都为 None** 时，该 symbol 该分钟**不输出**
- 至少有一个时输出一行，缺失部分的字段填 NA
- **Snapshot 为 None、Order 存在时**：TradingDay/ActionDay/UpdateTime/LocalTime 从 `order.time`/`order.rcvtime` fallback 派生（见字段映射表中各字段的"转换规则"列，标注"无 snapshot 时从 order 取"的字段），其余 snapshot-only 字段填 NA

### 4.4 排序

- 同一分钟内按 **symbol 升序** 排列
- 分钟间按 **HHMM 升序**（由 flush 顺序自然保证）

### 4.5 Seqno 分配

- `_tickfile_seqno` 计数器，**每次 flush 一个分钟前 +1**（先递增再赋值：`_tickfile_seqno += 1; seqno = _tickfile_seqno`）
- 同分钟所有 symbol 共享相同 seqno
- **Live 模式跨日重置**：在 `_step1_cross_day_check` 中，**在昨天分钟的 `_write_minute_files` 循环之后、`_write_checkpoint` 之前**（flusher.py ~L124-126 之间）执行 `_tickfile_seqno = 0`
- **Replay 模式**：`_tickfile_seqno` 在 `ReplayEngine.__init__` 中初始化为 0，replay 只处理单日无需跨日重置
- **Seqno 不连续**：如果某分钟无任何 snapshot/order 数据（完全空闲），该分钟不会触发 tickfile 写入，seqno 跳过该数字。Seqno 表示"第 N 个有数据的分钟"，不是"自开盘以来第 N 分钟"

## 5. 架构

### 5.1 新增模块

**`src/minute_bar/tickfile.py`**：纯逻辑，无 I/O

| 函数 | 签名 | 职责 |
|---|---|---|
| `build_tickfile_row` | `(snapshot: Optional[SnapshotRecord], order: Optional[OrderRecord], seqno: int, code_table_getter: Optional[Callable[[str], Optional[ParsedCode]]]) -> str` | 构建 65 列 CSV 行。当 `code_table_getter` 为 `None` 时（无 code table 配置），UpperLimitPrice 和 LowerLimitPrice 输出 NA |
| `select_tickfile_records` | `(raw_records: Dict[str, List[SnapshotRecord]], snapshot_copy: Dict[str, SnapshotRecord], orders: List[OrderRecord], latest_order_by_symbol: Dict[str, OrderRecord]) -> List[Tuple[str, Optional[SnapshotRecord], Optional[OrderRecord]]]` | 按 4.x 规则选取每 symbol 最佳记录，返回 `(symbol, snap, order)` 元组列表，按 symbol 排序 |
| `TICKFILE_HEADER` | `str` | 65 列 header 常量 |

**`src/minute_bar/writer.py`** 新增函数：

| 函数 | 职责 |
|---|---|
| `get_tickfile_path(output_dir: str, minute_key: str) -> str` | 返回 `output/tickfile/YYYY/YYYYMMDD/tickfile_YYYYMMDD.csv` |
| `write_tickfile_rows(output_dir: str, minute_key: str, selected: List[Tuple[str, Optional[SnapshotRecord], Optional[OrderRecord]]], seqno: int, code_table_getter: Optional[Callable] = None) -> None` | 遍历 `selected` 调用 `build_tickfile_row` 生成 CSV 行。

**写入逻辑**：**首次写入检测**：`not os.path.exists(path)` 时执行首次写入路径（`atomic_write`），包含 header + 数据行；否则验证已有文件 header 后 append。

**Append 路径**：使用 `encoding='utf-8', newline=''`（与现有 `append_snapshot_records` 一致），`f.flush()` + `os.fsync()` 确保落盘。**文件打开模式**：截断检查需读取文件末尾，因此先用 `'r'` 模式读取验证末行，然后在同一 `_get_write_lock` scope 内以 `'a'` 模式追加写入（不可用 `'r+'` 以避免覆盖已有数据的风险）。两步操作在同一个 write lock scope 内完成，防止 TOCTOU。

**异常处理**：每个 `build_tickfile_row` 调用 wrap 在 `try/except` 中，异常时 log ERROR 含完整 traceback（`logger.error("...", exc_info=True)`，含 symbol、seqno、exception），跳过该 symbol 继续处理。`build_tickfile_row` 是纯逻辑函数，异常意味着代码 bug 而非数据问题，ERROR 级别确保不被日志系统限流或忽略。结束时如有跳过的 symbol，log WARNING 汇总：`"Tickfile: skipped {N}/{M} symbols for minute={minute_key}"`。如果某分钟所有 symbol 都失败，文件仍包含 header（首次写入时）或仅跳过该分钟数据行（append 时），不影响后续分钟写入。**末行截断修复**：append 路径在追加前读取文件末尾，检查最后一行是否为有效行（65 列）。如果不是（crash 导致的截断行），先追加换行符确保新数据从新行开始，并 log WARNING。**截断检查和追加操作均在 `_get_write_lock(path)` scope 内执行**，防止 TOCTOU race。**DEBUG 断言**（可选）：`write_tickfile_rows` 在 DEBUG 模式下可断言每行 `row.count(',') == 64`（65 列 = 64 个逗号），用于早期 bug 检测；生产环境不做此检查以避免性能开销。

**原子性保障**：**atomic_write 失败**：整个首次写入路径 wrap 在 try/except 中，失败时清理 `.tmp` 文件（含残留 `.tmp` 清理——如存在旧 `.tmp` 则先删除）并 re-raise（与 `_flush_minutes_internal` 的 `IOError → SystemExit` 模式一致） |
| `recover_tickfile_seqno(output_dir: str, minute_key: str) -> int` | 从已有 tickfile 文件中恢复最后一行有效 seqno（seqno 单调递增，即等于 max seqno）。实现方式：从文件头顺序逐行读取（文件以 `encoding='utf-8', newline=''` 打开，与写入路径一致），只保留最后一个有效 seqno 值（O(1) 内存，O(N) 时间）。跳过字段数不足 65 的行和 seqno 非整数的行。CSV 行可用 `line.split(',')` 安全解析（tickfile 字段不含逗号或引号）。**仅读取 `tickfile_YYYYMMDD.csv`，忽略同目录下的 `.tmp` 残留文件**。文件不存在、仅 header、或 0 字节时返回 0。**异常处理**：`open()` 失败（`FileNotFoundError` 等）返回 0，与 "文件不存在" 行为一致。**文件大小保护**：实现时应在读取前检查 `os.path.getsize(path)`，如超过阈值（如 200MB）则 log WARNING 并跳过恢复、返回 0，防止异常大文件阻塞启动。**调用位置**：由 flusher 层在 `_tickfile_seqno += 1` 之前调用——当 `_tickfile_seqno == 0` 且文件已存在时，调用 `recover_tickfile_seqno` 获取恢复值赋给 `_tickfile_seqno`，然后再递增。`write_tickfile_rows` 不负责恢复逻辑。**Lock 约束**：不可获取 `_get_write_lock`——由 flusher 在调用 `write_tickfile_rows`（会获取 write lock）之前调用。**单引擎进程前提**保证了在无 write lock 的情况下读取是安全的（同一 tickfile 同一时刻只能被一个引擎进程写入）。**不需要 write lock 的根本原因**：tickfile 在运行期间从不被删除（首次 atomic_write 创建后只 append），因此 `os.path.exists` 检查和文件读取不会看到不一致状态 |

### 5.2 修改模块

**`src/minute_bar/aggregator.py`**：
- `SharedState.__init__` 新增 `latest_order_by_symbol: Dict[str, OrderRecord] = {}`。字典大小由市场 symbol 数量自然限制（JP equity ~4000 symbols），无需额外 bounding
- `SharedState.process_order` 更新 cache：
  ```python
  existing = self.latest_order_by_symbol.get(record.symbol)
  if existing is None or (record.time, record.rcvtime) >= (existing.time, existing.rcvtime):
      self.latest_order_by_symbol[record.symbol] = record
  ```
  使用 `(time, rcvtime)` 元组比较，与 `maybe_update_latest_unlocked` 的 tie-breaking 模式一致（同 time 时后到者优先级更高）。
- **`process_order` 标记为 TEST ONLY**：live 模式下由 `_order_loop` 直接写入 `raw_order_buffers` 和 `latest_order_by_symbol`（见 engine.py 修改），不在生产路径调用此方法。实施时必须在 `process_order` 的 docstring 中标注此约束——"TEST ONLY: do not call from live mode. Calling from live `_order_loop` would cause seqno duplication."

**`src/minute_bar/engine.py`**：
- 传递 `enable_tickfile=self._config.output.enable_tickfile` 给 flusher 构造器
- **`_order_loop` 中更新 `SharedState`**（live 模式的 order 不经过 `process_order`，必须在此显式写入 SharedState）：
  ```python
  # 批处理模式：在内层 for line in lines 循环中累积，循环结束后单次 lock 批量写入
  pending_shared_orders = []  # List[Tuple[Union[str, object], OrderRecord]]
  LATE_CACHE_MARKER = object()  # sentinel for late-record cache update
  for line in lines:
      # ... existing parse logic ...
      record = build_order_record(parsed, seqno)
      # Late record detection (existing logic, MUST be before append):
      if minute_key in self._flushed_order_minutes:
          # Late record: update latest_order_by_symbol only if newer than cache.
          # Late records come from already-flushed minutes; normally their time is
          # older than cache entries from subsequent minutes, so the (time, rcvtime)
          # comparison naturally rejects them. But if a late record IS newer (same
          # minute, later rcvtime), updating the cache gives fresher carry-forward data.
          pending_shared_orders.append((LATE_CACHE_MARKER, record))
          # ... existing late record handling ...
          continue  # <-- NOT appended to buffers or raw_order_buffers
      buffers.setdefault(minute_key, []).append(record)  # 本地 buffer（order CSV 用）
      pending_shared_orders.append((minute_key, record))  # 累积 SharedState 写入
  # 循环结束后单次 lock 批量写入
  with self._state.lock:
      for mk, rec in pending_shared_orders:
          if mk is LATE_CACHE_MARKER:
              # Late record: only update latest_order_by_symbol (not raw_order_buffers).
              # Uses same (time, rcvtime) comparison as normal path — late records
              # from flushed minutes usually lose the comparison, but if they win
              # (same minute, later rcvtime), the cache gets a fresher entry.
              existing = self._state.latest_order_by_symbol.get(rec.symbol)
              if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                  self._state.latest_order_by_symbol[rec.symbol] = rec
          else:
              self._state.raw_order_buffers.setdefault(mk, []).append(rec)
              existing = self._state.latest_order_by_symbol.get(rec.symbol)
              if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                  self._state.latest_order_by_symbol[rec.symbol] = rec
  ```
  本地 `buffers[minute_key]` 继续用于 order CSV 文件写入（现有行为不变）。`SharedState.raw_order_buffers` 仅供 tickfile 生成时读取（与 replay 模式一致的 SharedState bridge 模式）。**Note**：`_order_loop` 不调用 `SharedState.process_order()`——直接写入 `raw_order_buffers` 和 `latest_order_by_symbol`。`process_order` 标记为 TEST ONLY（见 aggregator.py 修改），不应在 live 模式调用，否则 seqno 会重复。
  **Late record 路径**：`_order_loop` 中的 late record 分支（已 flush 分钟的 order）直接 `continue` 跳过后续逻辑，不写入 `raw_order_buffers`。Late record 使用 `LATE_CACHE_MARKER`（`object()` sentinel，避免字符串碰撞）标记，在 batch write 时仅更新 `latest_order_by_symbol`（不写入 `raw_order_buffers`），使用与正常路径相同的 `(time, rcvtime) >=` 比较。由于 late record 来自已 flush 分钟，其 time 通常小于 cache 中来自后续分钟的记录，比较自然淘汰；仅在 late record 确实更新时（如同一分钟内更晚到达的 order）才覆盖 cache，为 carry-forward 提供最新可用数据。
  **锁获取优化**：批处理模式将锁获取次数从 O(records) 降至 O(chunks)，与 `_data_loop` 的 drain loop 模式一致。每个 chunk 处理完毕后批量写入，避免逐条竞争 `SharedState.lock`
  **异常安全性**：如果内层循环（parse + build）抛异常，`pending_shared_orders` 中的部分数据不会被写入 SharedState——这是安全的。现有 `_order_loop` 的外层 try/except 会捕获异常并设置 `self._order_thread_error`，thread 随即退出。引擎通过 checkpoint 机制从已提交 offset 重启，未提交的数据被完整 replay

**`src/minute_bar/flusher.py`**（以下仅列出新增/修改的参数，非完整签名）：
- `ClockWatermarkFlusher.__init__` 新增 `enable_tickfile: bool = False`、`_tickfile_seqno: int = 0`
- `_flush_minutes_internal` 中在 lock scope 内拷贝 `latest_order_by_symbol`。**注意**：此函数有两个独立的 lock scope 需要此拷贝——正常 flush path（`_flush_minutes_internal` ~L251-258）和 cross-day path（`_step1_cross_day_check` ~L92-112）：
  ```python
  with self._state.lock:
      # ... existing pops ...
      snapshot_copy = dict(self._state.latest_snapshot)
      latest_order_copy = dict(self._state.latest_order_by_symbol)  # shallow copy，OrderRecord 是 frozen dataclass
  ```
  将 `latest_order_copy` 传递给 `_write_minute_files`。**多分钟 batch 说明**：`latest_order_copy` 在 lock scope 内一次性拷贝，整个 batch 共享同一快照。在多分钟 flush（stall recovery、startup catch-up）中，batch 内后续分钟可能使用略旧的 order carry-forward（order thread 在 batch 处理期间可能更新了 cache）。这是已知行为——carry-forward 数据仍为有效记录，只是可能非最新。Live 模式 `order_records` 来自现有的 `raw_order_buffers` pop（非新增逻辑），`latest_order_copy` 是新增的 cache 拷贝
- `_write_minute_files` 签名新增 `latest_order_copy: Optional[Dict[str, OrderRecord]] = None`（默认 `None` 向后兼容现有调用点），末尾调用 tickfile 生成：
  ```python
  if self._enable_tickfile:
      # Seqno recovery: at most once per day, before first increment
      path = get_tickfile_path(self._output_dir, minute_key)
      if self._tickfile_seqno == 0 and os.path.exists(path):
          self._tickfile_seqno = recover_tickfile_seqno(self._output_dir, minute_key)
      self._tickfile_seqno += 1
      code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
      selected = select_tickfile_records(raw_records or {}, snapshot_copy, order_records or [], latest_order_copy)
      write_tickfile_rows(self._output_dir, minute_key, selected, self._tickfile_seqno, code_table_getter=code_getter)
  ```
- `_step1_cross_day_check` 中需在昨天分钟 flush 循环**之前**拷贝 `latest_order_by_symbol`（在 lock scope 内），传递给循环中的 `_write_minute_files`。循环之后再重置。**拷贝在过滤之前——这是有意的**：跨日分钟 flush 使用昨日的 order cache（未过滤），因为正在 flush 的是昨天的 pending minutes。过滤在循环之后执行，为新交易日准备干净状态：
  ```python
  # 在 pending/pending_raw/pending_orders 的 lock scope 中（flusher.py ~L92-112），新增：
  latest_order_copy = dict(self._state.latest_order_by_symbol)

  # 循环中调用 _write_minute_files 时传递 latest_order_copy：
  self._write_minute_files(minute_key, minute_snapshot, data, raw, orders, latest_order_copy)

  # 循环之后、_write_checkpoint 之后重置：
  self._tickfile_seqno = 0
  with self._state.lock:
      # 仅保留属于新交易日的 order，过滤属于旧交易日的 stale 记录
      # current_date 已在第一个 lock block 中计算，此处直接复用
      self._state.latest_order_by_symbol = {
          sym: rec for sym, rec in self._state.latest_order_by_symbol.items()
          if str(rec.time)[:8] == current_date
      }
  ```
  与现有 `output_minutes.clear()`、`last_totalvol_by_symbol.clear()` 等重置保持一致（flusher.py ~L128-131），防止昨日 order 数据 carry-forward 到新交易日。**Lock scope 说明**：`latest_order_copy` 拷贝添加在现有 `snapshot_copy = dict(self._state.latest_snapshot)` 所在的同一个 lock scope 末尾（flusher.py ~L112），与 snapshot 拷贝在同一次 lock 获取中完成。
- `flush_all_remaining()` 无需修改——它委托给 `_flush_minutes_internal`，后者按上述修改包含 `latest_order_by_symbol` 拷贝后，shutdown 场景自动覆盖。最后几分钟的 tickfile 数据通过此路径正常写入。`_flush_minutes_internal` 内部已在 lock scope 内拷贝 `latest_order_by_symbol`，shutdown 场景下使用线程停止前的最新 order 状态**设计决策**：`latest_snapshot` 跨日不清空（snapshot 的 carry-forward 跨日保留，因为昨收价等字段对新交易日仍有参考价值），而 `latest_order_by_symbol` 跨日按日期过滤清空（order 数据时效性更强，跨日后的旧 bid/ask 无意义）。因此新交易日第一分钟：snapshot 字段可能有值（来自昨日 carry-forward），order 字段全 NA（cache 被清空），直到第一条 order 数据到达。这与 snapshot 的处理模式不同但合理——`_snapshot_at_minute_end` 也会在跨日时 pop 清空，新交易日第一分钟使用空 snapshot_copy。**`_tickfile_seqno` 不持久化在 checkpoint 中**——它是 ephemeral counter，crash 后通过 `recover_tickfile_seqno()` 从文件恢复，因此将其重置放在 `_write_checkpoint()` 之前或之后不影响 crash-recovery 正确性

**`src/minute_bar/replay.py`**：
- `ReplayEngine.__init__` 新增 `_enable_tickfile`、`_tickfile_seqno = 0`
- **Order 数据桥接（SharedState bridge）**：replay 的 `_stream_orders` 在独立线程运行，有自己的 `buffers` 字典。为让 tickfile 生成能获取 order 数据，采用 SharedState 桥接方案：
  ```python
  # _stream_orders 中，在 record = build_order_record(parsed, seqno) 之后：
  # 1. 现有逻辑不变：append 到本地 buffers[minute_key]
  buffers.setdefault(minute_key, []).append(record)
  # 2. 新增：同时 append 到 SharedState.raw_order_buffers（tickfile 生成用）
  # 必须使用批处理模式：在内层循环累积到本地列表后，循环结束时单次 lock 批量写入
      # 批量写入示例（使用 self._state 而非局部 state）：
      with self._state.lock:
          for mk, rec in pending_orders:
              self._state.raw_order_buffers.setdefault(mk, []).append(rec)
              existing = self._state.latest_order_by_symbol.get(rec.symbol)
              if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                  self._state.latest_order_by_symbol[rec.symbol] = rec
  ```
  - `state`（SharedState）的创建和赋值应在 `ReplayEngine.run()` 中、`write_executor.submit(self._stream_orders, ...)` 之前完成：`self._state = SharedState(...)`。这样 `_stream_orders` 通过 `self._state` 访问 SharedState 时，赋值已完成，避免跨线程时序依赖。`_stream_snapshots` 和 `_stream_orders` 签名不变。`_stream_orders` 通过闭包引用 `self._state`（硬性约束：`self._state` 赋值必须在 `write_executor.submit` 之前完成）
  - 本地 `buffers` 仅供 order CSV 文件写入使用（现有行为不变）
  - `SharedState.raw_order_buffers` 仅供 tickfile 生成时读取和 pop
  - **内存开销**：order 记录在 `DELAYED_FLUSH_ROUNDS` 窗口内被双倍存储（本地 + SharedState），snapshot flush tickfile 写入后 SharedState 中的数据被 pop 释放
- **Replay SharedState 重构总览**：将 `_stream_snapshots` 中的局部 `state` 提升为 `self._state`，使 `_stream_orders`（独立线程）和 `_flush_snapshot_minute`（主线程）共享同一个 SharedState 实例。数据流：`run()` -> `self._state = SharedState(...)` -> `_stream_snapshots(self._state)` / `_stream_orders(self._state via closure)`。
- **Implementation Checklist**（replay.py SharedState 共享重构）：
  **全局实施步骤（按推荐顺序）**：
  1. `config.py`：`OutputConfig` 新增 `enable_tickfile: bool = False`
  2. `aggregator.py`：`SharedState.__init__` 新增 `latest_order_by_symbol`，`process_order` 增加 cache 更新并标记 TEST ONLY
  3. 新建 `tickfile.py`：实现 `TICKFILE_HEADER`、`build_tickfile_row`、`select_tickfile_records`
  4. `writer.py`：新增 `get_tickfile_path`、`write_tickfile_rows`（含末行截断修复）、`recover_tickfile_seqno`
  5. `flusher.py`：在 `__init__`、`_flush_minutes_internal`、`_write_minute_files`、`_step1_cross_day_check` 中新增 tickfile 相关代码（不替换现有逻辑）
  6. `engine.py`：`_order_loop` 新增 SharedState 批处理写入
  7. `replay.py`：按下方 Checklist 重构 SharedState 共享

  1. 在 `ReplayEngine.run()` 中、`write_executor.submit` 之前创建 `self._state = SharedState(...)`
  2. 将 `_stream_snapshots` 中所有局部变量 `state` 替换为 `self._state`（建议使用 IDE rename refactoring）
  3. `_stream_orders` 通过闭包引用 `self._state`（硬性约束：赋值必须在 submit 之前完成）
  4. `_flush_snapshot_minute` 中所有 `state` 引用替换为 `self._state`，并移除函数签名中的 `state` 参数（改为通过 `self._state` 访问）
  5. `_stream_orders` 中所有 SharedState 写入代码使用 `self._state`（通过闭包引用），包括 `raw_order_buffers` 和 `latest_order_by_symbol` 的批处理写入
- **`_flush_snapshot_minute` 末尾新增 tickfile 生成**（此代码块在 Implementation Checklist 步骤 4 即 state→self._state 重构完成后实现）：
  ```python
  if self._enable_tickfile:
      # Seqno recovery: at most once per day, before first increment
      path = get_tickfile_path(output_dir, minute_key)
      if self._tickfile_seqno == 0 and os.path.exists(path):
          self._tickfile_seqno = recover_tickfile_seqno(output_dir, minute_key)
      self._tickfile_seqno += 1
      with self._state.lock:
          # Atomic pop——在单个 lock scope 内原子性获取并移除
          order_records = self._state.raw_order_buffers.pop(minute_key, [])
          latest_order_copy = dict(self._state.latest_order_by_symbol)
      code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
      selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)
      write_tickfile_rows(output_dir, minute_key, selected, self._tickfile_seqno, code_table_getter=code_getter)
  ```
  - **为什么用 atomic pop**：copy-then-pop 方案在 copy 和 pop 之间存在竞态窗口——`_stream_orders` 在此窗口内 append 的新记录会被 pop 删除但未被 copy 读到，导致数据丢失。Atomic pop 在单个 lock scope 内完成获取+移除，pop 之后到达的新记录进入新 list。如果 snapshot flush 先于 order 全部到达，tickfile 中该分钟 order 数据不完整，依赖 carry-forward 补充——这是 replay 模式的已知限制
- **线程安全**：
  - `SharedState.raw_order_buffers` 写：`_stream_orders`（order 线程），在 `self._state.lock` 下
  - `SharedState.raw_order_buffers` 读/pop：`_flush_snapshot_minute`（主线程），在 `self._state.lock` 下
  - `SharedState.latest_order_by_symbol` 同理，读写均在 `self._state.lock` 下
  - `_tickfile_seqno`：仅在主线程的 `_stream_snapshots` 中递增（`_flush_snapshot_minute` 由 `_stream_snapshots` 直接调用，非通过 executor 提交），无需额外同步
- **延迟 flush 对齐**：replay 的 snapshot 和 order 各自有独立的 `DELAYED_FLUSH_ROUNDS`。tickfile 由 snapshot flush 触发，在 atomic pop `raw_order_buffers` 时获取当时已到达的全部 order 数据。如果 order 数据尚未完全到达，tickfile 中该分钟的 order 字段可能不完整（依赖 carry-forward 补充）。这是 replay 模式的已知限制——replay 的 snapshot 流和 order 流在独立线程中运行，各自以不同速率消费数据，tickfile 数据获取的时序取决于这两个线程的相对进度。**影响评估**：JP equity order 数据量（70M+ records）远大于 snapshot（~15M records），order 线程消费速率系统性慢于 snapshot。大部分分钟的 tickfile order 字段可能都依赖 carry-forward，而非当前分钟的实际 order 数据。如果下游需要完整 order 数据，应直接使用 `order/` 目录下的 per-minute CSV 文件，而非 tickfile。**可选优化**：在 `_flush_snapshot_minute` 中 atomic pop 之前，检查 order 线程 watermark 是否已超过当前 minute_key（需在 SharedState 中维护 order watermark），未超过时等待短暂时间（如 100ms 循环检查，最多 3 次），可大幅降低数据不完整概率
- **EOF final flush**：`_stream_snapshots` 末尾的 EOF final flush 也调用 `_flush_snapshot_minute`，因此最后剩余的分钟也会获得 tickfile 输出（包括 atomic pop `raw_order_buffers` 和 carry-forward），无需特殊处理
- **Orphaned `raw_order_buffers` 清理**：atomic pop 后，order 线程继续写入同一 minute_key 的记录会创建新 list，此 orphaned 数据永远不会被再次 pop。**选定方案**：在 replay 结束的 final flush 后，遍历 `self._state.raw_order_buffers` 统计并 log INFO orphaned 总量（key 数量和总记录数），不单独清理（replay 结束后 SharedState 即被释放）。这是最简方案，避免在每分钟 flush 中增加额外复杂度

**`src/minute_bar/config.py`**：
- `OutputConfig` 新增 `enable_tickfile: bool = False`
- `load_config()` 中 `[output]` section 新增解析：`config.output.enable_tickfile = parser.getboolean("output", "enable_tickfile", fallback=False)`；配置示例：`enable_tickfile = true  # [output] section 下`

### 5.3 端到端数据流

每分钟 flush 时的 tickfile 生成流程：

```
Live 模式 (clock thread)：
  _flush_minutes_internal(minute_keys)
    ├─ [lock] pop ohlcv/raw_snapshot/raw_order buffers, copy latest_snapshot + latest_order_by_symbol
    └─ for each minute_key:
         _write_minute_files(minute_key, snapshot_copy, data, raw, orders, latest_order_copy)
           ├─ write_snapshot_file(...)     # 现有逻辑
           ├─ write_kline_file(...)        # 现有逻辑
           ├─ write_order_file(...)        # 现有逻辑
           └─ if enable_tickfile:
                _tickfile_seqno += 1
                selected = select_tickfile_records(raw, snapshot_copy, orders, latest_order_copy)
                  → [(symbol, snap, order), ...]  # 按 symbol 升序
                write_tickfile_rows(output_dir, minute_key, selected, seqno, code_table_getter)
                  → for (sym, snap, ord) in selected:
                      build_tickfile_row(snap, ord, seqno, code_table_getter) → CSV 行
                  → 首次 atomic_write(header + rows)，后续 append + flush + fsync

Replay 模式 (main thread)：
  _flush_snapshot_minute(state, minute_key, ...)
    ├─ [lock] pop ohlcv/raw_snapshot buffers, copy _snapshot_at_minute_end
    ├─ write_executor.submit(write_snapshot_file, ...)
    ├─ write_executor.submit(write_kline_file, ...)
    ├─ f.result()  # 等待写入完成
    └─ if enable_tickfile:
         _tickfile_seqno += 1
         [lock] order_records = raw_order_buffers.pop(minute_key), copy(latest_order_by_symbol)
         selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)
         write_tickfile_rows(output_dir, minute_key, selected, seqno, code_table_getter)
```

**注**：stall detection 触发的 flush（`_step3_minute_output` 中 `stall_flush_sec`）同样经过 `_flush_minutes_internal` → `_write_minute_files`，因此 tickfile 输出路径与正常 flush 一致，无需额外处理。

### 5.4 线程安全

- **`latest_order_by_symbol` 写**：live 模式在 `_order_loop` 中（order thread），持有 `SharedState.lock`；replay 模式在 `_stream_orders` 中（独立线程），写入 `SharedState.latest_order_by_symbol`，持有 `state.lock`
- **`latest_order_by_symbol` 读**：live 模式在 `_flush_minutes_internal` 中（clock thread），**在 `self._state.lock` scope 内 shallow copy** 为 local dict 后传给 `_write_minute_files`；replay 模式在 `_flush_snapshot_minute` 中（主线程），同样在 `state.lock` 下 shallow copy。`OrderRecord` 是 frozen dataclass，shallow copy 即可
- **`_tickfile_seqno`**：live 模式仅在 clock-thread 中访问；replay 模式仅在 `_stream_snapshots` 主线程中访问——**不**通过 ThreadPoolExecutor 提交
- **tickfile 文件写入**：通过 `_get_write_lock(path)` 序列化同一文件的并发写入

### 5.5 Crash 恢复

- **Tickfile 是 append-only daily 文件**，crash 后可能存在以下问题：
  - 重复行：分钟数据已写入 tickfile 但 checkpoint 未更新，restart 后重新 flush
  - 不完整行：crash 发生在 append 过程中
- **恢复策略**：
  1. 重启时 `recover_flushed_minutes` 从已有 snapshot/order 文件推断已 flush 的分钟
  2. **Seqno 恢复**：`_tickfile_seqno` 不持久化。重启后首次 tickfile 写入时，由 flusher 层在 `_tickfile_seqno += 1` 之前调用 `recover_tickfile_seqno(output_dir, minute_key)` 从已有 daily tickfile 文件中恢复。**运行前提**：同一 tickfile 文件同一时刻只能被一个引擎进程写入（这是现有 minute_bar 引擎的隐含前提，由 checkpoint 机制保证）。`recover_tickfile_seqno` 的 log 中包含进程 PID，便于排查多进程冲突。实现方式：从文件头顺序逐行读取，只保留最后一个有效 seqno 值（O(1) 内存，O(N) 时间）。跳过字段数不足 65 的行（crash 导致的截断行）、跳过 Seqno 字段非整数的行，每跳过一行 log WARNING。CSV 行可用 `line.split(',')` 安全解析（tickfile 字段不含逗号或引号）。如果文件不存在或为空（仅 header 或 0 字节），返回 0。**Recovery 每个引擎生命周期每个 daily 文件最多执行一次**（仅在 `_tickfile_seqno == 0` 且文件已存在时触发），后续写入直接使用已恢复的 `_tickfile_seqno` 值递增。跨日 reset 后 `_tickfile_seqno` 重置为 0，新日期首分钟会再次触发 recovery（如果新日期的 tickfile 文件已存在）。**性能特征**：恢复扫描 O(N) 时间，N 为 pre-crash 数据行数。全天 tickfile 通常 < 100MB（JP equity ~4000 symbols × 390 minutes），扫描在 SSD 上 < 1 秒。此为一次性开销，不影响正常运行时性能。**文件大小保护**：实施时增加文件大小上限检查（如 >200MB），超过阈值时 log WARNING 并跳过恢复，直接从 seqno=0 开始（异常大文件可能由重复 append 或下游误操作导致）
  3. **`write_tickfile_rows` 首次写入检测**：如果 daily tickfile 文件已存在且非空，读取第一行验证是否匹配 `TICKFILE_HEADER`；如果匹配，后续 append 即可；如果文件存在但为空（0 字节）或仅含 header 行，执行 `atomic_write` 覆盖重写（降级恢复）；如果文件非空但 header 不匹配（内容损坏），log ERROR 并跳过 tickfile 写入（需人工介入）
  4. **重复行接受**：tickfile 可能包含 restart 前的重复分钟数据（包含重复 seqno）。这是 append-only daily 文件的已知限制。如果需要严格去重，可在 day-end 时用外部脚本去重（不在引擎内处理）
  5. **Replay 模式无需恢复**：replay 始终从 seqno=0 开始，处理单日，不涉及 crash-restart

### 5.6 不做的事

- **不处理迟到记录**：迟到数据 append 到 snapshot/order 文件时不重新生成 tickfile
- **不支持 code.csv 的 per-minute 取最早**：code 表数据与分钟无关，取当前最新状态即可
- **不做 tickfile 去重**：crash-restart 可能产生重复行，不做在线去重
- **不做 order carry-forward 过期限制**：无论 order 数据多旧，仍会 carry-forward 使用
- **不做数据校验**：不验证 Volume 单调性、BidPrice < AskPrice 合法性、TradingDay == ActionDay 一致性——这些是下游关注点
- **不包含 update_flag**：tickfile 是按行合成的汇总文件，不是逐记录的原始数据，不需要 update_flag 列
- **跨日边界 `latest_order_by_symbol` carry-forward**：跨日 flush 处理的是前一天的 pending minutes（最晚到 1530），此时 `latest_order_copy` 仅包含当天 8:00-15:30 范围内的 order 数据。由于 JP equity 数据产生时间固定在 8:00-15:30（JST），不存在跨日混入未来数据的可能。carry-forward 是指“当前分钟无数据时延用历史记录”，不是“将未来数据添加到现在”。循环后的日期过滤重置确保新交易日从干净的 order cache 开始
- **Live 模式 order 字段是 flush-time snapshot**：order thread 逐条 append 到 `raw_order_buffers[minute_key]`，clock thread 在 flush 时 pop 整个 buffer。Pop 之后 order thread 继续处理的同一分钟后续记录 append 到新空列表，但不会再被 flush（`flushed_snapshot_minutes` 已包含该分钟）。Order 没有 late queue 机制。这意味着 tickfile 的 order 字段可能不包含该分钟全部 order 数据。如果下游需要完整 order 数据，应直接使用 `order/` 目录下的 per-minute CSV 文件
- **Tickfile 写入在 clock thread 上同步执行**（live 模式）：每分钟 append + fsync 会阻塞 clock thread，延迟通常 < 100ms（本地 SSD），网络存储可能更高。如未来出现性能问题可改为异步写入
- **`enable_order=False` + `enable_tickfile=True`**：当 `enable_order=False` 时，`_order_loop` 立即返回（engine.py），`SharedState.raw_order_buffers` 和 `latest_order_by_symbol` 始终为空。tickfile 仍正常生成——仅 snapshot 数据的 symbol 出现在 tickfile 中，所有 order 派生字段（BidPrice1/AskPrice1/BidVolume1/AskVolume1）输出 NA。`_flush_minutes_internal` 仍拷贝空的 `latest_order_by_symbol`（空字典），`select_tickfile_records` 正确处理空 order 数据
- **心跳包 vs carry-forward 不对称行为**：某 symbol 当前分钟仅有心跳包（raw_records 中 lastprice=0 的最早记录）→ 选取该记录 → LastPrice=NA（0→NA 规则）。而同一 symbol 如果当前分钟无任何 raw_records（使用 carry-forward snapshot）→ LastPrice 可能为非 NA 值（来自前一分钟的有效价格）。这是 spec 选择规则（raw_records 优先于 snapshot_copy）和 0→NA 规则共同作用的结果，符合需求设计但可能令下游困惑

### 5.7 补充说明

- **`get_tickfile_path(output_dir, minute_key)`**：参数名为 `minute_key`（YYYYMMDDHHMM 格式）是为了与现有 `get_snapshot_file_path(output_dir, minute_key)` 签名保持一致。函数内部从 `minute_key[:4]` 提取 YYYY、`minute_key[:8]` 提取 YYYYMMDD 用于目录和文件命名。参数 `minute_key` 仅用于提取日期部分（前 8 位），实际输出为 daily 文件。同一分钟内多次调用返回相同路径
- **`InstrumentID` 来源**：`select_tickfile_records` 返回 `(symbol, snap, order)` 元组，`InstrumentID` 取自元组中的 `symbol` 字段。snapshot 和 order 在该函数中通过 symbol 匹配，始终一致
- **`TradingDay` 与 `ActionDay` 始终相同**：两者都从同一 source 的 time 字段前 8 位派生（snapshot 优先，order fallback），因此对任意单行必然相等。两个名称保留是为了下游系统兼容
- **`code_table_getter` lambda 闭包**：`lambda symbol, t=self._code_table: t.table.get(symbol)` 通过默认参数捕获 `self._code_table` 引用（非快照）。使用公开属性 `CodeTable.table`（property，返回 `self._table`）。由于 `CodeTable` 通过 `.refresh()` 原地更新 `_table` dict（不替换对象），lambda 始终看到最新状态。lambda 仅执行单 key dict lookup（`dict.get`），GIL 保护下线程安全，无需额外加锁。**依赖假设**：此 lambda 依赖 `CodeTable._table` 被 `.refresh()` 原地修改而非替换。如果未来重构 `CodeTable` 替换 `_table` 对象，此 lambda 需同步更新。**注意**：`CodeTable.load()` 调用 `_table.clear()` 后逐条填充，不应与 tickfile 生成并发调用（仅在启动时执行一次）

### 5.8 Observability

| 级别 | 消息 | 场景 |
|---|---|---|
| INFO | `"Tickfile append: {path} minute={minute_key} ({N} symbols, seqno={seqno})"` | 每分钟 flush 成功 |
| INFO | `"Tickfile order absence: minute={minute_key} has {N} symbols with snapshot but zero order records (carry-forward only)"` | live 或 replay 模式某分钟 order 完全缺失（`len(order_records) == 0`），所有 order 字段依赖 carry-forward |
| WARNING | `"Code table missing entry for symbol {symbol}, limit prices set to NA"` | code_table 查找失败 |
| WARNING | `"Snapshot decimal ({sd}) != code decimal ({cd}) for symbol {symbol}"` | decimal 不一致 |
| WARNING | `"Snapshot decimal ({sd}) != order decimal ({od}) for symbol {symbol} (carry-forward?)"` | snapshot/order decimal 不一致（限流：每 symbol 每分钟最多 1 条，用 `set[(symbol, minute_key)]` 追踪） |
| ERROR | `"Tickfile row build failed for symbol {symbol} seqno={seqno}: {exception}"` | build_tickfile_row 异常（纯逻辑函数异常 = 代码 bug），含 exc_info=True 完整 traceback |
| WARNING | `"Tickfile: skipped {N}/{M} symbols for minute={minute_key}"` | 分钟内有 symbol 被跳过（N>0 时输出） |
| INFO | `"Tickfile seqno recovered: {path} seqno={recovered_seqno}"` | crash 恢复成功恢复 seqno 值 |
| WARNING | `"Tickfile seqno recovery: skipped corrupted line at line {n}"` | crash 恢复扫描跳过截断行 |
| ERROR | `"Tickfile write failed: {path} minute={minute_key}: {exception}"` | I/O 错误 |
| INFO | `"Tickfile header rewrite: {path} (empty or header-only file, overwritten)"` | 降级恢复：文件为空或仅 header 时覆盖重写 |
| ERROR | `"Tickfile file exists but header corrupted: {path}"` | crash 恢复检测到损坏 |
| DEBUG | `"Tickfile order carry-forward: symbol={symbol} using order from minute={orig_minute} (age={age_minutes} min)"` | carry-forward order 跨越超过 30 分钟时记录（帮助诊断 stale bid/ask 数据） |
| INFO | `"Tickfile daily summary: {path} total={N} rows across {M} minutes"` | 跨日或 shutdown 时输出当日 tickfile 汇总（行数 + 分钟数），用于运维监控 |
| WARNING | `"Tickfile short rcvtime: symbol={symbol} rcvtime={rcvtime} (len < 12), UpdateTime set to NA"` | rcvtime 长度不足，无法提取完整时分（Column #16） |
| WARNING | `"Tickfile truncated last line detected: {path}, appending newline before new data"` | append 路径检测到 crash 导致的不完整末行 |
| INFO | `"Tickfile orphaned order buffers: {N} keys, {M} total records (replay final flush)"` | replay 结束时未被 pop 的 order buffer 统计 |
| WARNING | `"Tickfile seqno recovery skipped: {path} file too large ({size}MB > {threshold}MB)"` | 恢复时大文件保护跳过 |

## 6. 测试策略

| 测试层级 | 覆盖内容 |
|---|---|
| 单元测试 | `TICKFILE_HEADER` 列数断言：`assert len(TICKFILE_HEADER.split(',')) == 65` |
| 单元测试 | `build_tickfile_row`：decimal 转换、NA 字段、时间格式、limit 映射、除零保护、LastPrice=0→NA、decimal=0、BidPrice1=0→NA、IsShortRestricted 其他值→NA |
| 单元测试 | `select_tickfile_records`：raw 优先、carry-forward、单边缺失、排序、order carry-forward 跨多分钟 gap |
| 单元测试 | `write_tickfile_rows`：首次 atomic write + header、后续 append、zero-symbol minute 不写数据行、已有文件 header 校验（匹配 → append，不匹配 → log ERROR 跳过）、build_tickfile_row 异常时跳过该 symbol、atomic_write 失败时清理 `.tmp` |
| 单元测试 | `SharedState.latest_order_by_symbol`：写入、更新、旧记录不覆盖新记录 |
| 单元测试 | `recover_tickfile_seqno`：空文件、正常文件、末行截断、字段数不足、seqno 非整数 |
| 集成测试 | Flusher 集成：`_flush_minutes_internal` 触发 tickfile 写入、lock 下拷贝 `latest_order_by_symbol` |
| 集成测试 | 跨日重置：seqno 正确重置、`latest_order_by_symbol` 清空、新交易日不含昨日 carry-forward |
| 集成测试 | Live mode order bridge：`_order_loop` 写入 `raw_order_buffers` → tickfile 中出现当前分钟 order 数据 |
| 端到端测试 | ReplayEngine 全链路：多 symbol × 多分钟 → 验证 seqno、排序、字段正确性、order 字段非空 |
| 端到端测试 | Live 模拟：data_simulator 驱动，验证实时 tickfile 生成 |
| 集成测试 | 并发安全性：多线程同时 append 同一 tickfile 文件，验证 `_get_write_lock` 序列化写入正确 |
| 边界测试 | decimal=0：原始值 448000 → 448000.0（无缩放），原始值 0 → NA |
| 边界测试 | decimal<0：使用 10^0=1，不缩放 |
| 边界测试 | 全零价格 symbol（未交易）、停牌 symbol、空分钟（无数据） |
| 边界测试 | totalamount decimal 转换、Turnover 值正确性（累计值，非增量——与 kline amount 增量语义不同） |
| 回归测试 | 全量 `tests/` 运行，`enable_tickfile=False` 时无影响 |

### 关键测试用例

1. **Turnover decimal 转换正确性**：`totalamount=350251000, decimal=2` → Turnover=`3502510.0`
2. **Order carry-forward 跨分钟 gap**：symbol 有 order 在 0900，无 0901/0902 order → 验证 0902 使用 0900 order，保留原始 time
3. **Snapshot decimal != code decimal**：snapshot.decimal=2, code.decimal=3 → 验证 limit 价格使用 code.decimal，log WARNING；同 symbol 同分钟多次触发只 log 1 条 WARNING（限流验证）
4. **LastPrice=0 → NA**：未交易 symbol 验证 LastPrice、OpenPrice、ClosePrice 均为 NA
5. **Snapshot-only 行**：order 为 None 时，TradingDay/UpdateTime 从 snapshot 取，BidPrice1=NA、BidVolume1=NA、AskPrice1=NA、AskVolume1=NA
6. **Order-only 行**：snapshot 为 None 时，TradingDay 从 order.time 取，LastPrice=NA、PreClosePrice=NA、IsShortRestricted=NA、所有价格列=NA，BidPrice1 有值；验证 `LocalTime` 从 `order.time` 派生（JST 格式正确），`UpdateTime` 从 `order.rcvtime` 派生（CST 格式 `YYYYMMDD HH:MM:00`）
7. **Replay 端到端**：3 symbol × 3 分钟 → 9 行（假设所有 symbol 在所有分钟均有数据），每分钟内行按 InstrumentID 升序排列，seqno 模式为 [1,1,1, 2,2,2, 3,3,3]（同分钟同 seqno）。order 字段通过 carry-forward 非空（测试使用小数据集，order 数据在 snapshot flush 前已全部到达；生产 replay 场景中 order 字段可能使用前一分钟 carry-forward）
8. **BidPrice1/AskPrice1=0 → NA**：order 的 bidprice=0 时输出 NA 而非 0.0
9. **build_tickfile_row 异常隔离**：一条记录抛异常不影响同一分钟其他 symbol 输出，异常 symbol 的行被跳过
10. **decimal=0 价格**：原始值 448000 → 448000.0（无缩放），原始值 0 → NA
11. **NaN 表示**：验证输出 CSV 中缺失字段为 `NA` 字面量（非空字符串）
12. **IntraDailyReturn `-0.0` 归一化**：LastPrice == PreClosePrice 时输出 `0.0` 非 `-0.0`
13. **Seqno gap（空分钟）**：某分钟无数据时不触发写入，seqno 不递增；下一有数据分钟 seqno = previous + 1
14. **Order carry-forward cache 为空**：symbol 从未有 order 数据 → BidPrice1=NA, BidVolume1=NA
15. **Volume=0**：totalvol=0 输出 `0`（不做 0→NA），bidsize=0 输出 `0`
16. **atomic_write 失败恢复**：首次写入 `os.replace` 失败时 `.tmp` 被清理，不残留
17. **Corrupted header 检测**：已有 tickfile 文件第一行不匹配 `TICKFILE_HEADER` → log ERROR，无数据写入，无异常抛出
18. **`shortsellflag` 非标准值**：`shortsellflag=2`（或其他非 0/1 值） → `IsShortRestricted='NA'`
19. **2-10 档 NA 验证**：BidPrice2~10、BidVolume2~10、AskPrice2~10、AskVolume2~10 全部输出 `NA`（非 `0`）
20. **跨日 seqno 重置**：模拟跨日场景——验证日期 T 的最后 seqno 为 N，日期 T+1 的首 seqno 为 1（非 N+1），`latest_order_by_symbol` 已按日期过滤清理，T+1 的 tickfile 不包含 T 日的 order carry-forward（假设 clean start，T+1 的 tickfile 文件不存在。Crash recovery 场景下 T+1 首 seqno 为 `recovered_seqno + 1`）
21. **UpdateTime 短 rcvtime 防护**：`rcvtime=2026052209`（长度 11 < 12）→ UpdateTime 输出 NA，log WARNING；同时验证正常长度 rcvtime 的 UpdateTime 格式不受影响
22. **Replay order 时序竞争**：注入 order 数据时人为延迟 order 线程，验证 tickfile 中 order 字段正确 carry-forward，并验证该 symbol 的 tickfile 输出行包含 carry-forward order 字段（而非当前分钟部分到达的 order 数据）
23. **跨日边界 order carry-forward**：跨日时验证昨日最后一分钟 tickfile 的 order 字段（应为当日 8:00-15:30 范围内的数据，不可能包含新一天数据）
24. **Header 损伤恢复**：tickfile 文件存在但为空（0 字节）时重启，验证不永久跳过 tickfile 写入（降级重写）
25. **并发 append 安全性**：多线程同时 append 同一 tickfile 文件，验证数据完整性（依赖 `_get_write_lock` 序列化）
26. **`recover_tickfile_seqno` 大文件性能基准**：~100MB tickfile（~1.5M 行），验证恢复时间 < 2s（标记为 `@pytest.mark.slow`）
27. **心跳包 snapshot**：当前分钟 raw_records 中该 symbol 仅有 lastprice=0 的心跳包（且为 time 最早的记录），验证 LastPrice=NA 而非 carry-forward 价格
28. **跨日 order 清空验证**：新交易日第一分钟无 order cache，验证 BidPrice1/AskPrice1 为 NA
29. **末行截断修复**：构造一个最后一行字段数不足 65 的 tickfile 文件，验证 append 路径在追加新数据行前自动补换行符，新数据从新行开始，且 log WARNING
30. **Order carry-forward decimal 变化**：symbol 在 0900 有 decimal=2 的 order，0901-0909 无 order，验证 carry-forward 使用 decimal=2，snapshot/order decimal 不一致时 log WARNING
31. **`enable_order=False` + `enable_tickfile=True`**：所有 order 字段为 NA，`latest_order_by_symbol` 为空，`_flush_minutes_internal` 仍拷贝空的 `latest_order_by_symbol`，tickfile 写入无报错
32. **并发 tickfile append（stall flush 触发）**：stall detection 触发 flush 时正常 tick 也在 flush，验证 `_get_write_lock` 阻止交错写入
33. **Tickfile header-only 文件恢复后 seqno**：空文件被覆盖重写后，新数据 seqno 从 1 开始（非 0）
34. **心跳包 snapshot（非最早记录）**：当前分钟 raw_records 中该 symbol 有非心跳记录（time 更早）和心跳包（lastprice=0，time 更晚），验证选取非心跳记录（LastPrice 有值）
35. **Late record cache 更新（新时间戳胜出）**：symbol 在 0900 有 order (time=09000100000)，cache 已有 0901 的记录 (time=09010100000)；late record 的 time < cache entry 的 time，不覆盖。再构造 late record time=09000500000 > 缓存中另一 symbol 的旧 entry，验证 cache 正确更新
36. **Tickfile append 顺序性**：分钟 A（seqno=1）先 append，分钟 B（seqno=2）后 append，验证文件中 A 的行确实在 B 的行之前
37. **Snapshot carry-forward + raw_records 交互**：某 symbol 同时有 raw_records（1 条，time 较早）和 snapshot_copy（不同记录），验证 raw_records 被选中（非 snapshot_copy）
38. **Turnover `totalamount=0`**：`totalamount=0, decimal=2` → Turnover=`0.0`（非 NA），验证零成交额是合法值不触发 0→NA 规则
39. **`code_table_getter=None`**：不配置 code table 时（`code_table_getter=None`），验证 UpperLimitPrice=NA、LowerLimitPrice=NA，其他字段不受影响
40. **`shortsellflag` 字段缺失**：源数据中 `shortsellflag` 字段缺失（parser 默认为 0），验证 IsShortRestricted='N'（非 NA）
41. **`flush_all_remaining` shutdown tickfile 输出**：live 模式 shutdown 时验证最后几分钟 tickfile 正常写入（含 `latest_order_by_symbol` carry-forward），文件完整无截断
42. **`enable_tickfile=False` 零副作用**：配置关闭时，无 tickfile 文件创建、`latest_order_by_symbol` 不填充、`_order_loop` 不写 SharedState、`_write_minute_files` 跳过 tickfile block，现有功能完全不受影响
