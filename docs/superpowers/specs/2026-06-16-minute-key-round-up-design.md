# Minute-Key Round-Up 设计（snapshot/order 时间戳归属上一分钟末 → 下一分钟）

> **Date**: 2026-06-16
> **Status**: 设计完成，待用户 review → 进入实现计划
> **Parent**: 源于 tickfile 质量检查准则 #3
> **类型**: 语义变更（行为改变），需 golden 测试更新 + 全量回归

---

## 1. 背景

Tickfile 质量检查时发现 **minute_key 归属语义** 与生产需求不符。

**当前实现**（round-down / 向下取整）：`time_to_minute_key(t) = str(t)[:12]`
- 时间戳 `09:00:01.000` → minute **0900**
- 见 `src/minute_bar/clock.py:49` + `order_accel/src/lib.rs:1277`

**生产需求**（round-up / 向上取整）：snapshot/order 的时间戳是**分钟末快照**语义——
`09:00:01.000` 描述的是"0900→0901 这一分钟结束时的状态"，应归属 **0901**。

## 2. 用户确认的规则（brainstorm 锁定 + 实施后边界细化）

区间语义：**左开右闭**——bar M 覆盖 `((M-1):00.000, M:00.000]`。

| 决策点 | 选择 |
|--------|------|
| 区间 | **左开右闭** `(M-1:00.000, M:00.000]` |
| 应用范围 | **order 和 snapshot 一致**（同时间戳必落同分钟，tickfile 能正常合成） |
| 非边界时间戳（SSMMM>0，如 09:00:01.000） | round-up → **0901**（下一分钟） |
| 精确边界时间戳（SSMMM=0，如 09:01:00.000 / **15:30:00.000 收盘**） | **归自身分钟** → 0901 / **1530**（不 +1） |
| 最末分钟 | 收盘 `15:30:00.000` → **1530**（不产生 1531） |

**规则一句话**：`minute_key(t) = 时钟分钟(t)` 当 `t` 落在精确分钟边界（SSMMM=0），否则 `时钟分钟(t) + 1`。等价于 ceil-to-minute 且边界归自身。

> **实施后细化（2026-06-16）**：初版用严格 floor+1（整点也+1），但源数据收盘时间戳正好是
> `15:30:00.000`，floor+1 会把它推到 spurious 1531 分钟。改为左开右闭后收盘正确归 1530。
> 影响仅限全天 ~35K 条精确边界记录（基本都在收盘），盘中上亿 tick 不变。end_time/start_time
> 保持 `end=M`/`start=M-1min`（已匹配 `(M-1,M]` 区间），无需改动。

## 3. 取整映射表

| 时间戳（17-digit） | 时钟分钟 | → minute_key | 说明 |
|-------------------|---------|-------------|------|
| `20260528090001000` (09:00:01.000) | 0900 | **0901** | 非边界，round-up |
| `20260528090059000` (09:00:59.000) | 0900 | **0901** | 非边界，round-up |
| `20260528090100000` (09:01:00.000) | 0901 | **0901** | 精确边界，归自身 |
| `20260528095901000` (09:59:01.000) | 0959 | **1000** | 非边界，跨小时进位 |
| `20260528153000000` (15:30:00.000) | 1530 | **1530** | **收盘精确边界，归自身（不产生 1531）** |
| `20260528153001000` (15:30:01.000) | 1530 | **1531** | 非边界，round-up |
| `20260528113001000` (11:30:01.000) 午休前 | 1130 | **1131** | 非边界，round-up |

## 4. 实现方案（A：改 `time_to_minute_key` 源头）

在两个单一源点实现 floor+1，所有调用点自动一致。

### 4.1 算法（Python + Rust 同构）

输入 17-digit time int。`[0:8]=date [8:12]=HHMM [12:17]=SSMMM`。

```
s = str(time)
if len(s) < 12: return s[:12]               # 畸形输入兜底
if chars[12:] 全为 '0': return s[:12]        # 精确边界（SSMMM=0）→ 归自身分钟（左开右闭）
# 否则 round-up：HHMM +1 分钟，带进位
parse: date=chars[0:8], hh=chars[8:10], mm=chars[10:12]
mm += 1
if mm == 60: mm = 0; hh += 1
if hh == 24: hh = 0; date += 1 day           # 跨日进位
return f"{date}{hh:02d}{mm:02d}"
```

Python 用 `datetime.strptime(s[:12])+timedelta(minutes=1)`（进位免费）；Rust 无 chrono，
手写进位 + `increment_yyyymmdd`（闰年/月日）。两者输出逐字节一致。

**实现方式**：字符串解析 hh/mm → 整数 +1 → 重格式化。明确**不用**整数运算（如 `time // 100000 + 1`）
——整数除法无法处理 mm=59 进位到 hh 的 60 进制，且会在 0959→1000 时产生 `0960` 这类非法 minute_key。
字符串解析 + 条件进位是唯一正确方式。

### 4.2 改动文件

| 文件 | 改动 |
|------|------|
| `src/minute_bar/clock.py:49` | `time_to_minute_key` → floor+1（含跨小时/跨日进位） |
| `order_accel/src/lib.rs:1277` | `time_to_minute_key` → floor+1（与 Python 同构） |
| `src/minute_bar/clock.py:41` `minute_key_to_end_time` | **改实现**：返回 `M 时刻`（label=截止），见 §5 |
| `src/minute_bar/clock.py:70` `minute_key_to_start_time` | **改实现**：返回 `M 时刻 − 1 分钟`，见 §5 |
| cross-day handler（flusher.py `_step1_cross_day_check`） | **审查** 1531/次日 0000 处理（见 §6） |

### 4.3 不需要改动的（验证）

- `is_expired` / `is_data_driven_expired`（clock.py:61,77）：相对比较，随 `end_time`/`start_time`
  一致平移后 wall-clock 时机不变，见 §5.3。✅
- `select_tickfile_records`（tickfile.py:204）：同分钟内取最早记录，minute_key 已一致 rounded → 不变。✅
- order/snapshot/tickfile 输出：自动用新 minute_key 分组，无需逐处改。✅

## 5. 区间语义与 `minute_key_to_end_time` / `start_time` 改动

### 5.1 区间语义（用户确认）

round-up 下，minute_key 的 **label 命名的是该分钟的截止时刻（右开边界，不含）**。
bar 0901 覆盖时钟分钟 0900 的数据，区间 `[09:00:00.000, 09:01:00.000)` **左闭右开**：

- 因为 floor+1 且"整点也+1"，`09:00:00.000` 也归 0901（区间左闭起点）
- label `0901` = 截止时刻 `09:01:00.000`，**永不用未来数据**：0901 只能用 `< 09:01:00.000`
  的数据；`09:01:00.000` 一到，0901 截止、该 flush

一般化：bar `M` 覆盖区间 `[(M-1分钟):00:00.000, M:00:00.000)`，label `M` = 右开截止边界。

### 5.2 两个函数必须改实现（不是只改注释）

当前实现（round-down 下正确）：
| 函数 | 当前返回 | bar 0900 例子 |
|------|---------|--------------|
| `minute_key_to_end_time(M)` | `M 时刻 + 1 分钟` | end_time(0900) = 09:01:00（区间右开边界） |
| `minute_key_to_start_time(M)` | `M 时刻` | start_time(0900) = 09:00:00（区间左闭起点） |

round-up 下必须改为（指向正确的时钟分钟边界）：
| 函数 | 改为返回 | bar 0901 例子 |
|------|---------|--------------|
| `minute_key_to_end_time(M)` | **`M 时刻`**（label = 截止） | end_time(0901) = 09:01:00 |
| `minute_key_to_start_time(M)` | **`M 时刻 − 1 分钟`** | start_time(0901) = 09:00:00 |

即：`end_time` 从 `+1min` 改为 `+0`，`start_time` 从 `+0` 改为 `−1min`。两者一致地平移 −1 分钟，
使它们重新对齐 bar 的真实数据区间。

### 5.3 为什么 `is_expired` / `is_data_driven_expired` 仍正确

- `is_expired(M, delay)` = `end_time(M) + delay <= now_jst()`：round-up 下 end_time(0901)=09:01:00，
  bar 0901 的最后数据在 `< 09:01:00` 到达 → now≥09:01:00+delay 时 flush。与 round-down 下
  bar 0900（end_time=09:01:00）的 flush 时刻**完全相同**（wall-clock 不变）。✅
- `is_data_driven_expired(M, w, delay)` = `start_time(w) >= start_time(M)+delay`：纯相对比较，
  start_time 一致平移 → 关系不变。✅

**关键不变量**：改 `time_to_minute_key`(+1) + 改 `end_time`/`start_time`(−1min) 后，
flush 的 wall-clock 时机和 watermark 推进关系**与 round-down 完全一致**，只是分钟 label 整体 +1。

## 6. 1531 / 跨日处理审查

15:30:xx 的记录 floor+1 → **1531**。1531 超出 TSE 交易时段（0900-1500 + 午休）。
更极端：15:30:01 → 1531；若某条 23:59:xx（不应出现但防御）→ floor+1 = 次日 0000。

**需确认/审查**：
- cross-day handler（`_step1_cross_day_check`）按 `minute_key[:8]` (date) 判定跨日。
  1531 的 date 仍是当日 → 不触发跨日，作为当日最后分钟正常 flush。✅
- 次日 0000（仅当 23:59 数据）→ date 进位 → 触发跨日 flush，清理前日 buffer。
  这与现有跨日逻辑一致，但需验证 0000 这种异常分钟不破坏 flush。⚠️ 需测试

**实现策略**：round-up 算法本身按规则算（含跨日进位），不在 `time_to_minute_key` 内做封顶/过滤。
异常分钟（1531/次日0000）由下游的现有 trading-session 过滤（`is_trading_minute`）处理（若启用）。

## 7. 正确性 Invariants

1. **同时间戳一致**：相同 `time` 在 order 路径和 snapshot 路径算出**相同** minute_key。
2. **floor+1 单调**：`t1 < t2`（同分钟内）⇒ 同 minute_key；跨分钟边界 ⇒ minute_key +1。
3. **整点也 +1**：`x:00:00.000` → `x+1:00`（无边界特判）。
4. **进位正确**：mm=59→hh+1，hh=24→date+1（跨日）。
5. **flush 时机不变**：`time_to_minute_key`(+1) + `end_time`/`start_time`(−1min) 配套改后，`is_expired`/
   `is_data_driven_expired` 的 wall-clock flush 时机与 watermark 推进关系与 round-down 完全一致（§5.3）。
6. **输出文件一致**：order/snapshot/tickfile 同一分钟的所有记录落同一 minute_key 文件。

## 8. 测试计划（TDD）

### 8.1 单元测试（先写失败 → 实现 → 通过）
- `test_time_to_minute_key_round_up`：
  - 09:00:01.000 → 0901
  - 09:00:59.000 → 0901
  - 09:01:00.000 → 0902（整点也+1）
  - 09:59:01.000 → 1000（跨小时）
  - 15:30:01.000 → 1531（超出，允许）
- `test_round_up_cross_day`：构造 hh=23 mm=59 → 次日 0000（防御性）
- `test_round_up_order_snapshot_consistent`：相同 time 经 order(Rust) 和 snapshot(Python) 路径得同 key
- `test_end_time_is_label_moment`：`end_time("0901")` 时刻 = 09:01:00（不是 09:02:00）
- `test_start_time_is_label_minus_one`：`start_time("0901")` 时刻 = 09:00:00（不是 09:01:00）

### 8.2 Golden 测试更新
现有 golden 测试（`test_order_batch_golden.py` 等）断言的 minute_key 全部 +1：
- 0900 → 0901，0901 → 0902，等
- 需逐个更新期望值（这是预期的语义变更，非回归）

### 8.3 回归 + flush-timing 不变量
- `test_order_accel.py` / `test_snapshot_ohlcv_golden.py` / `test_tickfile_rust_golden.py` 全通过（更新期望后）
- **flush-timing 测试**：构造 watermark 推进序列，断言某分钟被 flush 的 wall-clock/data 时刻与
  round-down 实现一致（验证 §5.3 不变量——不是 label 偏移，是真实 flush 时机）
- clock.py 相关测试（`is_expired` 等）更新期望后全通过

### 8.4 端到端验证（tickfile 质量）
重跑 full_day_run，检查准则 #1/#2/#3：
- #3：09:00:01 记录落在 0901 分钟文件 ✅
- #2：每分钟 tickfile = snapshot + order 最早记录合成 ✅
- #1：每分钟 symbol 覆盖（无 snapshot+order 的 symbol 仍跳过，需确认是否可接受）

## 9. 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| Golden 测试期望值大量变动 | 高 | 低 | 预期的语义变更，逐个更新 |
| 跨日 1531/0000 破坏 flush | 中 | 中 | §6 测试覆盖；异常分钟由 session 过滤兜底 |
| `minute_key_to_end_time` 绝对值被外部消费 | 低 | 中 | §5 审查调用方；仅注释或修正 |
| order(Rust) 与 snapshot(Python) 算法不一致 | 中 | 高 | §7 invariant 1 + 双向一致性测试 |
| 现有 checkpoint/恢复路径假设 round-down | 低 | 高 | checkpoint 存的是 rounded key，恢复重建也用新规则 → 一致 |

## 10. 范围外

- tickfile stale 行修复（Q1，独立 spec）
- 性能优化（round-up 字符串解析比切片略慢，可忽略）

## 11. 相关文档

- `[[phase21-status]]` — Phase 21 Rust 去 GIL 阶段完成
- `[[tickfile-shutdown-forcegen-orderless]]` — Q1 stale（独立问题）
- brainstorming 对话：2026-06-16，规则全部用户确认
