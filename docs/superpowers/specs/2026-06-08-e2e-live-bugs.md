# E2E Live Test Bug Report — 2026-06-08

> **Test Date**: 2026-06-08
> **Data Date**: 20260528 (日股)
> **Config**: `config/test-tickfile-live.ini` (tickfile=on, poll=2ms, stall_flush=30s)
> **Simulator**: `--speed 100 --file-types order,snapshot,code --order-mode original`
> **Source Data**: 87,521,294 orders + 5,387,284 snapshots + 13,834 codes

---

## 🔴 P0 — Tickfile 缺失 24 个分钟（7.3% 数据丢失）

**Status**: 待修复
**File**: `src/minute_bar/engine.py:778-819`, `src/minute_bar/flusher.py:103-133,410-436`
**Impact**: Tickfile 不完整，缺失交易日约 7.3% 的分钟。**生产环境直接受影响。**

### 现象

329 个有 order + snapshot 数据的分钟中，tickfile 只生成了 305 个，缺失 24 个：

```
0900, 0906, 0908, 0915, 0923, 0930, 0938, 0951,
1022, 1036, 1056,
1113, 1114, 1115, 1116, 1117, 1118, 1119,  ← 连续 7 个!
1245, 1307, 1329, 1352, 1420, 1501
```

每个缺失分钟都有完整的 snapshot 文件（4505 symbols）和 order 文件（数十万条记录），但 tickfile 未生成。

### 关键证据 (Engine Log)

以 minute 0900 为例：

```
15:47:23 Wrote snapshot ...snapshot_minute_20260528_0900.csv (17556 rows, 4505 symbols)
15:47:24 Re-routed 15830 snapshot records to late queue (dropped 1423 ohlcv symbols)
...
15:50:43 Output order minute 202605280900 (747434 records)
15:50:44 Tickfile pending: 303 total, 0 eligible (order_watermark=202605280900)
...
15:50:57 Output order minute 202605280901 (562269 records)
15:50:59 Tickfile generated minute=202605280901 (4505 symbols)  ← 0900 被跳过!
```

**观察**:
1. Snapshot 0900 在 15:47:23 写入 → `_tickfile_pending["0900"]` 被设置
2. Order 0900 在 15:50:43 flush → `_tickfile_trigger_pending` 包含 "0900"
3. 但 "Tickfile pending: 303 total, **0 eligible**" — clock thread 的 eligibility 判断 `order_watermark > mk`（**严格大于**）导致 0900 不 eligible
4. Order 0901 在 15:50:57 flush → `_drain_tickfile_triggers()` 中 `mk <= latest` 应捕获 0900
5. **但 tickfile 只生成了 0901，0900 被跳过**

### 根因分析

涉及三条线程的竞态：

| 线程 | 触发路径 | Eligibility 条件 | 调用点 |
|------|----------|-------------------|--------|
| **order-thread** | `_drain_tickfile_triggers()` | `mk <= latest` (≤) | `engine.py:800` |
| **clock-thread** | `flusher.tick()` overflow | `order_watermark > mk` (>) | `flusher.py:118` |
| **snapshot-flusher** | `_flush_minutes_internal()` | `order_current_minute > mk` (>) | `flusher.py:432` |

**竞态场景**:
1. Snapshot flusher 写入 0900 → `_tickfile_pending["0900"]` 设置 (flusher.py:421)
2. Clock thread tick() 检查 overflow → `order_watermark="0900"` → `0900 > 0900 = False` → 不 eligible
3. Order thread flush 0900, 0901 → `_drain_tickfile_triggers()`:
   - `triggers = ["0900", "0901"]`
   - `latest = "0901"`
   - 遍历 `_tickfile_pending`: `mk <= "0901"` → 添加 0900（已在 triggers 中则跳过）
   - 全部入队: `queue.put("0900")`, `queue.put("0901")`
4. Tickfile writer 出队 "0900" → `_try_generate_tickfile("0900")`:
   - `_tickfile_pending.pop("0900")` → **返回 None!** ← 竞态发生点
   - `if pending is None: return` → **静默跳过，无任何日志**

**`_tickfile_pending.pop("0900")` 返回 None 的原因推测**:
- Clock thread 的 `flusher.tick()` overflow protection (flusher.py:131-133) 可能在 step 2 和 step 4 之间也触发了 `_enqueue_tickfile("0900")`，导致 tickfile writer 提前 pop 了数据
- 或 `_tickfile_pending` 被其他路径清除（如 cross-day check 或 late record 处理）

**关键代码缺陷**: `_try_generate_tickfile` (flusher.py:541-543) 在 pending 为 None 时**无任何日志**，导致问题极难追踪：
```python
pending = self._state._tickfile_pending.pop(minute_key, None)
if pending is None:
    return  # ← 静默跳过！应该 log WARNING
```

### 修复建议

1. **添加日志**: `_try_generate_tickfile` 在 `pending is None` 时至少输出 DEBUG 级别日志
2. **审查竞态**: 分析 `_tickfile_pending` 的所有 pop/remove 路径，确认是否有非 tickfile-writer 线程在 pop
3. **考虑 double-enqueue 防护**: tickfile writer 在 dequeue 时用 `set` 去重，或用 `threading.Event` 标记已处理的分钟
4. **添加 tickfile 完整性校验**: engine shutdown 时对比 `flushed_snapshot_minutes` 与 tickfile 实际生成的分钟集合，输出差集 WARNING

### 验证方法

```bash
# 运行 E2E 测试后对比
grep "Tickfile generated minute=" test/e2e_engine.log | wc -l  # 应 = snapshot 分钟数
# 对比缺失的分钟
python -c "
import os, csv
snap_dir = 'test/tickfile_live_output/snapshot/2026/20260528'
tf_path = 'test/tickfile_live_output/tickfile/2026/20260528/tickfile_20260528.csv'
snap_mins = {f.split('_')[-1].replace('.csv','') for f in os.listdir(snap_dir) if f.endswith('.csv')}
with open(tf_path) as f:
    reader = csv.reader(f)
    next(reader)
    tf_mins = {row[16].split(' ')[1].replace(':','')[:4] for row in reader}
missing = sorted(snap_mins - tf_mins)
print(f'Missing: {len(missing)} minutes: {missing}')
"
```

---

## 🟡 P1 — Order 数据丢失 13.4%（`MAX_LATE_ORDER_RECORDS_PER_MINUTE=50000` 截断）

**Status**: 测试环境放大问题，生产环境影响待评估
**File**: `src/minute_bar/engine.py:42,621-624`
**Impact**: Order 输出 75.8M vs 源 87.5M，丢失 11,747,893 条记录 (13.4%)。交易时段每分钟丢失 100K-700K 条。

### 现象

| 指标 | 值 |
|------|-----|
| 源 order 记录 | 87,521,294 |
| 输出 order 记录 | 75,773,401 |
| 丢失 | 11,747,893 (13.4%) |
| 受影响分钟数 | 交易时段全部 242 分钟 |

示例（分钟级丢失量）:
```
0900: 750,975 → 750,975 (0%)     ← 无丢失
0901: 562,288 → 562,288 (0%)     ← 无丢失
0902: 567,807 → 286,906 (49.5%)  ← 丢失一半
0903: 759,890 → 521,604 (31.4%)
0907: 619,115 → 107,208 (82.7%)  ← 丢失最多
```

### 根因

**源数据时间戳严重交错**: 源 order.csv 按 `rcvtime`（接收时间）排序，但 engine 使用 `time`（交易所时间戳）做分钟归属。不同交易所的时钟不同步导致同一分钟的记录在文件中散布在数百万行的范围内。

量化分析（对源数据全量扫描）:
```
Total late records (ts < max_minute_seen): 87,513,797 / 87,521,294 = 99.99%
Records exceeding 50000 cap per minute: 几乎所有交易时段分钟
Total would be dropped: ~70M (理论值，实际取决于 FileTailer 读取粒度)
```

**代码路径** (`engine.py:621-624`):
```python
MAX_LATE_ORDER_RECORDS_PER_MINUTE = 50000  # line 42

if minute_key in self._flushed_order_minutes:
    count = late_order_per_minute.get(minute_key, 0)
    if count >= MAX_LATE_ORDER_RECORDS_PER_MINUTE:
        continue  # ← 静默丢弃！
```

**时间戳交错样例** (源 order.csv 第 2,137,888-2,138,069 行):
```
Line 2137888: 0901 → 0902 (ts=20260528090200000)
Line 2137894: 0902 → 0901 (ts=20260528090159999)  ← 回退！
Line 2137896: 0901 → 0902 (ts=20260528090200000)
Line 2137915: 0902 → 0901 (ts=20260528090159994)  ← 再回退！
```

### 生产环境影响评估

| 场景 | 交错程度 | 数据丢失风险 |
|------|----------|---------------|
| **生产实时** | FileTailer 每次读取几秒数据，交错极小 | **低** |
| **Replay 模式** (`--replay`) | 按源文件顺序，与 live 测试相同 | **高** |
| **高倍速 Live 测试** (100x) | FileTailer 每次读取大量数据，交错被放大 | **极高** |

### 修复建议

1. **短期**: 调高 `MAX_LATE_ORDER_RECORDS_PER_MINUTE` 到 1,000,000 或改为可配置（ini 文件）
2. **中期**: E2E 测试使用 `--order-mode time` 按时间戳排序，消除交错
3. **长期**: Order thread 改用与 snapshot 相同的 watermark-based flush 机制，避免基于文件顺序的分钟推进
4. **立即**: 在 `continue` 前添加 DEBUG 日志记录丢弃的记录数

---

## 🟢 P2 — Snapshot Late Queue 溢出（1 条记录丢失）

**Status**: 低影响，记录备查
**File**: `src/minute_bar/aggregator.py`
**Impact**: 仅丢失 1 条 snapshot 记录 (symbol=2522, minute=0900)

### 现象

```
[WARNING] Late snapshot queue full (50000), dropping record for minute=202605280900 symbol=2522
```

### 根因

Snapshot late queue 容量固定为 50,000。在 100x 回放速度下，短时间内涌入大量跨分钟 snapshot 记录，超出队列容量。

### 修复建议

- 考虑将 queue 大小改为可配置
- 或在 queue 满时 log WARNING 但仍然处理（降级为同步处理而非丢弃）

---

## 🟢 P2 — Snapshot Re-routing Race Window（多次触发，OHLCV 数据丢失）

**Status**: 已知问题（Phase 15b 修复后的残留），记录备查
**File**: `src/minute_bar/flusher.py`
**Impact**: 每次触发丢失数百到数千个 symbol 的 OHLCV 数据，但 snapshot 文件仍通过 late path 补全

### 现象

```
Re-routed 15830 snapshot records to late queue for 1 already-flushed minutes (dropped 1423 ohlcv symbols)
Re-routed 34342 snapshot records to late queue for 1 already-flushed minutes (dropped 1803 ohlcv symbols)
...共 18 次触发，每次 dropped 428-1803 ohlcv symbols
```

### 根因

Clock thread 的 data-driven flush 和 snapshot 数据到达之间的 race window。当 flusher 检测到一个分钟的 OHLCV 已经被另一个线程 flush 过，它会将新数据 re-route 到 late queue，但 drop 掉部分 OHLCV 更新。

### 影响评估

- Snapshot 文件最终通过 late path 补全（每分钟 4505 symbols 完整）
- 但 re-route 过程中丢失的 OHLCV 可能影响 tickfile 中的快照数据准确性
- 与 P0 bug 可能存在关联（re-route 导致 `_tickfile_pending` 中的 snapshot_copy 不完整）

---

## 测试环境信息

```
Platform: Windows 11 Enterprise
Python: 3.x
Engine threads: data-thread, clock-thread, order-thread, tickfile-writer
Engine runtime: ~53 minutes (15:46:33 → 16:39:31)
Simulator: completed in ~2 minutes

Output files:
  Order:   417 files (0800-1130, 1205-1530), 75.8M records
  Snapshot: 329 files (0800, 0830, 0900-1130, 1230-1524, 1530), 6.35M rows
  Tickfile: 1 file, 405MB, 305 seqnos, 1,373,978 rows, 4505 symbols
  Checkpoint: test/checkpoint_tickfile.json

Engine log: test/e2e_engine.log
```

---

## 优先级排序

| Priority | Bug | Impact | Fix Complexity |
|----------|-----|--------|----------------|
| **P0** | Tickfile 缺失 24 分钟 | 生产环境数据不完整 | 中（竞态分析 + 日志 + 去重） |
| **P1** | Order 丢失 13.4% | 测试环境放大，生产影响待评估 | 低（调参）- 高（重设计） |
| **P2** | Snapshot late queue 溢出 | 仅 1 条记录丢失 | 低 |
| **P2** | Snapshot re-routing race | 已知残留问题 | 中 |
