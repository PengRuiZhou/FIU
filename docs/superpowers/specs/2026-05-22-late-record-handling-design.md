# Late Record 统一处理机制设计文档

## 概述

FIU 原始文件（snapshot.csv / order.csv）不保证按交易所 time 全局有序。数据按 symbol 分批写入、网络/线程调度延迟、上游推送乱序等因素，导致 Live 和 Replay 模式下均可能出现：**某个 minute 已 flush 后，仍收到该 minute 的 record**。

当前实现中，flush 后 pop 掉 buffer，迟到数据重建新 buffer，但 `flushed_minutes` 检查会跳过这些 buffer（Replay EOF）或 `atomic_write` 覆盖已有文件（Live watermark 重复 flush），导致数据丢失。

实测 0519 数据丢失量：
- Snapshot: 5,210 条（0.094%）
- Order: 1,818 条（0.002%）

本设计将 "Replay EOF 补丁" 升级为 **Live + Replay 统一 late record 处理机制**，保证 0 数据丢失。

---

## 1. Late Record 定义

```text
late record = record.minute_key in flushed_minutes（对应输出类型）
```

- Snapshot 和 Order 分别维护 `flushed_minutes: Set[str]`
- `flushed_minutes` 在 flush 成功（文件写入完成）后 add 对应 minute_key
- Live 和 Replay 使用同一判断逻辑

---

## 2. 处理规则总表

| 类型 | Late Record 处理 | Kline | latest_snapshot | 说明 |
|------|-----------------|-------|-----------------|------|
| **Order** | 追加行到已有 order_minute 文件 | N/A | N/A | Order record 是独立事件，append 语义自然 |
| **Snapshot** | 追加原始行到已有 snapshot_minute 文件，`update_flag="Y"` | 不重算 | 若新于当前则更新 | 保证原始 record 不丢，carry-forward 行不回写 |
| **Kline** | 不自动重写 | — | — | 需严格修正时走离线补算 |

### 2.1 业务口径决策

1. **已输出的 carry-forward rows 不回写**：late record 只追加原始行，不修改已输出的无交易 carry-forward 行
2. **已输出的 kline 不自动重算**：late record 对 OHLCV 的影响 <0.1%，不自动修正
3. **需要严格修正时走离线补算流程**：后续提供离线工具重算指定分钟的 kline/snapshot

### 2.2 snapshot_minute 文件同 symbol 多行规则

Late append 后，同一个 symbol 在同一个 snapshot_minute 文件中可能同时存在：

```csv
seqno,symbol,...,update_flag
123,1301,...,N     ← 原 carry-forward 行（无交易）
456,1301,...,Y     ← 后 append 的 late record（真实更新）
```

这不是错误，但下游读取时必须明确解释规则：

- `update_flag=Y` 代表真实 late record（含实际交易/价格变动）
- `update_flag=N` 是早前全市场补齐的 carry-forward 行
- 下游如需该分钟真实更新，应优先使用 `update_flag=Y` 的行
- 下游如需唯一状态行，应按 `time/rcvtime` 取最新，或排除 `update_flag=N`
- 每个 snapshot_minute 文件中同一 symbol **不再保证只有一行**

---

## 3. 处理流程

### 3.1 统一入口

```python
def process_record(record, minute_key, flushed_minutes, record_type):
    if minute_key in flushed_minutes:
        handle_late_record(record, minute_key, record_type)
    else:
        append_to_active_buffer(record, minute_key)
```

### 3.2 Order Late Record

```python
def handle_late_order(record, minute_key):
    path = get_order_file_path(minute_key)
    if os.path.exists(path):
        append_order_records(path, [record])  # 不写 header
    else:
        logger.warning("Late order minute %s file missing, creating new", minute_key)
        write_order_file(output_dir, minute_key, [record])  # 完整写入含 header
    late_order_count += 1
    late_order_minutes.add(minute_key)
```

### 3.3 Snapshot Late Record

```python
def handle_late_snapshot(record, minute_key):
    # 1. 追加原始行到 snapshot_minute 文件
    path = get_snapshot_file_path(minute_key)
    if os.path.exists(path):
        name = code_table.get_name(record.symbol)
        append_snapshot_records(path, [record], name)  # update_flag="Y"
    else:
        logger.warning("Late snapshot minute %s file missing, creating new", minute_key)
        # 极端情况：写完整文件
        write_snapshot_file(output_dir, minute_key, ..., raw_records={record.symbol: [record]})

    # 2. 更新 latest_snapshot + 计数（必须在 SharedState.lock 内）
    #    见 3.4 和 5.1 中 _step4 的实现

    late_snapshot_count += 1
    late_snapshot_minutes.add(minute_key)
```

### 3.4 latest_snapshot 更新规则（调用方持锁）

`latest_snapshot` 是被 data thread、flusher、checkpoint 并发访问的共享状态。所有调用方在调用前必须已持有 `SharedState.lock`。

作为 `SharedState` 的 unlocked 方法实现，方法本身不加锁，由调用方保证在 `with self._state.lock:` 块内调用：

```python
class SharedState:
    def maybe_update_latest_unlocked(self, record: SnapshotRecord) -> None:
        """调用方必须已持有 self.lock。仅在 record 新于当前值时更新 latest_snapshot。"""
        current = self.latest_snapshot.get(record.symbol)
        if current is None:
            self.latest_snapshot[record.symbol] = record
        elif (record.time, record.rcvtime) >= (current.time, current.rcvtime):
            self.latest_snapshot[record.symbol] = record
        # 若 record 时间戳更旧，不回退 latest
```

---

## 4. Writer Append 支持

### 4.1 新增函数

`writer.py` 新增两个 append 函数：

```python
def append_order_records(path: str, records: List[OrderRecord]) -> None:
    """追加 order 行到已有文件，不写 header"""
    with _get_write_lock(path):
        with open(path, "a", encoding="utf-8", newline="") as f:
            for rec in records:
                f.write(_format_order_row(rec) + "\n")

def append_snapshot_records(path: str, records: List[SnapshotRecord], code_table: CodeTable) -> None:
    """追加 snapshot 原始行到已有文件，不写 header，update_flag=Y"""
    with _get_write_lock(path):
        with open(path, "a", encoding="utf-8", newline="") as f:
            for rec in records:
                name = code_table.get_name(rec.symbol)
                f.write(_format_snapshot_row(rec, name, "Y") + "\n")
```

### 4.2 写入锁

按文件路径粒度加锁，防止并发 append + flush 冲突：

```python
_write_locks: Dict[str, threading.Lock] = {}
_write_lock_mutex = threading.Lock()

def _get_write_lock(path: str) -> threading.Lock:
    with _write_lock_mutex:
        if path not in _write_locks:
            _write_locks[path] = threading.Lock()
        return _write_locks[path]
```

Live 模式下 flusher 的 `atomic_write` 也需要走同一个文件锁：

```python
def atomic_write(path: str, content: str) -> None:
    with _get_write_lock(path):
        tmp_path = path + ".tmp"
        # ... 原有逻辑不变
```

### 4.4 Append 失败处理

Late append 的目标是保证 0 数据丢失。如果 append 失败后继续运行，会重新引入静默丢数据风险。因此：

- **Replay**：append 失败视为不可恢复错误，整体失败，不生成 success summary。重新运行即可恢复。
- **Live**：append 失败记录 FATAL 日志，触发 Engine 停止（与当前 `write_order_file` / `write_snapshot_file` 失败处理一致）

**Live 数据一致性要求**：

Live append 失败时，必须保证对应输入文件的 checkpoint / committed offset 不推进。否则会出现：

```text
late record 已读取 → append 失败 → checkpoint 却推进到该 record 之后 → 重启后从 checkpoint 继续 → 这条 late record 永久丢失
```

统一原则：**对于 late record，只有在 late append 成功后，才允许将对应输入 offset 视为 committed。**

具体要求：
1. late record 被读取后，只有在 append 成功后，才允许将该 record 对应的 offset 视为已提交
2. append 失败路径不得触发 checkpoint 写入
3. 重启后应从失败 record 之前的 offset 重新读取，并重新执行 late append
4. 对 order，继续使用现有 `committed_offset` 机制（`_flush_order_minute` 成功后才推进）
5. 对 snapshot，checkpoint 写入必须发生在 late append 成功之后，避免读取成功但落盘失败的数据被跳过

**Snapshot checkpoint 安全约束**：

如果 snapshot data thread 已读到 late record 并放入 `_late_snapshot_records`，FileTailer 的 read offset 可能已经前进了。因此 checkpoint 不能简单记录当前 read offset，而必须确认 late queue 已成功落盘：

```text
Snapshot checkpoint 写入前，必须确认 `_late_snapshot_records` 已成功 append 并清空；
如果 late append 失败，禁止写入包含该 late record 之后读取 offset 的 checkpoint；
否则重启后 late queue 丢失（内存态），record 也不会重读（offset 已跳过）。
```

CheckpointManager 写入 checkpoint 前，应在 `SharedState.lock` / `checkpoint_lock` 保护下确认：
1. `_late_snapshot_records` 为空
2. 当前没有正在执行中的 late append（无 in-flight）
3. 最近一次 late append 没有失败标记

否则不得推进 snapshot offset，本轮 checkpoint 应延后。

**Writer 层职责边界**：

```text
writer 层只负责抛出 append 异常，不直接推进 checkpoint，也不直接退出进程；
checkpoint 推进和进程退出由上层 Engine / Replay 控制。
```

**Live 停止后的恢复要求**：

1. **进程级重启能力**：Engine 应由外部进程管理器（systemd / supervisor / Docker restart policy）管理，FATAL 退出后自动重启，退出码非 0
2. **重启后状态恢复**：
   - 通过 Section 5.3 的 `recover_flushed_minutes()` 恢复已输出分钟集合
   - 通过 checkpoint 恢复读取 offset
   - 对 append 失败的 late record，由于 checkpoint 未推进，重启后会重新读取并重新 append
3. **告警通知**：FATAL 日志应触发运维告警（日志监控 / 告警规则），确保人工介入

```text
Live 部署要求：
- 进程管理器配置 restart on failure
- FATAL 退出必须返回非 0 exit code
- 日志监控对 FATAL 级别触发告警
- append 失败不得推进 checkpoint
- 重启后 recover_flushed_minutes + checkpoint 恢复，确保失败 record 可重放
```

```python
def append_order_records(path: str, records: List[OrderRecord]) -> None:
    try:
        with _get_write_lock(path):
            ...
    except IOError as e:
        logger.fatal("Late append failed for %s: %s", path, e)  # 或 logger.critical，取决于日志库
        raise  # 上层 Engine 负责终止，进程管理器负责重启
        # 注意：调用方不得在此之后推进 checkpoint / committed offset
```

### 4.3 文件路径计算

为避免路径计算逻辑重复，提取公共函数：

```python
def get_snapshot_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "snapshot", date_str[:4], date_str,
                        f"snapshot_minute_{date_str}_{hhmm}.csv")

def get_order_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "order", date_str[:4], date_str,
                        f"order_minute_{date_str}_{hhmm}.csv")
```

---

## 5. Live 模式改动

### 5.1 Flusher（Snapshot Late Record）

`flusher.py` `_step3_minute_output` 中，pop expired buffers 后，后续 tick 中检测 buffer 内是否有已 flush 分钟的数据：

方案 A（推荐）：在 `process_snapshot` 阶段直接判断 late record 并处理。

`aggregator.py` `process_snapshot()` 修改：

**注意**：`flushed_snapshot_minutes` 和 `_late_snapshot_records` 都属于 `SharedState`，late 判断和追加必须在同一把 `SharedState.lock` 内原子完成，否则 flusher 刚写完加入 `flushed_snapshot_minutes` 的同时，data thread 可能按旧状态把记录放进普通 buffer。

```python
def process_snapshot(self, parsed: ParsedSnapshot) -> bool:
    # ... 原有逻辑（在 self.lock 内） ...
    minute_key = time_to_minute_key(record.time)

    # late 判断和追加必须与 flushed_snapshot_minutes 在同一把锁内
    is_late = minute_key in self.flushed_snapshot_minutes
    if is_late:
        self._late_snapshot_records.append((minute_key, record))
        return True
    else:
        # 正常 buffer 逻辑
        ...
```

Flusher tick 中增加 late record 处理步骤：

```python
def tick(self) -> None:
    self._step1_cross_day_check()
    if not self._step2_first_data_check():
        return
    self._step3_minute_output()
    self._step4_handle_late_records()  # 新增

def _step4_handle_late_records(self) -> None:
    with self._state.lock:
        late_records = self._state.pop_late_snapshot_records()

    if not late_records:
        return

    grouped: Dict[str, List[SnapshotRecord]] = {}
    for minute_key, record in late_records:
        grouped.setdefault(minute_key, []).append(record)

    # 1. 文件 append（无需持锁，append 函数内部有文件级锁）
    for minute_key in sorted(grouped):
        path = get_snapshot_file_path(self._output_dir, minute_key)
        if os.path.exists(path):
            append_snapshot_records(path, grouped[minute_key], self._code_table)
        else:
            logger.warning("Late snapshot minute %s file missing", minute_key)

    # 2. 更新 latest_snapshot + 计数（必须在 SharedState.lock 内）
    with self._state.lock:
        for records in grouped.values():
            for record in records:
                self._state.maybe_update_latest_unlocked(record)
                self._state.late_snapshot_count += 1
                self._state.late_snapshot_minutes.add(time_to_minute_key(record.time))

    logger.info("Flushed %d late snapshot records across %d minutes",
                sum(len(v) for v in grouped.values()), len(grouped))
```

### 5.2 Engine Order Loop（Order Late Record）

`engine.py` `_order_loop` 中，`_flush_order_minute` 后 minute 从 buffers 中 pop。后续 record 到达时，需检测是否为 late record：

```python
# _order_loop 中 record 处理部分
minute_key = time_to_minute_key(parsed.time)

if minute_key in self._flushed_order_minutes:  # 新增
    # Late record: 直接 append
    record = build_order_record(parsed, seqno)
    path = get_order_file_path(output_dir, minute_key)
    if os.path.exists(path):
        append_order_records(path, [record])
    else:
        logger.warning("Late order minute %s file missing, creating new", minute_key)
        write_order_file(output_dir, minute_key, [record])
    self._late_order_count += 1
    self._late_order_minutes.add(minute_key)
    continue

# 正常 buffer 逻辑
...
```

`_flush_order_minute` 中记录 flushed minute：

```python
def _flush_order_minute(self, buffers, minute_key, output_dir):
    # ... 原有逻辑 ...
    self._flushed_order_minutes.add(minute_key)  # 新增
```

### 5.3 Live 重启恢复 flushed_minutes

程序重启后 `flushed_snapshot_minutes` / `_flushed_order_minutes` 为空，迟到 record 会被误判为普通 buffer 而非 late record，导致重复输出或覆盖已有文件。

**恢复策略：启动时从输出目录扫描已有文件重建 flushed_minutes。**

选择扫描文件而非持久化到 checkpoint 的理由：
- flushed_minutes 与实际输出文件状态保持一致，避免 checkpoint 与文件不一致
- 不增加 checkpoint 序列化复杂度

```python
def recover_flushed_minutes(output_dir: str, date: str) -> tuple[Set[str], Set[str]]:
    """从已有输出文件恢复 flushed_snapshot_minutes / flushed_order_minutes。"""
    snapshot_minutes = set()
    order_minutes = set()

    date_str = date
    snap_dir = os.path.join(output_dir, "snapshot", date_str[:4], date_str)
    if os.path.isdir(snap_dir):
        for f in os.listdir(snap_dir):
            # snapshot_minute_20250519_0930.csv → 202505190930
            m = re.match(r"snapshot_minute_(\d{8})_(\d{4})\.csv$", f)
            if m:
                snapshot_minutes.add(m.group(1) + m.group(2))

    order_dir = os.path.join(output_dir, "order", date_str[:4], date_str)
    if os.path.isdir(order_dir):
        for f in os.listdir(order_dir):
            m = re.match(r"order_minute_(\d{8})_(\d{4})\.csv$", f)
            if m:
                order_minutes.add(m.group(1) + m.group(2))

    return snapshot_minutes, order_minutes
```

**Engine 启动时调用：**

```python
# Engine.start() 或 __init__ 中
snapshot_mins, order_mins = recover_flushed_minutes(output_dir, today)
self._state.flushed_snapshot_minutes = snapshot_mins
self._flushed_order_minutes = order_mins
```

**Replay 不需要此逻辑**：Replay 每次从空 flushed_minutes 开始，因为是单次从头处理完整文件。

### 5.4 跨日清理

Live 模式长期运行时，以下集合不能无限增长，跨日时必须清理并按新交易日重新初始化：

- `flushed_snapshot_minutes` / `_flushed_order_minutes`
- `late_snapshot_minutes` / `_late_order_minutes`
- `late_snapshot_count` / `_late_order_count`（归零前记录到日志）

清理时机：`flusher.py` `_step1_cross_day_check()` 跨日重置时同步清理。新日期的 `flushed_minutes` 从空开始（新日尚无输出文件）。

```python
# _step1_cross_day_check 跨日重置部分
if late_snapshot_count > 0 or late_order_count > 0:
    logger.info("Cross-day: previous day late stats — snapshot: %d (%d mins), order: %d (%d mins)",
                late_snapshot_count, len(late_snapshot_minutes),
                late_order_count, len(late_order_minutes))
self._state.flushed_snapshot_minutes.clear()
self._state.late_snapshot_count = 0
self._state.late_snapshot_minutes.clear()
# order 侧同理
self._flushed_order_minutes.clear()
self._late_order_count = 0
self._late_order_minutes.clear()
```

---

## 6. Replay 模式改动

### 6.1 常规轮次 Late Record 检测

`replay.py` `_stream_snapshots` 中，record 到达时检测 late：

```python
for line in lines:
    parsed = parse_snapshot_line(line, ...)
    if parsed is None:
        continue
    record_date = str(parsed.time)[:8]
    if record_date != self._date:
        continue

    minute_key = time_to_minute_key(parsed.time)

    if minute_key in flushed_minutes:  # 新增：late record 检测
        late_records_by_minute.setdefault(minute_key, []).append(parsed)
        continue

    state.process_snapshot(parsed)
    current_round_minutes.add(minute_key)
    ...
```

每轮 poll 结束后处理 late records：

```python
# 处理本轮收集的 late records
if late_records_by_minute:
    self._flush_late_snapshots(late_records_by_minute, output_dir, code_table, state)
    late_records_by_minute.clear()
```

`_flush_late_snapshots()` 在 append late records 后，也必须按 `(time, rcvtime)` 规则更新 `state.latest_snapshot`，确保后续尚未输出分钟的 carry-forward 使用最新状态。更新在 `state.lock` 内调用 `maybe_update_latest_unlocked`。

### 6.2 EOF Final Flush 修改（兜底机制）

主路径是 6.1 中的即时 late append——一旦发现 `minute_key in flushed_minutes`，直接 late append，不再放入普通 buffer。

EOF flush 只处理 **尚未输出过的普通 active buffers**。如果 EOF 时仍存在已 flushed minute 的 residual buffer，说明主路径遗漏，记录 WARNING 后 late append：

```python
# EOF final flush: flush all remaining active buffers
for mk in sorted(state.ohlcv_buffers):
    if mk not in flushed_minutes:
        # 正常：首次 flush 该分钟
        self._flush_snapshot_minute(
            state, mk, output_dir, code_table,
            full_snapshot, full_kline, enable_kline, write_executor,
        )
        flushed_minutes.add(mk)
    else:
        # 兜底：该分钟已 flush 过，但 buffer 中仍有残留数据
        logger.warning("EOF: minute %s already flushed but buffer not empty — late append", mk)
        raw = state.raw_snapshot_buffers.get(mk, {})
        if raw:
            path = get_snapshot_file_path(output_dir, mk)
            for sym, recs in raw.items():
                append_snapshot_records(path, recs, code_table)
            # 兜底 append 后也要更新 latest_snapshot 和 late 统计
            with state.lock:
                for recs in raw.values():
                    for rec in recs:
                        state.maybe_update_latest_unlocked(rec)
                state.late_snapshot_count += sum(len(r) for r in raw.values())
                state.late_snapshot_minutes.add(mk)
        state.ohlcv_buffers.pop(mk, None)
        state.raw_snapshot_buffers.pop(mk, None)
```

### 6.3 Order Stream 同理

`_stream_orders` 中增加相同的 late record 检测逻辑，EOF flush 去掉 `flushed_minutes` 过滤。

---

## 7. 计数与监控

### 7.1 SharedState 新增字段

```python
# SharedState
flushed_snapshot_minutes: Set[str] = set()
late_snapshot_count: int = 0
late_snapshot_minutes: Set[str] = set()
```

### 7.2 Engine 新增字段

```python
# Engine
_flushed_order_minutes: Set[str] = set()
_late_order_count: int = 0
_late_order_minutes: Set[str] = set()
```

### 7.3 Replay Summary

`replay_summary_{date}.json` 新增字段：

```json
{
    "late_snapshot_records": 5210,
    "late_snapshot_minutes": ["202505190930", "202505190931"],
    "late_order_records": 1818,
    "late_order_minutes": ["202505190930"]
}
```

### 7.4 Live 日志

每次 late flush 输出日志：

```text
INFO: Flushed 12 late snapshot records across 3 minutes (total late: 5210)
WARNING: Late order minute 202505190930 file missing, creating new
```

---

## 8. 改动文件清单

| 文件 | 改动范围 | 复杂度 |
|------|---------|--------|
| `writer.py` | 新增 `append_order_records`、`append_snapshot_records`、`_get_write_lock`、路径函数；修改 `atomic_write` 加锁 | 中 |
| `aggregator.py` | `SharedState` 新增 late record 字段；`process_snapshot` 返回 late 状态；新增 `pop_late_snapshot_records` | 中 |
| `flusher.py` | 新增 `_step4_handle_late_records`；`tick` 中调用 | 中 |
| `engine.py` | `_order_loop` 增加 late record 检测和 append；`_flush_order_minute` 记录 flushed minute | 中 |
| `replay.py` | `_stream_snapshots` / `_stream_orders` 增加 late record 检测；EOF flush 去掉 flushed_minutes 过滤；新增 `_flush_late_snapshots` / `_flush_late_orders` | 中 |
| `replay.py` summary | 新增 late record 统计字段 | 低 |

---

## 9. 不在范围内

1. **Kline 自动重算**：late record 不触发已输出 kline 文件修正，走离线补算
2. **Snapshot carry-forward 回写**：不修改已输出的无交易 carry-forward 行
3. **离线补算工具**：后续单独设计
4. **Live 监控仪表盘**：后续单独设计，本期仅通过日志和 summary 暴露 late record 统计

---

## 10. 验证方案

1. **Replay 验证**：使用 0519 真实数据 replay，对比输入/输出条数，验证 0 丢失
2. **单元测试**：`writer.py` append 函数的并发安全性测试
3. **集成测试**：构造乱序数据（部分 symbol 延迟写入），验证 late record 正确追加
4. **Live 重启恢复**：重启后从已有输出文件恢复 flushed_minutes，迟到 record 仍能 late append
5. **同 symbol 多行**：同一分钟同一 symbol 已有 update_flag=N，late append 后新增 update_flag=Y，不覆盖原行
6. **latest_snapshot 更新传播**：late snapshot 更新 latest_snapshot 后，后续未输出分钟的 carry-forward 使用更新后的 latest
7. **并发写安全**：atomic_write 与 append_snapshot_records 并发写同一文件时，文件不损坏、不丢 header、不交叉写半行
