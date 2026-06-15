# Tickfile Order Thread Stall — 根因分析

> **Date**: 2026-06-04
> **Status**: Analysis Complete, Fix Pending
> **Related**: `docs/superpowers/specs/2026-06-03-tickfile-sync-design.md` (Phase 17)

## 1. 现象

Live E2E 测试（data_simulator speed=100）中，order thread 在处理到 minute 0857-0859 后停滞 3+ 分钟，导致所有后续 tickfile 只有 carry-forward 数据（0 真实 orders）。

**关键日志**：
```
18:08:27 [INFO] Tickfile generated minute=0800 (4458 symbols, 64600 orders) [thread=order-thread] ✅
18:08:35 [INFO] Tickfile generated minute=0830 (4505 symbols, 7402 orders) [thread=order-thread] ✅
18:08:48 [INFO] Output order minute 202605280859 (74894 records) ← order thread 最后输出
18:09:00 [INFO] Tickfile generated minute=0902 (4505 symbols, 0 orders) [thread=clock-thread] ⚠️
18:09:02 [INFO] Tickfile generated minute=0903 (4505 symbols, 0 orders) [thread=clock-thread] ⚠️
...（所有后续 tickfile 均为 0 orders，order_watermark 始终为 202605280859）
```

## 2. 根因链（三层叠加）

### 2.1 第一层：catch-up scan 积累大量 pending 分钟

**代码位置**: `engine.py:660-685` `_drain_tickfile_triggers`

当 order thread 处理完一批 order 数据后，drain 函数更新 `order_current_minute` 并执行 catch-up scan：

```python
def _drain_tickfile_triggers(self) -> None:
    ...
    with self._state.lock:
        self._state.order_current_minute = latest  # e.g., "202605280859"
        # Catch-up: 扫描所有 <= current 的 pending
        for mk in list(self._state._tickfile_pending):
            if mk <= latest and mk not in triggers:
                triggers.append(mk)
    # 串行生成所有 tickfile
    for mk in triggers:
        self._flusher._try_generate_tickfile(mk)
```

**问题**：Clock thread 在 order thread 处理 order 数据期间，已经把大量 snapshot 分钟 flush 到了 `_tickfile_pending`（每个过期分钟一条 entry）。当 order thread 的 watermark 跳到 0859 时，catch-up scan 发现 **~58 个 pending 分钟**（0801-0829, 0831-0859），全部加入触发列表。

**为什么有 58 个？**
- Speed=100 时，simulator 100x 速写入数据
- Clock thread 快速推进 watermark，一口气 flush 多个 snapshot 分钟
- 每个 flush 都往 `_tickfile_pending` 存入一条 entry
- 0800 和 0830 已被之前的 drain 处理（已 pop），但 0801-0829, 0831-0859 仍积压

### 2.2 第二层：`readlines()` 读取整个文件

**代码位置**: `writer.py:342-344`

`write_tickfile_rows` 的 append 路径每次写入前检查截断行：

```python
# Check for truncated last line
with open(path, "r", encoding="utf-8", newline="") as f:
    lines = f.readlines()  # ← 读取整个文件！
```

**IO 放大**：
- 文件在交易日中持续增长（最终 ~4500 symbols × ~240 分钟 = ~100 万行 / ~200MB）
- 每次 append 都全文件读取，只为检查**最后一行**是否截断
- 实测耗时：文件 76538 行 / 23MB 时，单次 `readlines()` + 解析 约 200-500ms
- 58 个 tickfile × 300ms = **17.4 秒**（且文件逐步增长）

### 2.3 第三层：`fsync()` + 文件锁竞争

**fsync**: `writer.py:361`
```python
os.fsync(f.fileno())  # ← 每次 append 都 fsync
```
- Windows 上 fsync 强制刷盘，耗时 50-300ms
- 58 个 tickfile × 150ms fsync = **8.7 秒**

**文件锁**: `writer.py:24-28` + `writer.py:301`
```python
def _get_write_lock(path: str) -> threading.Lock:
    # 按 path 粒度分配锁
    ...
# 所有分钟写入同一个 daily tickfile → 共享同一把锁
with _get_write_lock(path):
    ... readlines() + fsync() ...
```

- Clock thread 和 order thread **竞争同一把 daily tickfile 锁**
- Clock thread overflow 每秒生成 1-2 个 tickfile（各持锁 0.5-1s）
- Order thread drain 在 58 个 tickfile 循环中，每个都要等锁释放
- 锁等待时间叠加：58 × 等待时间 + 58 × 自己持锁时间

### 2.4 总耗时估算

```
单次 tickfile 生成 ≈ readlines(200ms) + select(50ms) + write(50ms) + fsync(150ms) = ~450ms
58 个 tickfile:
  - 无锁竞争: 58 × 450ms ≈ 26 秒
  - 有锁竞争（clock thread 交替持锁）: 26s + 等待时间 ≈ 60-120 秒
  - 实测: 3+ 分钟（与估算一致）
```

## 3. 为什么不能直接移除 drain 中的 tickfile IO

**直觉方案**：让 `_drain_tickfile_triggers` 只更新 `order_current_minute`，不做 tickfile IO，交给 clock thread 的 overflow 机制。

**三个关键问题**：

### 3.1 `>` vs `>=` 边界 → 最后一分钟成为孤儿

Clock thread 的触发条件是 `order_current_minute > minute_key`（**严格大于**）。

```
Order thread 处理完 0931 → order_current_minute = "0931"
Clock thread flush 0931 snapshot → _tickfile_pending["0931"] = {...}
触发检查: "0931" > "0931" → False → 不触发！
```

Minute 0931 的 tickfile **永远不会被 clock thread 触发**，因为 clock thread 只在 `_flush_minutes_internal` 中检查，而 0931 只会被 flush 一次。

**drain 的 catch-up scan 是处理此边界的唯一机制**：
```python
for mk in list(self._state._tickfile_pending):
    if mk <= latest:  # ← 用 <= 而非 <
        triggers.append(mk)
```

### 3.2 移除 catch-up scan 导致 pending 积压

没有 catch-up scan，`order_current_minute == minute_key` 的 pending 条目只能等：
- overflow（10 分钟后）→ tickfile 延迟 10 分钟
- EOF（shutdown 时）→ tickfile 在关闭时才生成
- 这意味着**每个 batch 的最后一分钟**都有 10 分钟延迟

### 3.3 overflow 成为瓶颈 → 阻塞 clock thread

如果所有 tickfile 生成都落到 overflow，clock thread 的 `tick()` 方法要做文件 IO，阻塞 0.5-1s/tickfile。这会延迟 snapshot flush，形成恶性循环。

## 4. 可行的修复方向

### 方向 A：IO 优化（不改架构）

- **seek 优化**：`readlines()` → `seek(-4KB, 2)` + binary tail read（已验证通过，2-5s → 0.7-1.3s）
- **去除 tickfile fsync**：tickfile 是衍生数据，可从 snapshot+order 重生成，不需要每条 fsync
- 效果：单次 ~100ms，58 个 ≈ 5.8s（可接受，仅启动时积压一次）

### 方向 B：后台 Tickfile Writer 线程

- drain 中 tickfile 生成改为入队到专用线程
- order thread 仅做 enqueue（微秒级），不阻塞
- 后台线程串行生成（天然无锁竞争）
- 需要新增线程管理、优雅关闭逻辑

### 方向 C：Per-minute Tickfile 文件

- 每个 minute 独立 tickfile → 消除 daily 文件锁竞争
- 变更范围大：下游消费者、recovery、replay、live 全需改动

## 5. 术语表

| 术语 | 含义 |
|------|------|
| `_tickfile_pending` | SharedState 中已 flush snapshot 但等 order 的 tickfile 数据 |
| `order_current_minute` | Order thread 处理进度（在 drain 中更新） |
| catch-up scan | drain 中扫描 `_tickfile_pending` 找到 order 已到达的分钟 |
| overflow | clock thread `tick()` 中 pending > 10 时强制生成最旧分钟 |
| daily tickfile | 所有分钟写入同一文件 `tickfile_YYYYMMDD.csv` |
| carry-forward | tickfile 使用 `latest_order_by_symbol` 而非当前分钟真实 order |
