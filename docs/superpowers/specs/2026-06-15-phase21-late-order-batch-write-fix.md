# Phase 21 Live Benchmark 分析报告：Order 线程 Late-Order 写盘瓶颈

> **Date**: 2026-06-15
> **Status**: 根因已定位，修复方案待实施
> **Parent**: `docs/superpowers/specs/2026-06-11-phase21-a-plus-b-design.md`
> **发现方式**: `tests/test_e2e_phase21_benchmark.py::TestPhase21E2ELiveBenchmark::test_live_benchmark`
> **背景**: Phase 21 实现完成后（46/46 tests passing），首次 live E2E benchmark 暴露的真实性能问题

---

## 1. 执行摘要

Phase 21 将 order parse + group + buffer build 移入 Rust 后，所有单元/golden/warmup 测试通过，
但 **live E2E benchmark 揭示 order 线程在 0900 开盘峰值后卡死**，无法在 SLA 内完成 0850–0910 窗口。

**核心结论**：Phase 21 的 Rust 化覆盖了主路径（per_minute_buf），但遗漏了 **late order 写盘路径**
——该路径仍是 Python 逐条 `open()`，在 0900 峰值时成为纯 I/O 瓶颈（非 GIL）。

| 指标 | 实测 |
|------|------|
| Order watermark 最终位置 | **0907**（卡死，8 分钟不前进） |
| Snapshot 线程最终位置 | 1017（正常推进） |
| Tickfile pending 积压 | **319 分钟**（0908+ 无 order 数据） |
| 总耗时 | 481s（7-min cap 截断 + shutdown） |
| Order 线程状态 | `join timeout`，仍在 `open()` 系统调用中 |

---

## 2. 测试环境与方法

### 2.1 测试架构

```
data_simulator (后台线程)        Engine (live mode)
  replay input/*.csv          tail sim_out/ + process
  100Kx speed ──────────────▶ Phase 21 Rust 加速
  写入 sim_out/                    输出 engine_out/
```

- **Source**: `D:/FIU/input/order.csv.20260528` (5.4GB) + snapshot (682MB) + code (1.4MB)
- **Simulator**: `data_simulator.Simulator`, speed=100000 (100Kx 实时)
- **Engine**: live mode，`csv_dir = sim_out/`，Phase 21 三个 flag 全开
- **测量窗口**: 0850–0910（21 分钟，覆盖 0900 开盘峰值）
- **Hard cap**: 7 分钟（诊断优先，不强制 SLA）

### 2.2 监控机制

测试主线程每 50ms 轮询 `engine._state.order_current_minute`，记录每个 minute_key 首次到达的时间戳，
在 order watermark 越过 0910 后停止。

---

## 3. 实测数据

### 3.1 运行结果

```
FAILED tests/test_e2e_phase21_benchmark.py::...::test_live_benchmark
1 failed in 481.10s (0:08:01)
```

### 3.2 Order watermark 推进轨迹（从日志提取）

| 时间段 | order_watermark | Tickfile pending |
|--------|-----------------|------------------|
| 初始 | 0903 | 323 |
| 推进 | 0904 | 322（卡 22 次 warning） |
| 推进 | 0905 | 321 |
| 推进 | 0907 | 319（卡 18 次 warning） |
| **卡死** | **0907** | **319（不再下降）** |

### 3.3 诊断 stack trace

```
CRITICAL engine.py:428  Threads still alive after join timeout: ['order']; skip final flush
ERROR    engine.py:442  Thread order stack:
  File "engine.py", line 940, in _order_loop
    append_order_records(path, [rec])
  File "writer.py", line 99, in append_order_records
    with open(path, "a", encoding="utf-8", newline="") as f:
```

### 3.4 Shutdown 检查

```
Shutdown CHECK 3 WARN: 319 tickfile minutes EXTRA (no snapshot+order):
['202605280908', '202605280909', '202605280910', '202605281011', ...]
```

0908 起的所有分钟都没有 order 数据，导致 319 分钟的 tickfile 无法生成。

---

## 4. 根因分析

### 4.1 卡死位置

Phase 21 在 `engine.py:_order_loop` 的 late order 写盘分支：

```python
# engine.py:932-944 (当前实现)
decoded_late = decode_late_order_buf(late_order_buf)
for rec in decoded_late:                # ← 逐条循环
    self._late_order_count += 1
    self._late_order_minutes.add(str(rec.time)[:12])
    late_mk = str(rec.time)[:12]
    path = get_order_file_path(output_dir, late_mk)
    if os.path.exists(path):
        append_order_records(path, [rec])   # ← 每条一次 open(path, "a")！
    else:
        write_order_file(output_dir, late_mk, [rec])
    if self._config.output.enable_tickfile:
        pending_shared_orders.append(("__LATE__", rec))
```

**每条 late record 都触发一次 `open(path, "a")` 系统调用**。

### 4.2 为什么 0900 峰值触发

- 0900 开盘时，分钟边界（minute_key）变化剧烈
- `process_order_batch` 用 `_flushed_order_minutes` 集合判定 late：
  已 flush 的 minute 再来的 record 即为 late
- 0900 附近 minute 快速 flush（0850–0900 都已 flush），大量 record 被归入 late_order_buf
- 这些 late record 数量可达单批次数千条，逐条 `open()` 把 order 线程拖死在 I/O

### 4.3 排除其他原因

| 假设 | 证据 | 结论 |
|------|------|------|
| GIL 竞争（Phase 4 老问题） | snapshot 线程正常推进到 1017 | ❌ 排除 |
| Tickfile writer 阻塞 | tickfile 生成 203–281ms/分钟，正常 | ❌ 排除 |
| Rust process_order_batch 慢 | per_minute_buf 主路径正常输出到 0907 | ❌ 排除 |
| **Per-record 文件 I/O** | stack 卡在 `open()`，snapshot 不受影响 | ✅ **根因** |

### 4.4 与 Phase 4 GIL 问题的区别

| 维度 | Phase 4（GIL） | 本次（I/O） |
|------|---------------|------------|
| 瓶颈类型 | GIL 竞争 | 文件 I/O 系统调用 |
| 受影响线程 | order（被 snapshot 抢 GIL） | order（自己卡在 open） |
| snapshot 表现 | 抢占 89% GIL | 正常推进 |
| 修复方向 | Rust 化释放 GIL | 批量写减少 open 次数 |

本次问题 **不是** Phase 4 GIL 问题的复发，而是 Phase 21 Rust 化**遗漏 late order 路径**导致的新瓶颈。

---

## 5. 影响评估

### 5.1 功能影响

- **Order 数据丢失**：0908+ 的 late order 仍能写入（只要文件存在），但 order watermark 不推进
- **Tickfile 不完整**：0908+ 的 319 分钟因缺 order 数据无法生成 tickfile
- **无数据错误**：已写入的数据本身正确，只是后续处理停滞

### 5.2 生产影响

- Live 模式下，0900 开盘后约 7 分钟内 order 处理停滞
- Snapshot 和 tickfile 的历史数据仍正确，但实时性中断
- 若 0900 后无大量 late order，问题不会触发（replay 模式无此问题）

---

## 6. 修复方案

按 `minute_key` 分组，每分钟一次批量 `append_order_records`：

```python
# engine.py:932-944 (修复后)
decoded_late = decode_late_order_buf(late_order_buf)

# 按 minute_key 分组，把逐条 open() 收敛为每分钟一次
late_by_minute: dict[str, list[OrderRecord]] = {}
for rec in decoded_late:
    self._late_order_count += 1
    mk = str(rec.time)[:12]
    self._late_order_minutes.add(mk)
    late_by_minute.setdefault(mk, []).append(rec)
    if self._config.output.enable_tickfile:
        pending_shared_orders.append(("__LATE__", rec))

# 每分钟一次 open()，写整批
for late_mk, batch in late_by_minute.items():
    path = get_order_file_path(output_dir, late_mk)
    if os.path.exists(path):
        append_order_records(path, batch)
    else:
        write_order_file(output_dir, late_mk, batch)
```

**预期效果**：
- `open()` 次数：`len(decoded_late)`（数千）→ `len(late_by_minute)`（~1–3，每分钟一个）
- Order 线程不再阻塞在 I/O
- Tickfile 积压随 order watermark 推进自动疏通

### 6.1 正确性 Invariants

1. 相同文件写入：每条 late record 仍 append 到对应分钟的 order 文件
2. 相同记录顺序：分钟内 record 保持 `decode_late_order_buf` 的插入顺序
3. 相同计数：`_late_order_count` 每条 +1（在分组循环，非写盘循环）
4. 相同 `_late_order_minutes` 集合
5. 相同 tickfile 路由：每条 late record 仍 append 到 `pending_shared_orders`
6. append/create 决策不变：文件存在→append，缺失→create

---

## 7. 验证计划

### 7.1 单元测试

新增测试：构造跨多分钟的大量 late record，mock writer，断言：
- 每个 minute_key 只调用一次 `append_order_records`/`write_order_file`
- 输出文件含全部 record，顺序正确
- `_late_order_count` 等于输入 record 数

### 7.2 Live benchmark 重跑

```bash
python -m pytest tests/test_e2e_phase21_benchmark.py::TestPhase21E2ELiveBenchmark::test_live_benchmark -v -s
```

**通过标准**：
- Order watermark **越过 0910**（7-min cap 内）
- `window_completed == True`，per-minute 时间线显示 0850–0910 稳步推进
- 无 "order thread join timeout" CRITICAL
- Tickfile pending 不卡在 319

### 7.3 回归

- `tests/test_order_accel.py` 全通过
- `tests/test_order_batch_golden.py` 全通过
- snapshot/tickfile 测试不受影响（修复仅限 order 线程）

---

## 8. 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| 批量 append 改变文件内 record 顺序 | 低 | 中 | Invariant #2 保序；golden 测试覆盖 |
| 分组循环计数 off-by-one | 低 | 低 | Invariant #3 在分组循环计数；单测断言 |
| `_order_loop` 其他位置也有逐条 late 写盘 | 中 | 高 | 审计所有 `append_order_records` 调用点 |

**审计要求**：实施前 grep `_order_loop` 内所有 `append_order_records` 调用，确认 Phase 21 分支是唯一剩余的逐条写盘点（Phase 19 `e2e-live-test-bugs` 修过其他 late 路径）。

---

## 9. 范围外

- Phase 21 Rust 函数（`process_order_batch` 等）— 不变，已验证正确
- Snapshot 线程 / Tickfile writer — 不受影响
- GIL 竞争（Phase 4 根因）— 独立问题，本次修复针对 I/O 非 GIL

---

## 10. 相关文档

- `[[phase21-status]]` — Phase 21 实现完成（46/46 tests）
- `[[phase4-e2e-gil-analysis]]` — Phase 4 GIL 根因（注意区别：本次是 I/O 非 GIL）
- `tests/test_e2e_phase21_benchmark.py` — 发现本问题的诊断 benchmark
- `docs/superpowers/specs/2026-06-15-phase21-e2e-benchmark.md` — benchmark spec
