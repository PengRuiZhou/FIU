# Phase 4 E2E GIL Contention Analysis

> **Date**: 2026-06-11
> **Status**: Root Cause Identified — GIL contention between order thread and snapshot data thread
> **Parent**: `docs/superpowers/specs/2026-06-11-phase4-test-guide.md`
> **Related**: `docs/superpowers/specs/2026-06-10-rust-order-accel-design.md`

---

## 1. Summary

Phase 4 E2E 测试发现 order 线程在 peak minute (0900) 处理时间为 **171-180 秒**，远超 60s SLA。
Root cause 为 **order 线程与 snapshot 数据线程之间的 GIL 竞争**，导致 order 线程 99% 时间等待 GIL。

Rust order acceleration (Phase 20) 成功解决了 **parsing 瓶颈**（GIL released, 503K rec/s），
但 **per-record Python processing**（`_process_parsed_record` + `OrderRecord` 创建）仍需 GIL，
与 snapshot 数据线程的纯 Python 处理产生严重竞争。

---

## 2. Evidence Chain

### 2.1 Benchmark Results

| # | Test Scenario | 0900 minute time | rec/s | Multiplier |
|---|--------------|------------------|-------|------------|
| 1 | Rust `parse_order_batch` benchmark (single thread) | **1.5s** | 503,000 | 1× |
| 2 | Full order loop (single thread, tickfile=OFF) | **4.8s** | 155,000 | 3.2× |
| 3 | Full order loop (single thread, tickfile=ON) | **6.0s** | 125,000 | 4.0× |
| 4 | E2E multi-thread (tickfile=OFF) | **171s** | 4,371 | **35×** |
| 5 | E2E multi-thread (tickfile=ON) | **180s** | 4,152 | **37×** |

### 2.2 Per-Minute Gap Analysis (tickfile=ON run)

| Minute | Records | Wall-clock gap | Throughput |
|--------|---------|----------------|------------|
| 0800-0859 | ~646K total | 17s total | ~38K/s ✅ |
| 0859→0900 | 747,434 | **180s** | 4,152/s ❌ |
| 0900→0901 | 562,269 | 11s | 51,115/s ✅ (buffered) |
| 0901→0902 | 236,906 | 2s | 118,453/s ✅ (buffered) |
| 0902→0903 | 471,604 | **198s** | 2,382/s ❌ |
| 0903→0904 | 514,319 | **155s** | 3,318/s ❌ |
| 0904→0905 | 602,416 | **99s** | 6,085/s ❌ |
| 0905→0906 | 674,363 | 39s | 17,291/s ⚠️ |
| 0906→0907 | 57,208 | 1s | 57,208/s ✅ (buffered) |

### 2.3 Key Pattern

每次 `ReadMB ≈ 52.4` 时出现 stall（52.4MB = 100 × 512KB chunks）。
Buffered minutes（数据已在内存中）瞬间刷出（11s for 562K records = 51K/s）。
**瓶颈不在 I/O 或 flushing，而在 order 线程读+处理新数据的速度。**

### 2.4 Tickfile Impact Isolation

| Config | 0859→0900 gap | rec/s |
|--------|--------------|-------|
| tickfile=OFF | 171s | 4,371 |
| tickfile=ON | 180s | 4,152 |
| **Delta** | **+9s (+5%)** | -5% |

Tickfile 仅贡献 5% 额外开销，不是根因。

---

## 3. Root Cause Analysis

### 3.1 GIL Contention Mechanism

```
Order Thread                    Snapshot Data Thread
===========                     ====================
[Rust parse — GIL released]     [parse_snapshot_line — GIL held]
_wait for GIL_ ←────────────── [aggregate kline — GIL held]
[OrderRecord() — GIL held]     [write snapshot CSV — GIL held]
[_process_parsed_record]        [write kline CSV — GIL held]
_wait for GIL_ ←────────────── [parse next batch — GIL held]
...                             ...
```

- Order 线程的 Rust parsing 释放 GIL（503K rec/s）✅
- 但 `_process_parsed_record` + `OrderRecord` 创建需要 GIL
- Snapshot 数据线程全程持有 GIL（纯 Python 解析 + 聚合 + 写文件）
- 结果：order 线程 99% 时间在等待 GIL

### 3.2 Quantitative Breakdown

For 747K records in 180 seconds:
- **Rust parsing**: 747K / 503K = ~1.5s (GIL released)
- **Python per-record processing**: 747K × 26μs = ~19s (single-threaded estimate)
- **GIL wait**: 180s - 1.5s - 19s = **~159s (88% of time)**
- **Effective order thread GIL share**: ~19s / 180s = **~11%**

### 3.3 Why Snapshot Thread Dominates

- Snapshot file: 714MB, ~5.4M rows
- `parse_snapshot_line()`: pure Python, holds GIL
- Kline aggregation: pure Python, holds GIL
- File writes: Python I/O, holds GIL
- During peak period, both threads have high data volume
- Snapshot thread's GIL occupancy ≈ 89%, order thread gets only ~11%

---

## 4. Phase 4 Test Results Summary

### Automated Tests — ALL PASS

| # | Test | Priority | Status |
|---|------|----------|--------|
| 1 | Engine-level integration (2 tests) | P1 | ✅ PASS |
| 2 | Corrupted .pyd rollback (2 tests) | P1 | ✅ PASS |
| 5 | Warmup self-test + degradation (2 tests) | P2 | ✅ PASS |
| 3 | Concurrent benchmark (sustained 10s) | P0 | ✅ PASS (550K rec/s) |
| 4 | E2E performance (tickfile=ON) | P0 | ⚠️ Functional PASS, SLA FAIL (180s > 60s) |

### Regression: **395 passed** (389 baseline + 6 new), 0 failed

### Files Changed

| File | Change |
|------|--------|
| `tests/test_order_accel.py` | +6 tests: integration, rollback, warmup |
| `tests/test_order_accel_performance.py` | New — concurrent sustained load benchmark |
| `src/minute_bar/engine.py` | +17 lines warmup self-test (line ~234) |
| `config/test-concurrent-bench.ini` | New — concurrent benchmark config |
| `config/test-e2e-rust.ini` | New — E2E tickfile+rust config |

---

## 5. Recommended Next Steps (Phase 21+)

To achieve the 60s SLA for 0900 peak minute, the per-record Python processing must be
moved into the GIL-released section. Three approaches:

### Approach A: Rust `_process_parsed_record` (Recommended)

Move the per-record loop (date check → seqno → OrderRecord → buffer append)
entirely into Rust. Return only the final state (buffers, flushed minutes).

- **Pros**: Eliminates GIL contention completely; single code change
- **Cons**: Requires Rust-side buffer management
- **Estimated improvement**: 747K / 155K = ~4.8s (well under 60s)

### Approach B: Rust Snapshot Parser

Accelerate `parse_snapshot_line()` with Rust (same pattern as order acceleration).

- **Pros**: Reduces snapshot thread GIL occupancy from ~89% to ~10%
- **Cons**: Only addresses one side of the contention; order thread still limited

### Approach C: Flat Binary + Batch Processing

Use `parse_order_batch_flat` (already implemented) to return flat binary buffer,
then process in Python without per-record tuple creation.

- **Pros**: Already implemented; simpler than Approach A
- **Cons**: Still requires GIL for Python processing; improvement limited

### Recommended: **Approach A** (move order processing to Rust)
would bring 0900 minute from 180s → ~5s (35× improvement).

---

## 6. Three-Thread GIL Competition Analysis

### 6.1 三条线程的 GIL 竞争关系

系统有 3 条线程竞争 GIL：

```
        ┌─────────────────────────────────────────────────────┐
        │              GIL (同一时刻只有 1 个线程持有)            │
        └─────────────────────────────────────────────────────┘
                            ▲  ▲  ▲
                            │  │  │
              ┌─────────────┘  │  └─────────────┐
              │                │                  │
    ┌─────────┴───┐   ┌───────┴──────┐   ┌──────┴──────┐
    │ Order Thread │   │Snapshot Thread│   │ Tickfile    │
    │              │   │               │   │ Writer      │
    │ per-record:  │   │ parse: ~25μs  │   │ gen: 422ms  │
    │ ~3.5μs/rec   │   │ aggregate:    │   │ per minute   │
    │ × 747K =     │   │ ~10μs/rec     │   │ (background) │
    │ ~2.6s GIL    │   │ × 5.4M =     │   │              │
    │              │   │ ~135s GIL     │   │ 也被 snapshot │
    │              │   │               │   │ 阻塞！        │
    └──────────────┘   └───────────────┘   └──────────────┘
```

**Snapshot 线程不仅阻塞 Order 线程，也阻塞 Tickfile Writer。**

### 6.2 Tickfile Writer GIL 占用实测

| Minute | Symbols | Orders | Tickfile 生成耗时 | 备注 |
|--------|---------|--------|------------------|------|
| 0800 | 4,458 | 64,600 | **391ms** | 正常 |
| 0830 | 4,505 | 7,402 | **1,281ms** | GIL 竞争激烈时段 |
| 0900 | 4,505 | 747,434 | **422ms** | snapshot 高峰已过 |

- Tickfile writer 是异步后台线程，不阻塞 order 线程
- 但 snapshot 线程持有 GIL 时，tickfile writer 被迫等待
- 高峰时段 tickfile 生成可膨胀 3×（391ms → 1,281ms）

### 6.3 Tickfile 生成管线

```
Order Thread (per-record, GIL held)         Tickfile Writer (background thread)
====================================        ====================================
1. Parse record (Rust, GIL released)        6. Dequeue minute key
2. Create OrderRecord ← GIL                 7. Pop raw_order_buffers[minute] ← GIL
3. buf.records.append(record) ← GIL         8. Read latest_order_by_symbol ← GIL
4. raw_order_buffers.append ← GIL           9. select_tickfile_records() ← GIL (~5ms)
5. latest_order_by_symbol.update ← GIL      10. build_tickfile_row × 4505 ← GIL (~120ms)
                                             11. Write CSV to disk ← GIL (~100-200ms)
```

Tickfile writer 需要 `raw_order_buffers`（`List[OrderRecord]`，747K 个 Python 对象）
和 `latest_order_by_symbol`（`Dict[str, OrderRecord]`，~4,505 个 Python 对象）。
两者均由 order 线程在 per-record 循环中填充。

### 6.4 关键因果链

```
Snapshot 持有 GIL → Order 线程无法创建 OrderRecord → order CSV 无法生成
                 → tickfile trigger 无法入队 → tickfile 无法开始生成
                 →（tickfile writer 即使拿到 GIL 也无事可做）
```

**核心阻塞点在 order 线程。** 解决 order 的 GIL 竞争，tickfile 自然疏通。

---

## 7. Solution Approaches — Detailed Comparison

### 7.1 方案对比表

| 方案 | 预期 0900 耗时 | 提速倍数 | 复杂度 | 达标(<60s) | 收益确定性 |
|------|---------------|---------|--------|-----------|-----------|
| **A: Rust order 全批量** | 3-5s | 36-60× | ⚠️ 高 | ✅ 达标 | ⭐⭐⭐ |
| **B: Rust snapshot 解析** | 30-50s | 4-6× | ⭐ 中 | ⚠️ 边界 | ⭐⭐ |
| **C: flat batch 优化** | 140-160s | 1.1-1.3× | ⭐ 低 | ❌ 不达标 | ⭐ |
| **D: A+B** | 3-5s | 36-60× | ⚠️⚠️ 最高 | ✅ 达标 | ⭐⭐⭐ |
| **E: B 增强版（聚合也移入 Rust）** | 10-20s | 9-18× | ⚠️ 高 | ✅ 达标 | ⭐⭐⭐ |

### 7.2 方案 A：Rust Order 全批量处理

**做什么**：在 Rust 中完成 `parse → date check → seqno → minute_key → group-by-minute`，
返回已分组的 flat binary。Python 只做 per-minute 的 buffer 管理。

| 维度 | 评估 |
|------|------|
| **预期提速** | 180s → 3-5s（36-60×），因为 order 线程几乎不再需要 GIL |
| **改动范围** | 新增 Rust 函数 `process_order_batch()`（~200-300 行 Rust），修改 engine.py `_order_loop` |
| **复杂度** | 高 — Rust 需要管理 minute 分组、seqno 状态、late order 检测 |
| **卡点 1** | Rust 内部需维护 buffers（`HashMap<minute_key, Vec<record>>`），与 Python `_OrderMinuteBuffer` 同步 |
| **卡点 2** | tickfile 需要 `raw_order_buffers` + `latest_order_by_symbol`，需从 Rust flat binary 延迟反序列化 |
| **卡点 3** | late order 检测涉及 `_flushed_order_minutes` set 查询，需跨 FFI 传递 |
| **卡点 4** | `_flush_order_minute`（文件写入）必须留在 Python，只在 per-minute 边界触发 |
| **回归风险** | 中 — 新增 Rust 路径需要完整 parity test |
| **收益确定性** | 高 — 完全消除 per-record GIL，瓶颈直接消失 |

### 7.3 方案 B：Rust Snapshot 解析器

**做什么**：将 `parse_snapshot_line`（18 个 `int()` + 21 个 `strip()`）移入 Rust。
`process_snapshot` 聚合逻辑仍留 Python。

| 维度 | 评估 |
|------|------|
| **预期提速** | 180s → 30-50s（4-6×），snapshot GIL 占用从 ~89% 降到 ~30-40% |
| **改动范围** | 新增 Rust 函数 `parse_snapshot_batch()`（~150-200 行 Rust），修改 engine.py `_data_loop` |
| **复杂度** | 中 — 与 Phase 20 `parse_order_batch` 模式完全相同 |
| **卡点 1** | `ParsedSnapshot` 有 22 个字段（vs order 的 8 个），flat binary format 需设计 |
| **卡点 2** | `process_snapshot` 聚合逻辑（dict ops + OHLCV update）仍在 Python，仍持有 GIL |
| **卡点 3** | 提速上限受聚合限制 — 即使 parsing 为 0，聚合仍需 ~10μs × 5.4M = 54s GIL |
| **回归风险** | 低 — 复用 Phase 20 成熟模式 |
| **收益确定性** | 中 — 可能达标（30-50s），也可能不达标 |

### 7.4 方案 C：使用 `parse_order_batch_flat` + 优化 Python 循环

| 维度 | 评估 |
|------|------|
| **预期提速** | 180s → 140-160s（1.1-1.3×），效果有限 |
| **改动范围** | 修改 engine.py `_order_loop` 切换到 flat API |
| **卡点** | PyO3 返回开销只是冰山一角，真正瓶颈是 Python per-record processing |
| **回归风险** | 很低 — 已有 API |
| **收益确定性** | 低 — 无法达标（>60s），治标不治本 |

### 7.5 方案 D：A+B 双管齐下

| 维度 | 评估 |
|------|------|
| **预期提速** | 180s → 3-5s（与 A 相同），但更稳健 |
| **改动范围** | A + B 的全部改动 |
| **额外收益** | snapshot 线程也从 GIL 竞争中解放 → tickfile writer 更流畅 |
| **回归风险** | 中 — 两倍新路径需要 parity test |

### 7.6 方案 E：B 增强版（Snapshot 解析 + 聚合移入 Rust）

| 维度 | 评估 |
|------|------|
| **预期提速** | 180s → 10-20s（9-18×），snapshot GIL 占用从 ~89% 降到 ~5% |
| **改动范围** | 新增 Rust 函数（~400-500 行），包含 OHLCV 聚合逻辑 |
| **卡点 1** | OHLCV 聚合需 `HashMap<symbol, OHLCVAggregate>` + 浮点运算 |
| **卡点 2** | `latest_snapshot` dict（4505 symbols）需跨 FFI 同步 |
| **回归风险** | 中高 — 需验证 OHLCV 聚合结果的数值正确性 |

---

## 8. Three-Phase Evolution Roadmap

### 8.1 Phase 21 (A) — Rust Order Full Batch

**目标**：将 order per-record 处理移入 Rust，彻底释放 order 线程 GIL。

```
Order Thread (Phase 21)
=======================
Rust: parse → date check → seqno → minute_key → group-by-minute  [GIL RELEASED]
  ↓ 返回 flat binary per minute + latest_order per symbol
Python: decode latest_order (~50 entries) → update dict              [~0.1ms/chunk GIL]
Python: flush order minute → enqueue tickfile trigger               [per-minute GIL]
```

**Tickfile 影响**：
- `raw_order_buffers` 从 `List[OrderRecord]` → Rust 侧 flat binary
- Tickfile writer 按需反序列化（747K records in 后台线程，不阻塞 order 线程）
- `latest_order_by_symbol` 从 Rust flat binary 批量更新（~50 entries/chunk）
- Tickfile 生成本身不变（`build_tickfile_row` + CSV 写入），仍在后台线程

**Per-chunk GIL 时间对比**：

| 环节 | 当前 (per chunk) | Phase 21 (per chunk) |
|------|-----------------|---------------------|
| Rust parse | 12ms (GIL released) | 12ms (GIL released) |
| Date check + seqno + minute_key | ~3ms (GIL held) | **0ms** (in Rust) |
| OrderRecord 创建 (~6,600) | ~6ms (GIL held) | **0ms** (lazy decode) |
| buf.records.append | ~0.7ms (GIL held) | **0ms** (Rust-side buffer) |
| raw_order_buffers append | ~2ms (GIL held) | **0ms** (Rust-side flat buffer) |
| latest_order_by_symbol (~50) | ~0.5ms (GIL held) | **~0.1ms** (batch decode) |
| **Total order thread GIL** | **~12ms/chunk** | **~0.1ms/chunk** |
| **747K records (100 chunks)** | **~1.2s GIL time** | **~0.01s GIL time** |

### 8.2 Phase 22 (E) — Rust Snapshot Parser + Aggregation

**目标**：将 snapshot 解析和 OHLCV 聚合移入 Rust，释放 snapshot 线程 GIL。

- 消除三条线程间所有 GIL 竞争
- Tickfile writer 不再被 snapshot 阻塞
- 为 Phase 23（Tickfile Rust 化）奠定基础

### 8.3 Phase 23 — Rust Tickfile Generation (Future)

**目标**：将 tickfile 生成移入 Rust。

- 完全消除所有线程的 GIL 依赖
- 0900 minute 全流程（order + snapshot + tickfile）< 3s

### 8.4 各阶段 GIL 竞争变化

| 阶段 | Order GIL | Snapshot GIL | Tickfile Writer | 0900 耗时 |
|------|-----------|-------------|-----------------|----------|
| **当前** | ~1.2s/0900 | ~135s 全天 | ~422ms/0900 | **180s** ❌ |
| **Phase 21 (A)** | **~0.01s** | ~135s 全天 | ~422ms-1s | **3-5s** ✅ |
| **Phase 22 (E)** | ~0.01s | **~0.01s** | ~422ms | **3-5s** ✅ |
| **Phase 23** | ~0.01s | ~0.01s | **~0ms** | **< 3s** ✅ |

### 8.5 Tickfile 在各阶段的表现

| 阶段 | Tickfile 0900 生成 | 是否被阻塞 | 对 Order 线程影响 |
|------|-------------------|-----------|----------------|
| 当前 | 422ms（但 order 线程无法产出数据，tickfile 无事可做） | 被 snapshot 间接阻塞 | 不阻塞（后台线程） |
| Phase 21 | ~422ms-1s（与 snapshot 竞争 GIL，但 order 已自由） | 轻微阻塞 | 不阻塞 |
| Phase 22 | ~422ms（几乎无 GIL 竞争） | 无阻塞 | 不阻塞 |
| Phase 23 | Rust 原生生成，不经过 Python GIL | 无阻塞 | 不阻塞 |

---

## 9. Conclusion

**根因**：Snapshot 数据线程的纯 Python 处理（`parse_snapshot_line` + `process_snapshot`）
全程持有 GIL，同时阻塞了 Order 线程和 Tickfile Writer。

**Phase 21 (A) 为什么能解决**：将 order per-record 移入 Rust 后，
order 线程的 GIL 需求从 ~1.2s 降到 ~0.01s（120× 减少）。
即使 snapshot 仍占用 ~89% GIL，order 线程几乎不受影响。
Order CSV 快速生成 → tickfile trigger 快速入队 → tickfile writer 在后台消费。

**Phase 22 (E) 的增量价值**：释放 snapshot GIL 后，tickfile writer 不再被 snapshot 阻塞，
tickfile 生成恢复到正常速度（~422ms/minute vs 可能膨胀到 ~1s）。
同时为 Phase 23（Tickfile Rust 化）扫清障碍。
