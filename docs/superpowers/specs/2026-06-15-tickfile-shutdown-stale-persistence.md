# Tickfile Shutdown Stale-Row Persistence（待修复）

> **Date**: 2026-06-15
> **Status**: 根因已定位，修复方案待定（需 design review）
> **Parent**: `docs/superpowers/specs/2026-06-15-phase21-late-order-batch-write-fix.md`
> **发现方式**: `test/phase21_benchmark/full_day_run.py`（全天诊断）+ 代码级恢复路径审查
> **严重度**: 实时生产下影响小（gap≈0-1 分钟），但残留行永久不修复，数据完整性隐患

---

## 1. 问题描述

Engine 在 **order 落后 snapshot** 时被停止（重启/崩溃/kill/测试 cap），shutdown 路径
`flush_all_remaining` 会无条件为所有 `_tickfile_pending` 分钟生成 tickfile，**不检查 order
watermark**。这些"order 未到达"的分钟被生成时，order 侧只能用 `latest_order_by_symbol` 的
**冻结 carry-forward 值**，产生 stale（数据陈旧）行。

更严重的是：**重启后这些 stale 行不会被覆盖修复，永久残留在 append-only 的 tickfile 文件中。**

## 2. 硬证据

### 2.1 stale 行确实存在

`full_day_run.py`（100Kx simulator，900s cap，order 停在 1433，snapshot 到 1530）：

```
Shutdown CHECK 3 WARN: 50 tickfile minutes EXTRA (no snapshot+order):
  ['202605281436', ..., '202605281530']
Tickfile shutdown summary: enqueue=364, dequeue=364, generated=329, missing=0
```

tickfile 文件中 symbol 1311 的 bid 跨多分钟冻结（stale carry-forward）：
```
BidP1=2036.0 BidV1=31146  ← 14:38, 14:47, 14:51, 14:57, 15:06, ... 全部冻结
```

### 2.2 重启不修复（恢复链路全代码验证）

| 机制 | 重启行为 | 证据 |
|------|---------|------|
| tickfile 写盘 | append-only，文件存在即追加 | `writer.py:346` |
| `_generated_tickfile_minutes`（去重集） | **空**，不持久化 | checkpoint 无此字段；`aggregator.py:99` |
| `_tickfile_pending` | **空**，纯内存 | 无 checkpoint 字段 |
| `recover_tickfile_seqno` | 只恢复 seqno，不恢复已生成分钟 | `writer.py:427` |
| `flushed_snapshot_minutes` | 从磁盘恢复**全部** snapshot 分钟（含 gap 分钟） | `engine.py:643` |
| tickfile truncate | **无**（只清 `.tmp`） | grep 无 truncate/remove |

**重启执行链**（崩溃前 order=1435、snapshot=1530、tickfile 含 stale 1436-1530）：
1. order 从 checkpoint 恢复，处理 1436 → flush → 触发 tickfile 1436
2. `_try_generate_tickfile(1436)`：去重集空 → 通过 →
   `pending = _tickfile_pending.pop(1436)` = **None**（snapshot 已 flush 过 1436，
   且 1436 ∈ flushed_snapshot_minutes 不重 flush）
3. → "Tickfile skipped: no pending data" → **不重新生成**

**结论：stale 行永久残留。唯一修复手段是删除 tickfile 文件 + ReplayEngine 重跑。**

## 3. 两个无检查的强制生成路径

1. **shutdown**: `flusher.py:487-499` `flush_all_remaining` 无条件遍历 ALL `_tickfile_pending`
2. **cross-day**: `flusher.py:225-234` `_step1_cross_day_check` 无条件遍历剩余 pending

（live 主路径 `flusher.py:448` 的 gate `order_current_minute > minute_key` 是**正确**的，
日志 `Tickfile pending: N total, 0 eligible` 证明运行期间 0 抢跑。）

## 4. 影响评估

| 场景 | order vs snapshot | 停止时 stale gap | 重启后 |
|------|------------------|-----------------|--------|
| 100Kx 压测（benchmark/诊断） | order 远落后 | 50-198 分钟 | 永久残留 |
| **1x 真实生产** | 同步（吞吐 7x 余量） | **0-1 分钟** | 仍残留 |

**关键**：实时生产下吞吐余量充足（实测引擎 87K lines/s vs 峰值实时馈送 12.7K lines/s =
6.9x），order 不会积压，停止 gap 极小。但即便 1 分钟的 stale 也永久不修复。

> 注：100Kx benchmark 是人为 100,000 倍压力测试，order 必然跟不上、必然大 gap——这**不是**
> 实时生产问题，但放大了 Q1 的可观测性。

## 5. 修复方向（待 design review，未实施）

与 INV-TF1「关停不丢数据」不变量冲突，需权衡：

1. **治本**：checkpoint 持久化 `_generated_tickfile_minutes` + 重启时对 order<snapshot 的
   gap 分钟做标记/重新生成（改恢复逻辑，风险中等）
2. **治标**：shutdown/cross-day 强制生成时跳过 order 未到的分钟（与"不丢数据"不变量冲突）
3. **接受现状**：实时下影响极小，定期 replay 重跑清理（运维流程兜底）

## 6. 相关文档

- `[[phase21-late-order-batch-write-bug]]` — late-order 批量写修复（本次提交）
- `[[tickfile-shutdown-forcegen-orderless]]` — memory 记录（根因 + 全天验证）
- `test/phase21_benchmark/full_day_run.py` — 全天诊断脚本（产生本证据）
