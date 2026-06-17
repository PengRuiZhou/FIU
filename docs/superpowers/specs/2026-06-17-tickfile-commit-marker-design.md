# Tickfile Commit-Marker + Truncate 恢复设计（mid-append 崩溃恢复）

> **Date**: 2026-06-17
> **Status**: 设计已批准，待写实施计划
> **Parent**: 源于 tickfile-stale-fix（`2026-06-16-tickfile-stale-fix-design.md`）E2E 验证后的深度审查
> **类型**: 行为变更（tickfile 写盘加 commit marker）+ 恢复增强（truncate-to-last-commit）

---

## 1. 背景：mid-append 崩溃暴露的 gap

stale-fix（方案 A：shutdown 跳过 + replay 手术式补齐）解决了**优雅停止**时 order<snapshot 的 stale 行问题。但 E2E
讨论中发现一个**硬崩溃**场景的 gap：

tickfile 是**每天一个文件、append-only**（不像 order/snapshot 每分钟一个文件、tmp+rename 原子）。所以：
- **硬崩溃**（kill -9 / OOM / 断电）恰好发生在某分钟 tickfile 的 append 中途 → 该分钟**部分写入**（部分 symbol 行 + 可能截断尾行）。
- stale-fix 的 replay 扫描 `_extract_minutes_from_tickfile` 是**二值存在性检查**：只要该分钟有任意一条合法行（len==65）→ 标记"已生成"→ **replay 跳过它**。
- 结果：**部分分钟永久停留在"部分 symbol"状态**，缺失的 symbol 不会被补。

且 tickfile append-only 决定了**即便检测出部分分钟，replay 也无法干净补齐**——重生会 append，导致已写的部分行 + 新行 = **重复**。

## 2. 为什么不能像 order/snapshot 那样原子

| 输出 | 粒度 | 写法 | 原子 |
|------|------|------|------|
| order / snapshot | 每分钟一个文件 | tmp + `os.replace` | ✅ |
| tickfile | **每天一个文件** | append | ❌ |

POSIX 唯一的原子文件操作是 `rename`（整个文件替换）；**没有"原子追加"**。所以：
- per-minute 文件 → 整文件 rename → 原子（order/snapshot 模式）。
- per-day 文件 → 只能 append → 不原子。
- "`.tmp` 暂存 + 全量 append 到 daily" 也**不原子**——append 那一步仍可被中断。
- 保留 daily 单文件 + 原子的唯一方式 = 每分钟整文件重写 + rename（O(n²)，全天 ~2.4 亿行写入，不现实）。

**方案选择**：保留 daily 单文件（消费方依赖），用 **commit-marker + truncate** 实现等效原子性（本设计）。
（per-minute 原子方案 A 因布局变化被否决——见 §10。）

## 3. 设计：commit marker 作为"提交点"

### 3.1 Marker 写入（单一 chokepoint：`write_tickfile_rows`）

每分钟的数据行写完后，紧接着追加一条 **commit marker** 作为该分钟写入的最后行，然后 flush+fsync。
**marker 的 fsync = 该分钟的提交点**：

```
... 0932 的 ~4505 行 ...
#COMMIT,202605280932,4505        ← 0932 提交点（此前的 rows 已 fsync）
... 0933 的 部分行 ...            ← 💥 崩溃在此（无 0933 marker）
```

- **格式**：`#COMMIT,<minute_key>,<rowcount>`（3 字段，`#` 开头）。
  - `recover_tickfile_seqno` 和数据行 reader 用 `len(fields)==65` 判数据行 → marker（3 字段）被自动跳过。
  - 外部消费方按惯例跳过 `#` 注释行。
  - rowcount 用于诊断/校验（非强制）。
- **两条写路径都覆盖**（`write_tickfile_rows` 是 live `_try_generate_tickfile` 和 replay `_flush_snapshot_minute` 的唯一公共入口）：
  - atomic-create 路径（文件不存在）：content = `header + rows + marker`，整体 tmp+rename 原子。
  - append 路径（文件存在）：rows + marker 一起 append，flush+fsync；marker 的 fsync 即提交点。
- 因为 live 生成的分钟和 replay 生成的分钟都走 `write_tickfile_rows`，**两类分钟都打 marker**。

### 3.2 恢复函数（共享）：`_recover_tickfile_to_last_commit(output_dir, date)`

新增于 writer.py（与 `recover_tickfile_seqno` 并列）：
- 扫描 daily tickfile，记录所有合法 `#COMMIT,<minute>` marker 的字节偏移。
- 定位**最后一个合法 marker**。若其后还有数据（未提交的部分分钟）→ **truncate 文件到该 marker 换行之后**（丢弃部分分钟的全部行）。
- 返回 `(committed_set, had_markers)`：
  - `had_markers=True`：`committed_set` = 所有 marker 的 minute 集合（文件已 truncate 到最后 marker）。
  - `had_markers=False`（老文件无 marker）：`committed_set=None` → 调用方降级到现有 row-based `_extract_minutes`（不 truncate，兼容老文件）。
- 必须在 `_get_write_lock(path)` 内执行（TOCTOU 安全）。

### 3.3 调用方

- **ReplayEngine.run() 启动**：调 `_recover_tickfile_to_last_commit`：
  - `had_markers=True` → `self._generated_tickfile_minutes = committed_set`（marker 模式，替换原 `_extract_minutes` 扫描）。
  - `had_markers=False` → 降级 `_extract_minutes`（老文件兼容）。
  - 之后 gap-fill 跳过 committed 分钟、生成未 committed 的（truncate 已删掉的部分分钟 + gap 分钟）。
- **Engine live 启动**（崩溃重启）：调同一函数 → truncate 上次崩溃留下的部分分钟 → live 从 checkpoint 恢复重处理该分钟（live feed 仍有数据则重生成 + 打 marker；无数据则缺失，由 replay 补——与 gap 一致）。

## 4. 崩溃场景全覆盖

| 崩溃时机 | 文件状态 | 恢复动作 |
|---------|---------|---------|
| 写 rows 中途 | 部分 rows，无 marker | truncate 到上个 marker，重生 ✓ |
| 写 marker 中途 | 部分/截断 marker（非法） | 当作无 marker，truncate 到上个合法 marker ✓ |
| rows+marker fsync 之后 | 完整 rows + 合法 marker | 保留，replay 跳过 ✓ |
| 两分钟之间（上分钟已 commit） | 上分钟完整+marker，下分钟未开始 | 无需 truncate ✓ |

关键不变量：**文件有效内容 = 截到最后一个合法 marker**。marker 之后的一切都是未提交的，可安全丢弃重生。

## 5. 与现有 tail-check / newline-fix 的交互

现有 `write_tickfile_rows` append 路径有尾部 newline-fix（读尾部，若最后一行 `len(fields)!=65` → 补 `\n`）。
**加 marker 后，健康文件的最后一行是 marker（3 字段）而非数据行**，旧逻辑会误判（`3 != 65` → 触发 fix）。
因此 tail-check 必须调整：
- 识别 marker 行（`#COMMIT,` 开头）为合法最后一行 → 不触发 fix。
- 只有**截断的数据行**或**截断的 marker**才触发处理。
- 实际上 recovery 的 truncate-to-last-marker 比 newline-fix 更精确；二者需协同（recovery 在启动时做一次性 truncate，newline-fix 在每次 append 前做尾部保护）。具体在 plan 阶段定。

## 6. 向后兼容

- **新文件**（新代码写）：有 marker → marker 模式 + truncate。
- **老文件**（部署前生成，无 marker）：`had_markers=False` → 降级 row-based presence，不 truncate。
  - 接受老文件极小的 partial 风险，或一次性 replay-to-fresh-dir 重生。
  - 一旦用新代码写过，文件即有 marker，进入 marker 模式。

## 7. 测试计划（TDD）

- `test_write_tickfile_rows_appends_commit_marker`：写一分钟后，文件末行是 `#COMMIT,<minute>,<count>`。
- `test_recover_truncates_uncommitted_partial_minute`：构造 [完整分钟+marker + 部分分钟无 marker] → recovery → 断言部分分钟被 truncate、committed_set 正确。
- `test_recover_handles_mid_marker_crash`：截断 marker → 当作无 marker，truncate 到上个合法 marker。
- `test_replay_gap_fill_with_markers`：预置 marker 文件 + gap → replay → 跳过 committed、补 gap，无重复。
- `test_legacy_file_no_markers_falls_back`：无 marker 文件 → row-based presence，不 truncate。
- `test_atomic_create_includes_marker`：首分钟原子创建，文件含 header+rows+marker。
- 回归：现有 tickfile/replay/seqno-recovery 测试更新期望（marker 行存在）。

## 8. 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| 外部消费方不跳 `#` 行 → 解析失败 | 中 | 中 | 确认消费方；或 marker 用数据行格式（更重）；优先 `#` 注释惯例 |
| truncate 误删合法数据 | 低 | 高 | truncate 只在"最后 marker 之后"且仅未提交数据；锁内执行；单测覆盖 |
| tail-check / newline-fix 与 marker 协同 bug | 中 | 中 | §5 调整 + 单测；recovery 优先 |
| Engine live 启动加 recovery 的集成风险 | 中 | 中 | 共享函数复用；live 启动路径小心改；E2E 验证 |
| 老文件降级路径的 partial 风险残留 | 低 | 低 | 一次性 replay-to-fresh 重生老文件 |

## 9. 范围外

- per-minute tickfile 文件（方案 A，布局变化）—— 已否决（§10）。
- tickfile schema 变化（不加列，marker 是独立注释行）。
- 历史无 marker 文件的批量迁移（降级兼容，可选 replay-to-fresh）。
- snapshot/order 的原子性（已原子，无需改）。

## 10. 被否决的方案 A（per-minute 原子）记录

把 tickfile 改成每分钟一个文件（tmp+rename，像 order/snapshot）：
- 优点：mid-append 问题**彻底消失**（文件完整或不存在），replay 看 `os.path.exists` 即知完整性，无 marker 无 truncate，I/O 不增加。
- 否决原因：**输出布局变化**——消费方从读 1 个 daily 文件变成读 ~330 个 per-minute 文件（breaking change）。
- 本设计（B）保留 daily 单文件，代价是需要 marker + truncate 两套机制。

## 11. 相关文档

- `[[tickfile-shutdown-forcegen-orderless]]` — Q1 stale-fix（本设计的前序）
- `docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md` — stale-fix 设计
- `test/phase21_benchmark/stale_fix_demo.py` — E2E 演示（暴露本 gap 的讨论起点）
