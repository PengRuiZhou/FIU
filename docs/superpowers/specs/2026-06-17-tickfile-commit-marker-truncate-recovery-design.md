# Tickfile Commit-Marker + Truncate 恢复设计（mid-append 崩溃恢复）

> **Date**: 2026-06-17
> **Status**: Review Round 1 通过（7 Critical + 6 Major 已修复，见 review log），待 Round 2 复审
> **Parent**: 源于 tickfile-stale-fix（`2026-06-16-tickfile-stale-fix-design.md`）E2E 验证后的深度审查
> **类型**: 行为变更（tickfile 写盘加 commit marker）+ 恢复增强（truncate-to-last-commit）
> **Review**: `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`

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
  - **rowcount = 实际写入的 rows 行数**（`len(rows)`，即 `build_tickfile_row` try/except 后的存活数），**不是** `len(selected)`。仅诊断用途，recovery 不强校验（见 §3.2 M3：不一致仅 WARNING）。
- **两条写路径都覆盖**（`write_tickfile_rows` 是 live `_try_generate_tickfile` 和 replay `_flush_snapshot_minute` 的唯一公共入口）：
  - atomic-create 路径（文件不存在）：content = `header + rows + marker`，整体 tmp+rename 原子。
  - append 路径（文件存在）：rows + marker 一起 append，flush+fsync；marker 的 fsync 即提交点。
- 因为 live 生成的分钟和 replay 生成的分钟都走 `write_tickfile_rows`，**两类分钟都打 marker**。

**关键不变量（C7/M1）**：
- **INV-CM-MONO**：recovery **不依赖** rows 与 marker 之间的崩溃精确区分（kernel 按 page 刷回，不可靠区分）。它只依赖单调性——**marker 落盘 ⟺ 该分钟完整**；marker 未落盘（含 rows 部分写 / marker 部分写 / marker 完全没写）→ 一律 truncate 到上个合法 marker。
- **INV-CM-BATCH**：append 路径中 rows 与 marker 必须在**同一** `open(path,"a")` context 内**连续写入**，共用**单次** `f.flush()` + `os.fsync()`。**禁止**拆成两次 write 或两次 fsync（否则出现"rows 已 fsync 但 marker 未写"→完整数据被当 partial 删→重生重复）。

### 3.2 恢复函数（共享）：`_recover_tickfile_to_last_commit(output_dir, date)`

新增于 writer.py（与 `recover_tickfile_seqno` 并列）。

**Path 构造（m2）**：`path = get_tickfile_path(output_dir, f"{date}0000")`（任一该日 minute_key 都解析到同一 per-day 文件）。

**Marker 合法性校验（C3）**——定义 `_parse_commit_marker(line) -> Optional[str]`，返回 minute_key 或 None：
1. `line.startswith("#COMMIT,")`；
2. `split(",")` 后恰好 3 字段；
3. minute 字段为 12 位纯数字；
4. rowcount 字段为非负整数（CRLF 残留 `4505\r` 等非纯整数 → 非法）。
任何一项不满足 → 该位置**无合法 marker**（截断 marker / 损坏 marker 一律视为无 marker）。

**扫描 + truncate（C4/M4）**：
- 在 `_get_write_lock(path)` 内执行（INV-CM-LOCK，TOCTOU 安全）。
- 逐行扫描（跳过 header line_num==1），对每行调 `_parse_commit_marker`；合法则记录 `(minute_key, 字节偏移=该 marker 行首位置)`。
- **truncate 点 = 字节偏移最大**的合法 marker（按文件位置，**不**按 minute_key 排序——防跨日错位误判）。
- **truncate 偏移（C4）**：truncate 到该 marker 行**自身的换行符之后**——即**保留该 marker 行 + 其 `\n`**，文件以 `...#COMMIT,<minute>,<rowcount>\n` 结尾（INV-CM-LAST：truncate 后文件末行必为合法 marker）。这保证下次 append 不把已 commit 分钟当 gap 重生。
- 若该最后 marker 之后还有字节（未提交的部分分钟）→ **truncate 丢弃**；若文件恰好在最后 marker 换行处结束 → 不 truncate。
- **备份（C4）**：truncate 前，若 `new_size < old_size`，把被丢弃的 `[new_size, old_size)` 字节复制到同目录 `tickfile_{date}.csv.truncated.{timestamp}`（INV-CM-BACKUP，取证用，运维定期清理）。

**committed_set 来源（M2/M4）**：
- 返回 `(committed_set, had_markers)`。
- `had_markers=True`：`committed_set` = **所有合法 marker 的 minute 集合**（set 去重，处理 dup）∪ **marker 之前所有 65-字段数据行对应的 minute**（覆盖"新代码首次 append 到老文件"的混存场景——老 row-only 分钟不丢、不被当 gap 重生）。
- `had_markers=False`（老文件无 marker 且有数据）：`committed_set=None` → 调用方降级 row-based `_extract_minutes`（不 truncate）。
- 无数据行无 marker（空文件/仅 header）：`had_markers=False`，`committed_set=set()`，不 truncate。

**一致性校验（M4）**：扫描中若发现 dup marker（同 minute 多次）或 minute_key 按字节偏移非严格递增 → 记 WARNING（可能跨日错位/重复写），**不阻断** recovery。

**rowcount 校验（M3）**：对每个合法 marker，若其 rowcount ≠ 该分钟实际 65-字段行数 → 记 WARNING（`build_tickfile_row` 异常信号），**不阻断**。

**可观测性（M5）**：recovery 完成后输出结构化 log（truncate 发生时 WARNING，否则 INFO）：
```
tickfile_recovery: date={date} had_markers={bool} committed_minutes={n}
  last_commit_minute={mk} markers_found={n} truncate_bytes={b}
  pre_size={pre} post_size={post} backup={path_or_none}
```
metric（接入 engine 现有 `_tickfile_*` 计数器家族）：`tickfile_recovery_truncate_bytes`（累计）、`tickfile_recovery_committed_minutes`、`tickfile_recovery_had_markers`、`tickfile_recovery_invocations`。

### 3.3 调用方 + 时序不变量（C2）

**ReplayEngine.run() 启动**：调 `_recover_tickfile_to_last_commit`：
- `had_markers=True` → `self._generated_tickfile_minutes = committed_set`（marker 模式，替换原 `_extract_minutes` 扫描）。
- `had_markers=False` → 降级 `_extract_minutes`（老文件兼容）。
- 之后 gap-fill 跳过 committed 分钟、生成未 committed 的（truncate 已删掉的部分分钟 + gap 分钟）。

**Engine live 启动**（崩溃重启）：调同一函数 → truncate 上次崩溃留下的部分分钟 → live 从 checkpoint 恢复重处理该分钟（live feed 仍有数据则重生成 + 打 marker；无数据则缺失，由 replay 补——与 gap 一致）。

**调用时序不变量（C2，生产正确性）**——必须在 spec 闭环，不能推到 plan：
- **INV-CM-ORDER-1**：`_recover_tickfile_to_last_commit` 必须在 **`Engine.start()` spawn `_tickfile_writer_thread` 之前**调用（单线程，无并发）。插入点：engine.py `start()` 的 `enable_tickfile` 分支内、`self._tickfile_seqno = self._flusher._recover_tickfile_seqno()` 紧邻处，严格早于 `self._tickfile_writer_thread = Thread(...)`。否则 writer 线程已消费首个 minute 后再 truncate → 删掉刚写的合法行 → 丢数据。
- **INV-CM-ORDER-2**：recovery（含 truncate）必须在 `recover_tickfile_seqno` **之前**完成（或 recovery 内部一次扫描同时返回 last_seqno）。否则 seqno 取到被 truncate 的分钟 → 倒退/跳号。
- **INV-CM-SKIPSET**：recovery 返回的 `committed_set` 必须写入 live 引擎的 `self._state._generated_tickfile_minutes`（不只 replay）。否则 live writer 会重生已 commit 的分钟 → 重复行。
- **INV-CM-LOCK**：recovery 的 truncate 持 `_get_write_lock(path)`；writer 线程也持同一 lock → 互斥安全（且因 INV-CM-ORDER-1 时序，实际无并发）。

## 4. 崩溃场景全覆盖（C7 修订：不依赖 rows/marker 精确区分）

recovery 只认**单调性**：marker 落盘 ⟺ 该分钟完整。kernel 按 page 刷回，"写 rows 中途"与"写 marker 中途"不可靠区分，但恢复动作一致，故合并：

| 崩溃时机（合并后） | 文件状态 | 恢复动作 |
|---------|---------|---------|
| **marker 未落盘**（含 rows 部分写 / marker 部分写 / marker 完全没写） | 末尾是部分 rows 或截断/损坏 marker，**无合法末 marker** | truncate 到上个合法 marker，重生该分钟 ✓ |
| **marker 已 fsync** | 完整 rows + 合法 marker | 保留，replay/live 跳过 ✓ |
| **两分钟之间**（上分钟已 commit，下分钟未开始） | 上分钟完整+marker，文件正常结尾 | 无需 truncate ✓ |

关键不变量（重申 INV-CM-MONO / INV-CM-LAST）：**文件有效内容 = 截到最后一个合法 marker**；truncate 后末行必为合法 marker。marker 之后的一切都是未提交的，可安全丢弃重生。

## 5. 与现有 tail-check / newline-fix 的交互（C1 闭环）

现有 `write_tickfile_rows` append 路径有尾部 newline-fix（读尾部，若最后一行 `len(fields)!=65` → 补 `\n`）。
**加 marker 后，健康文件的最后一行是 marker（3 字段）而非数据行**，旧逻辑会误判（`3 != 65` → 每次正常 append 前插多余 `\n` → 文件累积空行污染）。

**必须在 spec 写死的修改（不再推到 plan）**——tail-check 合法末行谓词改为：
```python
def _is_legal_last_line(fields):
    """合法最后一行 = 65 字段数据行 或 #COMMIT marker 行。"""
    return len(fields) == 65 or (len(fields) >= 1 and fields[0] == "#COMMIT")
```
即 `need_newline_fix` 仅当最后一行**既非 65 字段也非 #COMMIT marker**（截断数据行 / 截断 marker / 非 UTF8）才为 True。marker 行（合法）→ 不 fix。

协同：recovery 的 truncate-to-last-marker 在启动时做一次性精确恢复（保证文件以合法 marker 结尾）；newline-fix 在每次 append 前做尾部保护（防止上一次 append 被 mid-append 截断的残留）。两者用同一 `_is_legal_last_line` 谓词，不冲突。

## 6. 向后兼容 + 回滚安全（C5/M2/m3）

**新代码读老文件（前向）**：
- 新文件（新代码写）：有 marker → marker 模式 + truncate。
- 老文件（部署前生成，无 marker，有数据行）：`had_markers=False` → 降级 row-based presence，不 truncate。接受极小 partial 风险，或一次性 replay-to-fresh-dir 重生。
- 空文件（仅 header，无数据无 marker）：`had_markers=False`，`committed_set=set()`，不 truncate。

**混存文件（M2，滚动升级场景）**：新代码首次 append 到老文件 → 文件 = `[老 row-only 分钟][新 marker 分钟]`。
- `had_markers=True`（发现新 marker）→ `committed_set` = marker 分钟 ∪ **marker 之前所有 65-字段行的分钟**（§3.2 committed_set 来源）。
- 老分钟不丢、不被 replay 当 gap 重生 → 无重复。
- 检测到此混存（首个 marker 之前存在 65-字段行）→ WARNING `tickfile_recovery: legacy_mix_minutes=N`（提示可 replay-to-fresh 清理）。

**老代码读新文件（回滚，C5）**——必须文档化的风险：
- 回滚到旧代码后，旧 `write_tickfile_rows` 的 tail-check（`len!=65` 判截断）会把 `#COMMIT,...`（3 字段）误判为截断行 → 每次 append 前插一个多余 `\n`。
- 影响：文件累积空行。**内部 reader（`recover_tickfile_seqno` / `_extract_minutes`）按 `len!=65` 跳过空行不崩**；但外部严格 CSV consumer 可能解析失败。
- 风险定性：**非破坏性数据损坏**（空行可被跳过/清理），但需告知。
- **Rollback playbook**（写入部署文档）：
  1. 回滚前统计：`grep -c '^#COMMIT' tickfile_*.csv` 确认哪些文件有 marker。
  2. 回滚后若需清理空行：一次性 `replay-to-fresh-dir` 重生当天 tickfile（或 `sed` 删空行）。
  3. 优先策略：回滚窗口期内暂停 live tickfile 写入，用 replay 重生。

## 7. 测试计划（TDD，C6 补强）

**Marker 写入**：
- `test_write_tickfile_rows_appends_commit_marker`：写一分钟后，文件末行是 `#COMMIT,<minute>,<count>`。
- `test_atomic_create_includes_marker`：首分钟原子创建，文件含 header+rows+marker。
- `test_append_rows_and_marker_single_fsync_batch`：monkeypatch `os.fsync` 计数 → 一次 append 调用恰好 1 次 fsync（INV-CM-BATCH，M1）。

**Recovery 正确性**：
- `test_recover_truncates_uncommitted_partial_minute`：[完整分钟+marker + 部分分钟无 marker] → recovery → 部分分钟被 truncate、committed_set 正确、末行是合法 marker（INV-CM-LAST）。
- `test_recover_handles_mid_marker_crash`：截断 marker → 当作无 marker，truncate 到上个合法 marker。
- `test_recover_truncated_marker_crlf_not_committed`：`#COMMIT,...,4505\r`（CRLF 残留）→ 非法 marker，不进 committed_set。
- `test_recover_malformed_marker_bad_minute_bad_rowcount`：非 12 位 minute / 非整数 rowcount → 非法。
- `test_recover_duplicate_marker_keeps_last_warns`：同 minute 两次 marker → set 去重 + WARNING，不阻断。
- `test_recover_out_of_order_minute_warns`：marker 字节顺序非递增 → WARNING，不阻断。
- `test_recover_rowcount_mismatch_warns`：marker rowcount ≠ 实际行数 → WARNING（M3）。
- `test_recover_backup_truncated_tail`：truncate 后存在 `.truncated.{ts}` 备份，内容=被丢弃字节（C4）。
- `test_recover_no_truncate_when_clean_boundary`：两分钟都 commit + 文件正常结尾 → 不 truncate（§4 第三行）。
- `test_recovery_holds_write_lock`：mock 断言 `_get_write_lock` 被 acquire（INV-CM-LOCK）。

**Tail-check 协同（C1）**：
- `test_tail_check_recognizes_marker_as_legal_last_line`：文件末行是 `#COMMIT,...` → append 下一分钟不插多余 `\n`。

**混存 + 兼容**：
- `test_recover_legacy_mix_includes_row_only_minutes`：[老 0930 rows][老 0931 rows][新 0932 rows+marker] → committed_set ⊇ {0930,0931,0932}（M2）。
- `test_legacy_file_no_markers_falls_back`：无 marker 有数据 → row-based，不 truncate。
- `test_legacy_empty_file_no_markers`：仅 header → had_markers=False，committed_set=set()，不 truncate（m3）。

**Live restart 路径（C2/M3）**：
- `test_engine_start_truncates_uncommitted_before_writer_thread`：构造含部分分钟的 tickfile，Engine.start() → 断言 writer 线程启动时文件已 truncate（INV-CM-ORDER-1）。
- `test_seqno_recovery_after_truncate_excludes_dropped_minute`：truncate 后 seqno 不指向已删分钟（INV-CM-ORDER-2）。
- `test_live_recovery_populates_skipset`：recovery 后 `_state._generated_tickfile_minutes` 含 committed（INV-CM-SKIPSET）。

**端到端（C6，核心价值证明）**：
- `test_e2e_mid_append_crash_recovery`：用 helper 写 `[header + 0901 rows + #COMMIT,0901,N + 0902 partial rows (no marker)]`，调 `ReplayEngine.run()` with snapshot 含 0901+0902 → 断言：最终 `#COMMIT` 数=2、0901 行数不变、0902 行数=预期、无空行无重复。模板：stale-fix `test_replay_fills_gap_without_corrupting_correct_rows`。

**外部 consumer 兼容（C5）**：
- `test_external_csv_reader_skips_hash_lines`：标准 csv.reader 解析含 `#COMMIT` 行的 tickfile，断言跳过/显式处理。

回归：现有 tickfile/replay/seqno-recovery 测试更新期望（marker 行存在；tail-check 谓词变化）。

## 8. 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| 外部消费方不跳 `#` 行 → 解析失败 | 中 | 中 | 确认消费方；§7 加 consumer 兼容测试；优先 `#` 注释惯例 |
| truncate 误删合法数据 | 低 | 高 | truncate 仅"最后 marker 之后"未提交数据；锁内执行（INV-CM-LOCK）；marker 严格校验（C3）；**备份 tail**（C4）+ 单测覆盖 |
| tail-check / newline-fix 与 marker 协同 bug | 高→低 | 中 | §5 写死 `_is_legal_last_line` 谓词（C1）+ 单测 |
| Engine live 启动加 recovery 的集成风险 | 中 | 高 | 时序不变量 INV-CM-ORDER-1/2（C2）；E2E 验证；共享函数复用 |
| 老文件降级路径的 partial 风险残留 | 低 | 低 | 一次性 replay-to-fresh 重生老文件 |
| **回滚：旧代码读新 marker 文件插空行（C5）** | 中 | 中 | §6 rollback playbook；内部 reader 跳空行不崩；外部 consumer 需知；replay-to-fresh 清理 |
| 混存文件老分钟被当 gap 重生（M2） | 中 | 高 | §3.2 committed_set 纳入 marker 前 row-only 分钟 + WARNING；单测 |
| rows+marker 拆两次 fsync → 重生重复（M1） | 中 | 高 | INV-CM-BATCH 不变量 + fsync 计数单测 |

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
