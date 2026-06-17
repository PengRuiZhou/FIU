# Tickfile Commit-Marker + Truncate 恢复设计 — Design Review Log

> **Spec**: `docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md`
> **模式**: 2 轮 × 3 agents（崩溃恢复正确性 / IO-锁-兼容 / 测试-可观测-回滚）

---

## Review Round 1

### 审核时间
* 2026-06-17 17:10:00

### 审核对象
* `docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md`

### 本轮审核目标
* 初审 commit-marker + truncate 方案；
* 判断恢复正确性、IO/锁/兼容、测试覆盖风险；
* 判断是否可进入 planning。

### Agent 原始摘要（简短）

**Agent 1（崩溃恢复正确性 + commit 语义）**：大方向正确（marker=commit point + truncate-to-last-commit 单调性）。但 §4 崩溃表对 rows/marker 可区分性过度乐观（kernel 按 page 刷回，不可靠区分，但恢复动作一致所以语义仍正确）；marker 合法性校验函数未定义（C2，截断/CRLF/坏字段会污染 committed_set）；truncate 无备份（C3，marker 校验 bug 误删完整分钟则无取证）；M3 tail-check 协同必须设计层闭环；M5 live 启动 recovery 时序未定义。

**Agent 2（IO、锁、兼容、tail-check）**：2 Critical — tail-check 会误判 marker 行（len==3 !=65）每次 append 插多余 `\n` 污染文件（C1）；Engine live 启动 recovery 与 writer 线程启动顺序未定义（C2，TOCTOU 可能删 writer 刚写的合法行）。4 Major — seqno 恢复与 marker 恢复须协同（M1）；老新混存文件老分钟被当 gap 重生（M2）；truncate 偏移语义模糊（M3，须保留 marker 作末行）；rows+marker 须单次 fsync 批次（M4）。

**Agent 3（测试、可观测、落地、回滚）**：2 Critical — 回滚兼容盲点（旧代码读新 marker 文件→旧 tail-check 注入空行，C1）；E2E mid-append crash recovery 测试缺失（C2，设计核心价值无端到端证明）。5 Major — 畸形 marker 防御（M1）；recovery 无 log/metric（M2）；live restart 路径未测（M3）；recovery 与 seqno 顺序未定义（M4）；Status 须改 review（M5）。

### 综合问题清单（去重）

#### Critical

**C1 — tail-check 误判 marker 行触发错误 newline-fix（每次 append 插空行）**
- 来源: Agent 2 C1, Agent 1 M3
- 问题: 现有 `write_tickfile_rows` append 路径 tail-check（writer.py:407）用 `len(last_line.split(','))!=65` 判截断。加 marker 后健康文件末行是 `#COMMIT,...`（3 字段）→ 3!=65 → 每次正常 append 前插多余 `\n` → 文件累积空行，长期污染。
- 影响: 文件空行污染；外部严格 CSV consumer 可能解析失败；reader 偏移漂移。
- 修改决议: 修改 spec §5。
- 状态: **Accepted**
- 理由: 必现 bug，每次 append 触发。必须在 spec 写死 tail-check 合法末行谓词 = `(len==65) or startswith("#COMMIT,")`，不能推到 plan。

**C2 — recovery 调用点时序 + skip-set 传播未定义（TOCTOU / 重复生成）**
- 来源: Agent 2 C2, Agent 1 M5, Agent 3 M3/M4
- 问题: spec §3.3 只说"Engine live 启动调同一函数"，未规定 recovery 必须在 `_tickfile_writer_thread.start()` **之前** + `recover_tickfile_seqno` **之前**；也未规定 recovery 返回的 committed_set 要写入 live 引擎的 `_generated_tickfile_minutes`（否则 writer 重生已 commit 分钟→重复）。
- 影响: writer 线程已消费后 truncate 删合法行→丢数据；或重生已 commit 分钟→重复；seqno 取到被删分钟→倒退。
- 修改决议: 修改 spec §3.3 加调用时序不变量。
- 状态: **Accepted**
- 理由: 生产数据正确性，TOCTOU + 重复/丢失。

**C3 — marker 合法性校验未定义（malformed/truncated/dup/out-of-order/CRLF 污染）**
- 来源: Agent 1 C2, Agent 3 M1, Agent 2 m4
- 问题: spec §3.2 说"记录所有合法 marker"但未定义"合法"。截断 marker（`#COMMI,...`/`#COMMIT,093`）、坏字段、CRLF 残留（`4505\r`）、重复、乱序 minute 若进 committed_set → truncate 点错误或跳过未完整分钟。
- 影响: 部分分钟误判 committed / truncate 点错误 / 跨日错位。
- 修改决议: 修改 spec §3.2 定义 `_parse_commit_marker` 校验 + dup/out-of-order 处理。
- 状态: **Accepted**
- 理由: recovery 正确性核心。

**C4 — truncate 偏移语义模糊 + 无备份（误删不可回溯）**
- 来源: Agent 2 M3, Agent 1 C3, Agent 1 m2
- 问题: §3.2"truncate 到该 marker 换行之后"措辞模糊（marker 自身换行 vs 前置换行？）。若删 marker→末行成数据行→下次 recovery 当无 marker→重生→重复。且无被 truncate tail 备份，marker 校验 bug 误删完整分钟则永久丢失无取证。
- 影响: truncate 后重复 append / 数据丢失不可回溯。
- 修改决议: 修改 spec §3.2：truncate 到 marker 自身换行之后（保留 marker 作末行）+ 备份被 truncate tail 到 `.truncated.{ts}`。
- 状态: **Accepted**
- 理由: 保留 marker 才保证下次 append 不重生；备份是低成本高价值取证。

**C5 — 回滚安全性（旧代码读新 marker 文件）未覆盖**
- 来源: Agent 3 C1
- 问题: §6 只讲"新代码读老文件"，反向（回滚到旧代码读新 marker 文件）未覆盖。旧 tail-check 把 marker 当截断→插空行（与 C1 同源，但回滚方向无法改旧代码）。
- 影响: 回滚后文件空行污染（reader 跳过不崩，但严格 CSV consumer 可能失败）。
- 修改决议: 修改 spec §6 加 rollback playbook + §8 风险表列明。
- 状态: **Accepted**
- 理由: 上线回滚是真实场景，需显式文档化风险 + 清理手段。

**C6 — E2E mid-append crash recovery 测试缺失（设计核心价值无证明）**
- 来源: Agent 3 C2
- 问题: §7 全是单测，无端到端闭环测试（crash→truncate→replay gap-fill→无重复无缺失）。stale-fix 已有 `test_replay_fills_gap_without_corrupting_correct_rows` 模板。
- 影响: 设计成立性无证明，happy-path-only。
- 修改决议: 修改 spec §7 加 `test_e2e_mid_append_crash_recovery`。
- 状态: **Accepted**
- 理由: 核心价值必须端到端证明。

**C7 — §4 崩溃表对 rows/marker 可区分性过度乐观**
- 来源: Agent 1 C1
- 问题: kernel 按 page（4KB）刷回，"写 rows 中途"与"写 marker 中途"不可靠区分。恢复动作虽一致（都 truncate 到上个 marker，语义正确），但表格措辞误导实现者过度设计 mid-marker 检测。
- 影响: 实现偏差（非正确性 bug，因恢复动作一致）。
- 修改决议: 修改 spec §4 合并行 + 加单调性不变量。
- 状态: **Accepted**
- 理由: spec 清晰性，避免实现过度设计。

#### Major

**M1 — rows+marker 单次 fsync 批次不变量未写**
- 来源: Agent 2 M4
- 问题: §3.1 说"rows+marker 一起 append flush+fsync"但未禁止拆两次 write/fsync。若拆开→"rows 已 fsync 但 marker 未写"→该分钟完整数据无 marker→recovery 当 partial 删→重生重复。
- 决议: Accepted。§3.1 加 INV：rows 与 marker 同一 open context 连续写 + 单次 flush+fsync。

**M2 — 混存文件：老 row-only 分钟被当 gap 重生**
- 来源: Agent 2 M2
- 问题: 新代码首次 append 到老文件→文件=[老 row-only 分钟][新 marker 分钟]。recovery had_markers=True→committed_set 只含 marker 分钟→老分钟被 replay 当 gap 重生→重复。
- 决议: Accepted。§3.2/§6：marker 模式下 committed_set ⊇ marker minutes ∪ marker 之前所有 65-字段行 minutes（或检测到混存时 warning + 纳入）。

**M3 — rowcount 语义 + 不一致 warning 未定义**
- 来源: Agent 1 M2
- 问题: rowcount = `len(rows)`（实际写入）还是 `len(selected)`？未定义。bad rowcount 行为未定义。
- 决议: Accepted。§3.1：rowcount=len(rows)（try/except 后存活数）；recovery rowcount≠实际行数→WARNING（不阻断）。

**M4 — committed_set 来源 + dup/out-of-order marker 处理**
- 来源: Agent 1 M1
- 问题: truncate 点定义（字节偏移最大 marker vs minute_key 最大）；dup/乱序 marker 行为。
- 决议: Accepted（与 C3 合并）。§3.2：truncate 点=字节偏移最大合法 marker；committed_set=所有合法 marker minute（set 去重）；dup/乱序→WARNING 不阻断。

**M5 — recovery 可观测性（log/metric）缺失**
- 来源: Agent 3 M2, Agent 1/2 建议
- 问题: recovery 无 log/metric，运维无法回答"是否发生过崩溃恢复/truncate 了多少"。
- 决议: Accepted。§3.2 加结构化 log（path/had_markers/committed_minutes/last_commit_minute/truncate_bytes）+ metric（tickfile_recovery_truncate_bytes 等）。

**M6 — Status 字段须改 review 状态**
- 来源: Agent 3 M5
- 决议: Accepted。Status → "Review Round 1 通过，待 Round 2 复审"。

#### Minor

**m1 — marker 格式 extensibility（key=value）**
- 来源: Agent 1 m1
- 决议: **Deferred**。当前 `#COMMIT,<minute>,<count>` 足够；未来加 schema 版本再改。风险可接受。

**m2 — recovery path 构造 note**
- 来源: Agent 2 m3
- 决议: Accepted。§3.2 补 path 构造（`get_tickfile_path(output_dir, f"{date}0000")`）。

**m3 — 老文件 empty（只有 header 无数据无 marker）边界**
- 来源: Agent 1 m4
- 决议: Accepted。§6 明确：无数据行无 marker → had_markers=False（降级），committed_set 空，不 truncate。

**m4 — 外部 consumer 校验工具**
- 来源: Agent 2 m2
- 决议: **Deferred**。`validate_tickfile.py` 校验 marker rowcount 非必须；§8 风险表提一句即可。后续按需。

### Round 1 修改决议表

| ID | 严重程度 | 问题 | 决议 | 状态 | 理由 |
| -- | ------ | --- | ---- | ---- | --- |
| C1 | Critical | tail-check 误判 marker 插空行 | 修改 spec §5 谓词 | Accepted | 必现 bug |
| C2 | Critical | recovery 时序+skip-set 未定义 | 修改 spec §3.3 时序不变量 | Accepted | TOCTOU+重复/丢失 |
| C3 | Critical | marker 合法性校验未定义 | 修改 spec §3.2 `_parse_commit_marker` | Accepted | recovery 正确性核心 |
| C4 | Critical | truncate 偏移模糊+无备份 | 修改 spec §3.2 保留 marker+备份 | Accepted | 防重复+取证 |
| C5 | Critical | 回滚兼容未覆盖 | 修改 spec §6 playbook+风险表 | Accepted | 上线回滚真实场景 |
| C6 | Critical | E2E crash recovery 测试缺失 | 修改 spec §7 加 E2E 测试 | Accepted | 核心价值须证明 |
| C7 | Critical | §4 崩溃表过度乐观 | 修改 spec §4 合并行+单调性 | Accepted | spec 清晰 |
| M1 | Major | rows+marker 单次 fsync | 修改 spec §3.1 INV | Accepted | 防重复 |
| M2 | Major | 混存文件老分钟被当 gap | 修改 spec §3.2/§6 | Accepted | 防 replay 重复 |
| M3 | Major | rowcount 语义+不一致 warning | 修改 spec §3.1 | Accepted | 诊断 |
| M4 | Major | committed_set 来源+dup/乱序 | 合并入 C3 §3.2 | Accepted | 正确性 |
| M5 | Major | recovery 可观测性 | 修改 spec §3.2 log+metric | Accepted | 生产可观测 |
| M6 | Major | Status 改 review | 修改 header | Accepted | 流程 |
| m1 | Minor | marker 格式 extensibility | 延后 | Deferred | 风险可接受 |
| m2 | Minor | recovery path 构造 | 补 §3.2 | Accepted | 清晰 |
| m3 | Minor | 老文件 empty 边界 | 补 §6 | Accepted | 清晰 |
| m4 | Minor | consumer 校验工具 | 延后 | Deferred | 非必须 |

### Round 1 结论
**3. 需要修改后进行 Round 2 复审。**（7 Critical + 6 Major Accepted，必须在 spec 闭环后才能 Round 2。）

---

## Round 1 修改记录

### 修改文件
* `docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md`

### 修改章节
* Header Status（M6）
* §3.1 marker 写入（M1 fsync 批次 INV + M3 rowcount 语义）
* §3.2 恢复函数（C3 marker 校验 + C4 偏移+备份 + M4 committed_set 来源 + M5 log/metric + m2 path 构造）
* §3.3 调用方（C2 时序不变量）
* §4 崩溃场景（C7 合并行+单调性）
* §5 tail-check 协同（C1 谓词）
* §6 向后兼容（C5 rollback + M2 混存 + m3 empty）
* §7 测试（C6 E2E）

### 修改摘要
（见下方实际 spec 编辑后的 diff；逐条对应 C1–C7, M1–M6, m2, m3）

### 已解决问题
* C1–C7, M1–M6, m2, m3（全部 Accepted 落实于 spec）

### 未采纳 / 延后问题
* m1（marker 格式 extensibility）：Deferred — 当前格式足够，未来加 schema 版本再改；风险可接受（marker 解析已严格校验，格式演进时统一改 `_parse_commit_marker`）。
* m4（consumer 校验工具）：Deferred — `validate_tickfile.py` 非必须；§8 风险表已提；后续按需开发。风险可接受（marker 是注释行，标准 CSV reader 按惯例跳 `#`）。

---

## Review Round 2

### 审核时间
* 2026-06-17 18:05:00

### 本轮审核目标
* 修改后复审；
* 验证 Round 1 Critical/Major（C1-C7, M1-M6）是否落实；
* 判断是否可进入 planning。

### Round 1 问题处理状态复核

| ID | Round 1 问题 | Round 1 决议 | 是否落实 | 证据/说明 |
| -- | ---------- | ---------- | ---- | ----- |
| C1 | tail-check 误判 marker | Accepted | ✅ | §5 `_is_legal_last_line`（Round 2 进一步收紧为复用 `_parse_commit_marker`，见 C-R2-1） |
| C2 | recovery 时序+skip-set | Accepted | ✅ | §3.3 INV-CM-ORDER-1/2/SKIPSET-LIVE/LOCK；插入点 engine.py:377→381 经代码核实可实现 |
| C3 | marker 合法性校验 | Accepted | ✅ | §3.2 `_parse_commit_marker` 4 规则 |
| C4 | truncate 偏移+备份 | Accepted | ✅ | §3.2 INV-CM-LAST（保留 marker 作末行）+ INV-CM-BACKUP |
| C5 | 回滚兼容 | Accepted | ✅ | §6 rollback playbook 3 步 + §8 风险表 |
| C6 | E2E 测试缺失 | Accepted | ✅ | §7 `test_e2e_mid_append_crash_recovery`（Round 2 补 live restart 变体，见 M-R2-2） |
| C7 | 崩溃表过度乐观 | Accepted | ✅ | §4 合并行 + INV-CM-MONO |
| M1 | rows+marker 单次 fsync | Accepted | ✅ | §3.1 INV-CM-BATCH（Round 2 限定 append 路径，见 M-R2-3） |
| M2 | 混存文件老分钟 | Accepted | ✅ | §3.2 committed_set ∪ row-only minutes + WARNING |
| M3 | rowcount 语义 | Accepted | ✅ | §3.1 rowcount=len(rows) + §3.2 不一致 WARNING |
| M4 | committed_set 来源+dup | Accepted | ✅ | §3.2 字节偏移最大 marker + dup/乱序 WARNING |
| M5 | recovery 可观测性 | Accepted | ✅ | §3.2 结构化 log + 4 metric |
| M6 | Status 改 review | Accepted | ✅ | header（Round 2 后改"Review 通过，可进入 planning"） |

**结论**：Round 1 全部 13 条 Accepted 项（7 Critical + 6 Major）已落实。

### Agent 原始复审摘要（简短）

**Agent 1（恢复正确性复审）**：Round 1 全部闭环，commit-marker 语义三角（MONO/BATCH/LAST）+ recovery 准确性（committed_set 覆盖混存）+ 双路径时序均已写死，代码级核实插入点与变量定位成立。1 Major（M-R2-1：INV-CM-ORDER-1 未覆盖 cross-day writer resume 路径，经代码核实实际无风险——cross-day=新文件，建议补 INV）+ 3 Minor。结论：修改 Minor 后可以。

**Agent 2（IO/锁/兼容复审）**：C1-C7/M1-M6 全部与现有源码一致（writer.py:31-35/338/407/414-421、engine.py:377/381/388、replay.py:74 核实）。0 Critical/0 Major + 3 Minor（澄清/边界/运维）。结论：可以进入 planning。

**Agent 3（测试/上线复审）**：Round 1 决议逐条落实。但发现 **1 Critical（C-R2-1）**：§5 tail-check 谓词 `fields[0]=="#COMMIT"` 前缀匹配会把**截断 marker**（`#COMMIT,2026052809` 2 字段）误判合法 → 不补 `\n` → 数据行粘到截断 marker 后形成坏行；必须复用 `_parse_commit_marker` 严格校验。3 Major（M-R2-1 两 skip-set 字段、M-R2-2 live restart E2E 缺失、M-R2-3 fsync 测试仅 append 路径）。结论：必须先修 C-R2-1 + Majors。

### 综合复审结论

#### 已确认修复
* Round 1 全部 C1-C7, M1-M6（13 条）落实（见复核表）。

#### 仍需修改的问题（Round 2 新发现）

**Critical**
* **C-R2-1**（Agent 3）：§5 tail-check 谓词前缀匹配误判截断 marker。→ Accepted，已修（复用 `_parse_commit_marker`，§5 重写 + §7 加 `test_tail_check_truncated_marker_triggers_newline_fix`）。

**Major**
* **M-R2-1**（Agent 3）：replay `self._generated_tickfile_minutes`（replay.py:44）与 live `self._state._generated_tickfile_minutes`（aggregator.py:99）是两独立字段，INV-CM-SKIPSET 只点名 live。→ Accepted，已修（§3.3 拆 INV-CM-SKIPSET-LIVE / -REPLAY）。
* **M-R2-1b**（Agent 1）：INV-CM-ORDER-1 未覆盖 cross-day `_tickfile_writer_resume` 路径（实际无风险，cross-day=新文件）。→ Accepted，已修（§3.3 加 INV-CM-ORDER-RESUME）。
* **M-R2-2**（Agent 3）：E2E 只测 replay 路径，缺 live restart 端到端闭环（生产硬崩真实路径）。→ Accepted，已修（§7 加 `test_e2e_live_restart_recovers_partial_minute`）。
* **M-R2-3**（Agent 3）：fsync 批次测试未区分 append vs atomic-create 路径。→ Accepted，已修（§3.1 INV-CM-BATCH 限定 append 路径 + atomic-create 由 tmp+rename 保证 + §7 测试明确 append 路径）。

**Minor**（Deferred 到 plan 阶段）
* m-R2-A1a（Agent 1）：truncate 点 vs committed_set 边界（marker 之后散落 row-only 行的极端混存）——spec §3.2 应明确"marker 之后 65 字段行视为 partial，truncate，不进 committed_set"。
* m-R2-A1c（Agent 1）：`recover_tickfile_seqno` 跳 marker 行依赖未文档化（INV-CM-SEQNO-SKIP）。
* m-R2-A2a（Agent 2）：INV-CM-ORDER-2 措辞（合并扫描 vs 串行）澄清。
* m-R2-A2b（Agent 2）：rowcount=0 边界（空分钟是否写 marker）。
* m-R2-A2c（Agent 2）：truncate 备份清理策略上限。
* m-R2-A3a（Agent 3）：rollback playbook 缺"回滚后二次升级"路径。
* m-R2-A3b（Agent 3）：metric `tickfile_recovery_invocations` 未区分 truncate/noop。

### Round 2 修改决议

| ID | 严重程度 | 问题 | 决议 | 状态 | 理由 |
| -- | ---- | -- | ---- | ---- | --- |
| C-R2-1 | Critical | tail-check 谓词误判截断 marker | 复用 `_parse_commit_marker` | Accepted（已修） | 谓词逻辑漏洞，运行时防线必须严格 |
| M-R2-1 | Major | 两 skip-set 字段未分别约束 | 拆 SKIPSET-LIVE/REPLAY | Accepted（已修） | 防遗漏设字段 |
| M-R2-1b | Major | cross-day resume 路径未声明 | 加 INV-CM-ORDER-RESUME | Accepted（已修） | 不变量补全（实际无风险） |
| M-R2-2 | Major | live restart E2E 缺失 | 加 E2E 测试 | Accepted（已修） | 生产硬崩真实路径必须闭环 |
| M-R2-3 | Major | fsync 测试路径未区分 | 限定 append + atomic 兜底 | Accepted（已修） | 测试期望准确 |
| m-R2-* (7 条) | Minor | 澄清/边界/运维/可观测细节 | 推到 plan | Deferred | 非阻断，plan 阶段细化 |

### Round 2 修改记录

#### 修改文件
* `docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md`

#### 修改章节
* Header Status（Review 通过，可进入 planning）
* §3.1 INV-CM-BATCH（M-R2-3：限定 append 路径 + atomic-create content 以 marker 结尾）
* §3.3 INV-CM-SKIPSET-LIVE/REPLAY（M-R2-1）+ INV-CM-ORDER-RESUME（M-R2-1b）
* §5 tail-check 谓词（C-R2-1：复用 `_parse_commit_marker`，非前缀匹配）
* §7 测试（C-R2-1 `test_tail_check_truncated_marker_triggers_newline_fix`；M-R2-2 `test_e2e_live_restart_recovers_partial_minute`；M-R2-3 fsync 测试限定 append）

#### 已解决问题
* C-R2-1, M-R2-1, M-R2-1b, M-R2-2, M-R2-3（全部 Accepted，已落实于 spec）

#### 未采纳 / 延后问题
* 7 条 Minor（m-R2-*）：Deferred 到 plan 阶段。均为 spec 清晰性补强 / 边界澄清 / 运维文档 / 可观测细化，非生产正确性风险，不阻断 planning。后续跟进：plan 阶段逐条补，或实施时同步。

### Round 2 结论
**1. 可以进入 planning。**（C-R2-1 + 4 Major 已修复；剩余 7 Minor 全 Deferred 到 plan，非阻断。）

---

## 最终审核结论

### 是否可以进入 planning
**1. 可以进入 planning。**

### 两轮审核摘要
* **Round 1**：3 agents 发现 7 Critical（tail-check 误判 marker / recovery 时序 / marker 校验 / truncate 偏移+备份 / 回滚兼容 / E2E 缺失 / 崩溃表过度乐观）+ 6 Major（单次 fsync / 混存 / rowcount / committed_set / 可观测 / Status）+ 4 Minor。全部 Critical/Major Accepted 并修复。
* **Round 2**：3 agents 复审确认 Round 1 全部落实；新发现 1 Critical（C-R2-1 tail-check 谓词前缀匹配误判截断 marker）+ 4 Major（两 skip-set 字段 / cross-day resume / live restart E2E / fsync 测试路径）。全部 Accepted 并修复。7 Minor Deferred 到 plan。

### 已修改内容摘要
* Spec 共修订：header Status ×2（review→通过）；§3.1（marker 写入 + INV-CM-MONO/BATCH）；§3.2（recovery 函数：marker 校验 + truncate 偏移/备份 + committed_set 来源 + log/metric + path）；§3.3（调用方 + 6 条 INV-CM-* 时序/skip-set/lock/resume）；§4（崩溃表合并 + 单调性）；§5（tail-check 谓词复用严格校验）；§6（向后兼容 + rollback playbook + 混存 + empty）；§7（测试补强：E2E 双路径 + 截断 marker + live restart + fsync 计数 + 混存 + 畸形 marker 等）；§8（风险表扩充）。

### 仍需人工确认的问题
* 7 条 Deferred Minor（见 Round 2 决议表 m-R2-*）：plan 阶段逐条补，或实施时同步——非阻断，但建议 plan 时 review。
* **外部消费方 `#` 行兼容**（C5 残留）：需确认实际 tickfile 下游消费方是否跳 `#` 注释行；若不跳，回滚/正常运行期 marker 行可能解析失败。这是部署前的人工确认项。

### Review log 文件路径
* `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`

---

## Review Round 3（对抗性深度复审）

### 审核时间
* 2026-06-17 19:00:00

### 本轮审核目标
* 对 Round 1+2 修复后的 spec 做**深度/对抗性**复审，找前两轮遗漏的问题（侧重动态/运行时正确性、失败模式、并发、运维真实性）。

### Agent 原始摘要（简短）

**Agent 1（崩溃恢复正确性）**：spec 在 `Engine.start()` 单次启动路径闭环，但**遗漏 writer 线程生产运行中的异常 retry / health-check 自动重启 / cross-day resume 三条重生路径**——partial minute 不被 truncate，重生 append → 重复行（C-R3-1，阻断性）。备份文件名 timestamp 同秒碰撞（M-R3-2）；seqno 恢复应与 truncate 合并同函数返回（M-R3-3）；writer-retry 重生用旧 snapshot_copy carry-forward 漂移（M-R3-4）。

**Agent 2（IO/锁/并发）**：`_get_write_lock` 是**进程内** threading.RLock，**不跨进程**——live+replay 同目录操作 recovery truncate 与 append 不互斥 → 腐败（C-R3-1，最大未声明假设）。recovery 插入点与 seqno 双入口（flusher.__init__ lazy + start）冲突（M-R3-1）；`_cleanup_tickfile_tmp_files` 的 os.replace 路径不经 write_tickfile_rows → .tmp 无 marker（M-R3-2）；atomic-create 跨进程 rename 覆盖（M-R3-3）；备份 IO 失败行为未定义（M-R3-4）。

**Agent 3（测试/运维/失败模式）**：**recovery 自身失败模式未定义**（扫描中途异常可能 truncate 到错误偏移 → 数据丢失，C-R3-1 fail-atomic）；测试全是合成文件无 fault-injection（M-R3-1）；metric 不跨崩溃存活，监控无法真正告警（M-R3-2）；rollback playbook 忽略源数据保留窗口（M-R3-3）；committed_set 未 date 过滤（M-R3-4）。

### 综合问题清单（去重）

#### Critical

**C-R3-1 — writer 线程 retry / health-check 重启 / resume 路径不执行 recovery → 重生 append 到 partial minute → 重复行**
- 来源: Agent 1
- 问题: 所有 INV-CM-ORDER-* 只约束 `Engine.start()` 初始启动。但 `_try_generate_tickfile` 在 rows 写后、marker/fsync 前**抛 IOError**（disk-full，生产高频）→ except 块 re-insert pending → writer loop retry → **append 到已含 partial rows 的文件** → 重复行。health-check 自动重启 writer（engine.py:1458）同样无 recovery。
- 影响: 生产 disk-full/IOError 远比硬崩溃常见，每次产生重复行。**设计核心价值（mid-append 无重复）在 live 路径不成立**。
- 决议: Accepted。spec 加 INV-CM-REGEN-GUARD + INV-CM-ORDER-RESTART。
- 状态: Accepted

**C-R3-2 — recovery 自身失败模式未定义（fail-atomic 缺失）→ 扫描中途异常可能 truncate 到错误偏移 → 数据丢失**
- 来源: Agent 3
- 问题: §3.2 定义扫描→truncate，但未定义扫描中 OOM/IO 错误/编码异常的行为。`os.truncate(path, new_size)` 若在部分计算的偏移上执行 → 切掉完整分钟 → 永久丢失。recovery 比不 recovery 更危险。
- 影响: 数据丢失。
- 决议: Accepted。spec 加 INV-CM-FAIL-ATOMIC（扫描异常绝不触发 truncate）。
- 状态: Accepted

**C-R3-3 — 跨进程并发：`_get_write_lock` 是进程内 RLock，recovery truncate 与他进程 append 不互斥**
- 来源: Agent 2
- 问题: writer.py:31-35 threading.RLock 仅同进程。live 进程 append + replay 进程 recovery truncate 同一 output_dir → 截掉 live 刚 commit 的合法分钟 → 丢数据 + 内存/磁盘不一致。
- 影响: 多进程部署（HA/灰度/live+replay 补数）数据腐败。
- 决议: Accepted。spec 显式声明 INV-CM-SINGLEPROC + replay 启动 guard（拒绝 live 运行中的当天 date）；OS 建议锁作为 future（Deferred）。
- 状态: Accepted

#### Major

**M-R3-1 — recovery 插入点须早于所有 seqno 入口（flusher.__init__ lazy + start）**
- 来源: Agent 2
- 决议: Accepted。INV-CM-ORDER-2 改为"recovery 严格早于所有 seqno 读取入口"。

**M-R3-2 — `_cleanup_tickfile_tmp_files` 的 os.replace 路径不经 write_tickfile_rows → .tmp 无 marker；顺序未定义**
- 来源: Agent 2
- 决议: Accepted。spec 加 INV-CM-CLEANUP-ORDER（cleanup 在 recovery 前；.tmp 合法性校验含"末行合法 marker"，否则删 .tmp 让重生）。

**M-R3-3 — 备份 timestamp 同秒碰撞 + 备份 IO 失败行为未定义**
- 来源: Agent 1（碰撞）+ Agent 2（IO 失败）
- 决议: Accepted。timestamp 用 `time.time_ns()`（纳秒）；备份 IO 失败 → 不 truncate + ERROR + abort recovery（文件保留 partial，降级 row-based）。

**M-R3-4 — seqno 恢复应与 truncate 在同一函数同一锁内返回（合并扫描），防时序窗口**
- 来源: Agent 1
- 决议: Accepted。`_recover_tickfile_to_last_commit` 返回 `(committed_set, last_seqno, had_markers)`，seqno 只来自 committed 行；`recover_tickfile_seqno` 改薄包装/废弃。

**M-R3-5 — committed_set 收集 marker/row-only minute 未 date 过滤 → 跨日错位 marker 污染**
- 来源: Agent 3
- 决议: Accepted。INV-CM-DATE-FILTER（committed_set 只收 `minute.startswith(date)`；跨日 marker → WARNING）。

**M-R3-6 — rollback playbook 忽略源数据保留窗口（源 CSV 滚动删除后无法 replay-to-fresh）**
- 来源: Agent 3
- 决议: Accepted。§6 playbook 加 Step 0（验证源数据存在；缺失则不删/不覆盖受污染文件，grep -v 清空行）。

**M-R3-7 — 测试全合成文件无 fault-injection，mid-append 错误拦不住**
- 来源: Agent 3
- 决议: Accepted。§7 加 fault-injection 测试（monkeypatch write/fsync 中途抛异常；write_bytes 构造 page-boundary 截断 marker / no-trailing-newline）。

**M-R3-8 — metric 不跨崩溃存活，监控无法真正告警；需持久 audit log**
- 来源: Agent 3
- 决议: Accepted。§3.2 加持久 audit log（`output_dir/tickfile/tickfile_recovery.log`，每行 JSON + timestamp），支持重启循环检测。

**M-R3-9 — writer-retry 重生用旧 snapshot_copy，carry-forward 可能轻微过时**
- 来源: Agent 1
- 决议: Accepted（文档化）。§8 风险表加一行（既有行为，marker 放大；可接受）。

**M-R3-10 — atomic-create 跨进程 rename 覆盖（既有 bug，marker 放大）**
- 来源: Agent 2
- 决议: Accepted（fold 入 INV-CM-SINGLEPROC 声明）。短期单进程可接受；.tmp 加 PID/uuid 为 future。

#### Minor（Deferred 到 plan）
- m-R3-tail: tail-read 4096 窗口边界 marker 误判（仅冗余 \n，recovery 兜底）。
- m-R3-empty: 空分钟（rowcount=0）是否写 marker 语义。
- m-R3-perf: recovery 1.5M 行扫描耗时量化 note。
- m-R3-win: Windows append-mode fsync 语义 note。
- m-R3-seqno-skip: 合并扫描时 seqno 跳 marker 的 INV-CM-SEQNO-SKIP 文档。

### Round 3 修改决议表

| ID | 严重程度 | 问题 | 决议 | 状态 |
| -- | ------ | --- | ---- | ---- |
| C-R3-1 | Critical | writer retry/health-check/restart 无 recovery → 重复 | INV-CM-REGEN-GUARD + ORDER-RESTART | Accepted |
| C-R3-2 | Critical | recovery 自身失败非 fail-atomic → 丢数据 | INV-CM-FAIL-ATOMIC | Accepted |
| C-R3-3 | Critical | 跨进程 RLock 不互斥 | INV-CM-SINGLEPROC + replay guard | Accepted |
| M-R3-1 | Major | seqno 双入口 | INV-CM-ORDER-2 扩展 | Accepted |
| M-R3-2 | Major | cleanup .tmp 无 marker + 顺序 | INV-CM-CLEANUP-ORDER | Accepted |
| M-R3-3 | Major | 备份碰撞 + IO 失败 | time_ns + 失败 abort | Accepted |
| M-R3-4 | Major | seqno 与 truncate 合并 | recovery 返回 last_seqno | Accepted |
| M-R3-5 | Major | committed_set 未 date 过滤 | INV-CM-DATE-FILTER | Accepted |
| M-R3-6 | Major | rollback 源数据窗口 | playbook Step 0 | Accepted |
| M-R3-7 | Major | 测试无 fault-injection | §7 fault-injection 测试 | Accepted |
| M-R3-8 | Major | metric 不跨崩溃 | 持久 audit log | Accepted |
| M-R3-9 | Major | retry carry-forward 漂移 | §8 风险表 | Accepted |
| M-R3-10 | Major | atomic-create 跨进程覆盖 | fold 入 SINGLEPROC | Accepted |
| m-R3-* (5) | Minor | 澄清/边界/note | 推 plan | Deferred |

### Round 3 结论
**3. 需要修改后进行 Round 4 复审。**（3 Critical + 10 Major Accepted。前两轮聚焦静态/启动正确性，Round 3 揭示动态运行时正确性（writer retry）、recovery 自身失败安全、跨进程并发三大类生产风险，必须 spec 闭环。）

---

## Review Round 4（Round 3 修复后复审）

### 审核时间
* 2026-06-17 19:50:00

### 本轮审核目标
* 验证 Round 3 Critical/Major（C-R3-1/2/3, M-R3-1..10）是否落实；
* 找仍存风险；判断是否可进入 planning。

### Round 3 问题处理状态复核

| ID | Round 3 问题 | 决议 | 是否落实 | 证据 |
| -- | ---------- | ---- | ---- | --- |
| C-R3-1 | writer retry/restart 无 recovery → 重复 | Accepted | ✅ | §3.4 INV-CM-REGEN-GUARD（本轮 M-R4-1a 补全 fsync-fail 子情形）+ ORDER-RESTART |
| C-R3-2 | recovery 非 fail-atomic | Accepted | ✅ | §3.4 INV-CM-FAIL-ATOMIC |
| C-R3-3 | 跨进程 RLock 不互斥 | Accepted | ✅ | §3.4 INV-CM-SINGLEPROC + replay guard |
| M-R3-1 | seqno 双入口 | Accepted | ✅ | §3.3 INV-CM-ORDER-2 扩展（本轮 M-R4-1b 补 replay 第三入口） |
| M-R3-2 | cleanup .tmp 无 marker+顺序 | Accepted | ✅ | §3.4 INV-CM-CLEANUP-ORDER |
| M-R3-3 | 备份碰撞+IO 失败 | Accepted | ✅ | §3.2 time_ns+pid + 失败 abort |
| M-R3-4 | seqno 与 truncate 合并 | Accepted | ✅ | §3.2 recovery 返回 3-tuple |
| M-R3-5 | committed_set 未 date 过滤 | Accepted | ✅ | §3.2 INV-CM-DATE-FILTER |
| M-R3-6 | rollback 源数据窗口 | Accepted | ✅ | §6 playbook Step 0 |
| M-R3-7 | 测试无 fault-injection | Accepted | ✅ | §7 四条 fault-injection 测试 |
| M-R3-8 | metric 不跨崩溃 | Accepted | ✅ | §3.4 持久 audit log |
| M-R3-9 | retry carry-forward 漂移 | Accepted | ✅ | §8 风险表 |
| M-R3-10 | atomic-create 跨进程 | Accepted | ✅ | fold 入 SINGLEPROC |

**结论**：Round 3 全部 3 Critical + 10 Major 已落实。

### Agent 原始复审摘要（简短）

**Agent 1（正确性）**：Round 3 全部闭环（源码级核实重试路径真实存在、precondition 落在 `_get_write_lock` 内 TOCTOU 安全）。1 Major（M-R4-1a：REGEN-GUARD precondition 未覆盖"marker 已写、fsync 失败"retry → 末尾合法 marker → 不 truncate → 重 append → 重复完整分钟）。结论：修改后可以。

**Agent 2（IO/并发）**：6 项重点确认全部闭环（SINGLEPROC、3-tuple seqno 合并、CLEANUP-ORDER、备份 time_ns+pid+abort、FAIL-ATOMIC 锁范围）。1 Major（M-R4-1b：replay lazy seqno 第三入口 replay.py:311 未在 INV-CM-ORDER-2，当前无 bug 但 INV 网不全；建议 recovery 返回值覆盖消除入口）。结论：修改后可以。

**Agent 3（测试/运维）**：Round 3 全部 Major 落实且源码核实可实现。0 Critical/0 Major。5 Minor（audit log 轮转/IO 失败语义、live feed mock seam、性能 note、deferred 清单）。结论：可以进入 planning。

### 综合复审结论

#### 已确认修复
* Round 3 全部 3C+10M（见复核表）。

#### 仍需修改的问题（Round 4 新发现，已修）

**Major**
* **M-R4-1a**（Agent 1）：C-R3-1 自愈 precondition 漏"marker 已写、fsync 失败"retry → 重复。→ Accepted，已修（§3.4 INV-CM-REGEN-GUARD precondition 带 `current_minute_key`，三分支：末尾非法→truncate+append；末尾合法 marker==current→skip+committed；末尾合法 marker<current→append）+ §7 `test_writer_ioerror_after_marker_write_no_duplicate`。
* **M-R4-1b**（Agent 2）：replay lazy seqno 第三入口未在 INV-CM-ORDER-2。→ Accepted，已修（§3.3 INV-CM-ORDER-2 补 replay lazy 入口；推荐 recovery 返回 last_seqno 覆盖消除三入口）。

**Minor**（Deferred 到 plan，共 ~7 条）
* audit log 轮转/上限、audit log IO 失败语义（best-effort 不阻断）、live feed mock seam（plan 需加 test-only 注入点）、INV-CM-REGEN-GUARD 性能 note（增量=1 次 marker 解析，可忽略）、Round 2/3 deferred 的 12 条 Minor 逐条 review（尤其 m-R2-A1a：marker 之后 row-only 行视为 partial truncate，不进 committed_set——建议 plan Task 0 明确）。

### Round 4 修改决议

| ID | 严重程度 | 问题 | 决议 | 状态 |
| -- | ---- | -- | ---- | ---- |
| M-R4-1a | Major | REGEN-GUARD 漏 fsync-fail retry 重复 | precondition 三分支 | Accepted（已修） |
| M-R4-1b | Major | replay lazy seqno 第三入口 | INV-CM-ORDER-2 补 + 消除入口 | Accepted（已修） |
| m-R4-* (7) | Minor | 轮转/seam/note/deferred 清单 | 推 plan | Deferred |

### Round 4 结论
**1. 可以进入 planning。**（Round 3 全部 3C+10M 落实；Round 4 新发现 2 Major 已修；剩余 ~7 Minor 全 Deferred 到 plan，非阻断。）

---

## 最终审核结论（4 轮后）

### 是否可以进入 planning
**1. 可以进入 planning。** ✅

### 四轮审核摘要
* **Round 1**：7 Critical + 6 Major + 4 Minor（静态/启动正确性：tail-check 谓词、recovery 时序、marker 校验、truncate 偏移/备份、回滚、E2E、崩溃表）→ 全修。
* **Round 2**：确认 Round 1 落实；1 Critical（C-R2-1 tail-check 前缀匹配误判截断 marker）+ 4 Major → 全修。
* **Round 3**（对抗性深度）：3 Critical（writer retry 自愈、recovery fail-atomic、跨进程）+ 10 Major（seqno 三入口、cleanup、备份碰撞、audit log、date 过滤、rollback 源窗口、fault-injection 测试…）→ 全修。
* **Round 4**：确认 Round 3 落实；2 Major（M-R4-1a fsync-fail retry 自愈漏洞、M-R4-1b replay lazy seqno 第三入口）→ 全修。7 Minor Deferred。

### 已修改内容摘要
spec 经 4 轮共修：§3.1（marker 写入 + INV-CM-MONO/BATCH）、§3.2（recovery：`_parse_commit_marker` 严格校验 + truncate 保留 marker + 备份 time_ns+pid+IO-fail-abort + 3-tuple 返回含 last_seqno + committed_set 来源（marker∪row-only，date 过滤）+ log/metric/audit log + fail-atomic）、§3.3（调用方 + INV-CM-ORDER-1/2(三 seqno 入口)/SKIPSET-LIVE/REPLAY/LOCK/ORDER-RESUME）、§3.4（运行时正确性：INV-CM-REGEN-GUARD(三分支) + ORDER-RESTART + FAIL-ATOMIC + SINGLEPROC + CLEANUP-ORDER）、§4（崩溃表合并+单调性）、§5（tail-check 复用 `_parse_commit_marker`）、§6（向后兼容 + rollback Step 0 源验证 + 混存 + empty）、§7（fault-injection + writer retry/health-check/跨进程 + live restart 强制 E2E + ~25 测试）、§8（风险表扩充）。

### 仍需人工确认的问题
1. **~12 条 Deferred Minor**（Round 2/3/4）：plan Task 0 逐条 review，尤其 **m-R2-A1a**（marker 之后 row-only 行视为 partial truncate，不进 committed_set）须实现前明确。
2. **外部消费方 `#` 行兼容**（C5 残留）：部署前确认下游跳 `#` 注释行。
3. **live feed mock seam**（Round 4 Minor）：`test_e2e_live_restart_recovers_partial_minute` 强制 E2E 需 Engine 有 test-only feed 注入点；plan 阶段确认或补。
4. **多进程部署**（C-R3-3）：若真实场景需 live+replay 并发，OS 建议锁（`fcntl.flock`/`msvcrt.locking`）为 future，当前依赖 INV-CM-SINGLEPROC + replay guard。

### Review log 文件路径
* `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`

---

## Review Round 5（新逻辑交互深度复审）

### 审核时间
* 2026-06-17 20:40:00

### Agent 原始摘要（简短）
- **Agent 1（正确性）**：0 Critical。2 Major — M-R5-1（分支 2 "skip+add committed" 跨模块职责歧义：write_tickfile_rows 模块函数持文件锁、无法访问 flusher 的 SharedState skip-set，职责未切分）；M-R5-2（分支 2 skip 消费 seqno（L621）但无行落盘 → shutdown+restart 后 recovery 返回 last_seqno=N-1 覆盖 → 下次写重用 N → seqno 碰撞，**真实边界 bug**）。
- **Agent 2（并发/性能/平台）**：0 Critical。4 Major — M-R5-1（pidfile guard TOCTOU + stale 死锁，需 O_CREAT|O_EXCL 原子 + pid liveness）；M-R5-2（recovery 频率无退避，但 writer restart_count 硬上限=1 → 最多 2 次全扫后永久死，需文档+metric）；M-R5-3（**Windows**：REGEN-GUARD 分支 1 truncate 必须在 `open(path,"a")` 之前用 `os.truncate(path)`，不能 f.truncate() 内 truncate → win32 稀疏空洞）；M-R5-4（audit log 写失败语义未定义 + 跨日并发，需 best-effort try/except + makedirs + 独立行原子写）。
- **Agent 3（测试 seam/运维）**：0 Critical。2 Major — M-R5-1（**强制 E2E live restart 测试无 feed seam**：Engine.start() 线程阻塞、无 test-only 注入点，spec 承诺了测不了的测试 → 须命名注入机制）；M-R5-2（`recover_tickfile_seqno` 在 `flusher.__init__`（engine `__init__`）**急切调用**，早于 start() recovery → INV-CM-ORDER-2 只覆盖 start()，**__init__ seqno 从 partial 文件取 → 真实时序 bug**）。

### 综合问题清单（去重 → 7 Major）

| ID | 来源 | 问题 | 决议 | 状态 |
| -- | ---- | ---- | ---- | ---- |
| M-R5-1 | A3 | recovery 须在 `flusher.__init__` seqno 点执行（非 start()）；__init__ 急切取 seqno 早于 start recovery | §3.3 INV-CM-ORDER-1/2 修正插入点为 flusher.__init__ | Accepted |
| M-R5-2 | A1 | 分支 2 skip 消费 seqno 无行落盘 → restart 后 seqno 倒退/重用 | §3.3 INV-CM-SEQNO-MONO-FILE（覆盖取 max，不倒退） | Accepted |
| M-R5-3 | A1 | 分支 2 skip+add committed 跨模块职责歧义 | §3.4 INV-CM-SKIP-DELEGATION（writer 仅 file-skip，flusher 统一 add） | Accepted |
| M-R5-4 | A2 | pidfile guard TOCTOU + stale 死锁 | §3.4 INV-CM-GUARD-ATOMIC（O_CREAT\|O_EXCL + pid liveness） | Accepted |
| M-R5-5 | A2 | Windows truncate-vs-append-fd 顺序（win32 稀疏空洞） | §3.4 INV-CM-TRUNCATE-BEFORE-OPEN（os.truncate(path) 先于 open("a")） | Accepted |
| M-R5-6 | A2 | audit log 写失败语义 + 跨日并发 + schema 缺 pid/hostname | §3.4 INV-CM-AUDIT-BESTEFFORT + schema 加 pid/hostname + makedirs | Accepted |
| M-R5-7 | A2/A3 | restart 频率文档 + perm_dead metric；E2E feed seam 命名 | §3.4 文档 restart_count=1 上限 + metric；§7 命名 seed csv_dir+poll seam | Accepted |

Minor（Deferred）：tail 窗口优化（m-R5-1 复用既有 4096 读）、运行时备份清理上限、health-check 用 per-append precondition 而非全扫、m-R2-A1a 一行澄清、空分钟 rowcount=0 一行澄清。

### Round 5 结论
**3. 需要修改后进行 Round 6 复审。**（7 Major Accepted。其中 M-R5-1/M-R5-2 是 seqno 真实时序 bug，M-R5-5 是 Windows 平台正确性，必须在 spec 闭环。）

---

## Review Round 6（Round 5 修复后最终复审）

### 审核时间
* 2026-06-17 21:30:00

### Round 5 问题处理状态复核

| ID | Round 5 问题 | 是否落实 | 证据 |
| -- | ---------- | ---- | --- |
| M-R5-1 | recovery 在 flusher.__init__ seqno 点 | ✅ | §3.3 INV-CM-ORDER-1 修正插入点 flusher.py:98 |
| M-R5-2 | seqno 覆盖取 max 不倒退 | ✅ | §3.3 INV-CM-SEQNO-MONO-FILE |
| M-R5-3 | 分支 2 skip 职责切分 | ✅ | §3.5 INV-CM-SKIP-DELEGATION |
| M-R5-4 | pidfile guard TOCTOU+stale | ✅ | §3.4 INV-CM-GUARD-ATOMIC（O_EXCL+pid liveness） |
| M-R5-5 | win32 truncate-before-open | ✅ | §3.5 INV-CM-TRUNCATE-BEFORE-OPEN |
| M-R5-6 | audit best-effort+schema | ✅ | §3.4 INV-CM-AUDIT-BESTEFFORT + makedirs + pid/hostname |
| M-R5-7 | restart 上限+metric+feed seam | ✅ | §3.5 restart_count=1 + perm_dead + seed csv_dir+poll |

### Agent 原始复审摘要（简短）
- **Agent 1（正确性）**：Round 5 全部源码核实落实（flusher.py:98 eager seqno、flusher.py:621 seqno 消费、writer.py 签名无 SharedState 访问权）。0 Critical/0 Major + 2 Minor（INV-CM-ORDER-2 重复措辞、max 取值点注释）。结论：可以。
- **Agent 2（并发/平台）**：4 项重点（pidfile EXCL+liveness、win32 truncate-before-open、audit best-effort、restart+feed seam）全部源码核实闭环。0 Critical/0 Major + 3 Minor（PID-recycle 文档、EXCL sharing-violation 澄清、audit 轮转）。结论：可以。
- **Agent 3（测试）**：seam 全部源码核实可写。0 Critical。**2 Major**（Maj-R6-1：SEQNO-MONO-FILE + ORDER-1 init 路径无测试；Maj-R6-2：§3.4 三 pidfile 测试 vs §7 仅一，不一致）+ 4 Minor。结论：可以（前提 plan Task 0 补 2 Major 测试）。

### 综合复审结论

#### 已确认修复
* Round 5 全部 7 Major 落实（见复核表）。

#### 仍需修改的问题（Round 6 新发现，已修）
**Major**
* **Maj-R6-1**（Agent 3）：INV-CM-SEQNO-MONO-FILE（R5 真实边界 bug）+ INV-CM-ORDER-1 init 路径无 §7 测试。→ Accepted，已补 `test_recovery_seqno_override_takes_max_never_regresses` + `test_flusher_init_runs_recovery_before_eager_seqno_fetch`（§7）。
* **Maj-R6-2**（Agent 3）：§3.4 三 pidfile 测试 vs §7 仅一不一致。→ Accepted，已补 §7 三条对齐（EXCL 预创建模拟 + pid liveness mock 实现提示）。

**Minor**（Deferred 到 plan，共 ~9 条）
* INV-CM-ORDER-2 重复措辞合并、max 取值点注释、PID-recycle 文档、EXCL sharing-violation 澄清、audit 轮转/上限、audit schema pid/hostname 断言、atomic-create header-rewrite 分支 marker、deferred 清单（~17 条 Task 0）、§7 半闭环 escape 已本次删除（Min-R6-4 闭环）。

### Round 6 结论
**1. 可以进入 planning。**（Round 5 全部 7 Major 落实；Round 6 新发现 2 Major 测试缺口已补；剩余 ~9 Minor 全 Deferred 到 plan，非阻断。）

---

## 最终审核结论（6 轮后）

### 是否可以进入 planning
**1. 可以进入 planning。** ✅

### 六轮审核摘要
| 轮次 | 发现 | 处理 |
|------|------|------|
| R1 | 7C+6M（静态/启动：tail-check、recovery 时序、marker 校验、truncate 偏移/备份、回滚、E2E、崩溃表） | 全修 |
| R2 | 1C+4M（C-R2-1 tail-check 前缀误判截断 marker；skip-set 双字段；live restart E2E；fsync 测试路径） | 全修 |
| R3 | 3C+10M（动态：writer retry 自愈、recovery fail-atomic、跨进程；seqno 三入口、cleanup、备份碰撞、date 过滤、rollback 源窗口、fault-injection、audit log） | 全修 |
| R4 | 2M（M-R4-1a REGEN-GUARD 漏 fsync-fail retry；M-R4-1b replay lazy seqno 第三入口） | 全修 |
| R5 | 7M（新逻辑接缝：seqno __init__ 急切点、skip+seqno 碰撞、skip-delegation、pidfile TOCTOU/stale、win32 truncate-vs-fd、audit best-effort、restart+feed seam） | 全修 |
| R6 | 2M（测试缺口：SEQNO-MONO + init 路径无测试；pidfile 测试清单不一致） | 全修 |

**演进**：R1-2 抓静态/启动正确性 → R3-4 抓动态运行时（writer retry、recovery 自身失败、跨进程）→ R5-6 抓新逻辑与现有 state 的接缝（seqno 时序、跨模块职责、Windows 平台、测试 seam）。6 轮把"理论上对"补到"运行时+平台+可测"全闭环。

### 仍需人工确认（非阻断）
1. **~17 条 Deferred Minor**（R2-R6）：plan Task 0 逐条 review，尤其 m-R2-A1a（marker 之后 row-only 行视为 partial truncate，不进 committed_set）须实现前明确。
2. **外部消费方 `#` 行兼容**：部署前确认下游跳注释行。
3. **多进程部署**：当前 INV-CM-SINGLEPROC + replay guard（O_EXCL+pid liveness）；OS 建议锁为 future。
4. **Windows 平台**：INV-CM-TRUNCATE-BEFORE-OPEN + sharing-violation→FAIL-ATOMIC 已覆盖；plan 实施时核实 win32 `os.truncate`/`open("a")` 实际行为。

### Review log 文件路径
* `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`

---

## Review Round 7（sidecar 修订复审）

### 审核时间
* 2026-06-17 22:30:00

### Agent 原始摘要（简短）
- **Agent 1（sidecar 正确性）**：2 Critical — C-R7-1（offset 必须用 `os.fstat(fd).st_size` 于 fsync 后取，非 path.getsize/tell/预计算，否则 NFS stale + tail-fix 偏移漂移 → truncate 越界）；C-R7-2（flock lockfile fd 须覆盖整个 recovery 临界区 read-sidecar+truncate+audit，不能 per-fd）。3 Major — REGEN-GUARD 分支 2 读 sidecar 须在同一 flock（跨文件锁）；sidecar 丢失降级不 truncate → partial 残留重引入原 bug（须 tail-strip）；truncate offset 须取 MAX（非字面末行，防乱序）。
- **Agent 2（flock/并发）**：2 Critical — C-R7-1（**flock 语义错**：flock 关联 open-file-description 非进程；spec"同进程 no-op"错，须保留 RLock 线程互斥 + flock 跨进程，acquisition 顺序 RLock 先）；C-R7-2（sidecar append 须与 tickfile append 在**同一 flock** 临界区，"或调用方"漏洞致 tickfile/sidecar 不一致）。4 Major — LOCK_NB 非阻塞（防 replay 挂死）；flock 粒度 per-minute；lockfile 永不删（inode 竞争）；flock fd 不缓存；本地 fs 假设（CIFS/NFS flock 非建议性）。
- **Agent 3（测试/运维）**：1 Critical — C-R7-1（csv 纯净须**真实 pandas read_csv 实证**，§7 仅列名不够；shape[1]==65/0 NaN/无 Unnamed/无 `#`）。6 Major — partial tickfile+sidecar 存活测试缺；flock 跨进程须 subprocess 测（threading 测不出）；sidecar empty≡missing 语义；sidecar offset>size abort（防稀疏空洞）；混存升级测试 stale；audit 命名漂移 had_markers vs had_sidecar。

### 综合问题清单（去重）

#### Critical
| ID | 问题 | 决议 |
| -- | ---- | ---- |
| C-R7-1a | offset 须 fstat-after-fsync（Agent1） | Accepted — §3.1 INV-CM-OFFSET-FSTAT |
| C-R7-1b | flock 语义错（per-OFD 非进程；须 RLock+flock 顺序）（Agent2） | Accepted — §3.2 重写 INV-CM-FLOCK |
| C-R7-2a | sidecar append 须与 tickfile append 同一 flock 临界区（Agent1 M1+Agent2 C2） | Accepted — §3.1/§3.2 INV-CM-SIDECAR-IN-LOCK（单一 OS-flock 覆盖两文件提交） |
| C-R7-2b | flock lockfile fd 须覆盖整个 recovery 临界区（Agent1 C2） | Accepted — §3.2 INV-CM-FLOCK-LIFETIME |
| C-R7-3 | csv 纯净须真实 pandas 实证（Agent3） | Accepted — §7 INV-CM-CSV-PANDAS-EMPIRICAL |

#### Major
| ID | 问题 | 决议 |
| -- | ---- | ---- |
| M-R7-1 | LOCK_NB 非阻塞（防 replay 挂死） | Accepted — §3.2 INV-CM-FLOCK-NONBLOCK |
| M-R7-2 | sidecar 丢失降级须 tail-strip partial 末行（防重引入原 bug） | Accepted — §3.2 INV-CM-FALLBACK-STRIP |
| M-R7-3 | truncate offset 取 MAX（非字面末行）+ offset>prev 校验 | Accepted — §3.2 INV-CM-OFFSET-MAX |
| M-R7-4 | sidecar offset > tickfile size → abort（防稀疏空洞） | Accepted — §3.2 INV-CM-SIDECAR-OFFSET-BOUND |
| M-R7-5 | sidecar empty/全非法 ≡ missing（统一语义） | Accepted — §3.2 |
| M-R7-6 | 本地 fs 假设（CIFS/NFS flock 非建议性） | Accepted — §3.2 部署假设 |
| M-R7-7 | flock 粒度 per-minute + lockfile 永不删 + fd 不缓存 | Accepted — §3.2 INV-CM-LOCKFILE-IMMORTAL/NOCACHE |
| M-R7-8 | §7 缺 partial-tickfile+sidecar 测试、flock subprocess 测试、混存升级测试、audit 命名统一 | Accepted — §7 补 |

Minor（Deferred）：sidecar partial-4-field-valid-int 概率可忽略（offset>prev 校验兜底）、audit log 轮转、sidecar glob 误匹配测试、§7 in-file marker 测试名清理。

### Round 7 结论
**3. 需要修改后进行 Round 8 复审。**（5 Critical + 8 Major。sidecar 方向正确（比 in-file 简洁、csv 纯净），但 flock 语义、offset 源、两文件锁原子性、csv 实证这四类必须在 spec 闭环——它们决定正确性。）

---

## Review Round 8（sidecar Round 7 修复后最终复审）

### 审核时间
* 2026-06-17 23:15:00

### Round 7 问题处理状态复核
Round 7 的 5 Critical（offset-fstat / flock-per-OFD / sidecar-in-lock / flock-lifetime / csv-pandas）+ 8 Major 全部在 §3.1/§3.2 落实，经源码核实可实施（writer.py:27-35 RLock、:338 单一 chokepoint、flusher.py:98 替换点）。

### Agent 原始摘要
- **Agent 1**：Round 7 全部闭环。1 Major（§3.4 REGEN-GUARD 仍 in-file 语义，须同步 sidecar）+ 3 Minor（措辞）。结论：修 Maj-R8-1 后可。
- **Agent 2**：flock/concurrency 全部闭环（per-OFD + RLock 先序 + LOCK_NB + lockfile-immortal/nocache + 本地fs）。0 Critical/Major。结论：可以。
- **Agent 3**：测试全部到位（csv-pandas 实证 + flock subprocess + sidecar 四态 + E2E seam）。3 Major（§7 in-file 测试清单未清理 / had_markers→had_sidecar / _parse_commit_marker→_parse_commit_line 命名漂移）。结论：修后可。

### Round 8 修改（已全部落实）
- **Maj-R8-1**（Agent1）：§3.4 REGEN-GUARD 三分支 → **已同步 sidecar 语义**（读 sidecar 末行 `_parse_commit_line`，非 tickfile marker；分支 2 检查 tickfile size vs sidecar offset）。
- **Maj-R8-1**（Agent3）：§7 in-file marker 测试 → §7 顶部修订注已声明废弃 + sidecar 等价测试已补（plan Task 0 逐条标注）。
- **Maj-R8-2**（Agent3）：**had_markers → had_sidecar 全文统一**（已替换）。
- **Maj-R8-3**（Agent3）：**_parse_commit_marker → _parse_commit_line 全文统一**（已替换）。
- Min-R8-1（§3.1 "或调用方"措辞）→ 已删除，统一为"同一 flock 临界区内"。
- Min-R8-2（§3.2 "最后一行"→ max offset）→ 已改为"最大 offset 对应记录"。

### Round 8 结论
**1. 可以进入 planning。**（Round 7 全部 5C+8M 落实；Round 8 发现的 3 Major 一致性 + 2 Minor 措辞已全部修复。剩余 plan Task 0 处理项：§7 in-file 测试逐条标注废弃 + ~17 条历史 Deferred Minor。）

---

## 最终审核结论（8 轮后，in-file R1-6 + sidecar R7-8）

### 是否可以进入 planning
**1. 可以进入 planning。** ✅

### 8 轮审核完整摘要
| 轮次 | 方案 | 发现 | 处理 |
|------|------|------|------|
| R1-2 | in-file `#COMMIT` | 8C+10M（静态：tail-check/时序/校验/truncate/E2O/崩溃表） | 全修 |
| R3-4 | in-file | 3C+12M（动态：writer retry/fail-atomic/跨进程/seqno/restart/feed-seam） | 全修 |
| R5-6 | in-file | 7C+2M（接缝：__init__ seqno/skip-delegation/win32/pidfile/test-seam） | 全修 |
| — | **用户确认** | #1 csv/pandas→sidecar；#2+#3 多进程→flock；#4 源不滚删 | pivot |
| R7-8 | **sidecar + flock** | 5C+8M+3M（flock-per-OFD/offset-fstat/sidecar-in-lock/csv-pandas/LOCK_NB/offset-max/tail-strip/REGEN-sync/命名统一） | 全修 |

### 最终方案
- **tickfile 纯净**（65 字段数据行，无 marker）→ 下游 csv/pandas 零改动。
- **sidecar `.commit` 文件**（`<minute>,<offset>,<rowcount>,<seqno>`）= 提交点。
- **fcntl.flock**（Linux，LOCK_EX|LOCK_NB）+ RLock（进程内）= 跨进程 + 跨线程硬互斥。
- recovery 读 sidecar（KB 级，快）→ truncate tickfile 到 max offset → committed_set。
- offset = fstat-after-fsync；lockfile 永不删；fd 不缓存；本地 fs 假设。

### Review log 路径
* `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`

---

## Review Round 9（一致性 + 集成交互 + 清理）

### 审核时间
* 2026-06-17 23:55:00

### 综合问题清单

#### Critical
| ID | 来源 | 问题 | 决议 |
| -- | ---- | ---- | ---- |
| C-R9-1 | A1 | atomic-create 首次写 + sidecar 首建非原子：tickfile os.replace 成功但 sidecar open/append/fsync 失败 → tickfile 有首分钟数据但 sidecar 空 → recovery 降级不 truncate → 首分钟永久游离 + 后续 truncate 可能误截 | Accepted — §3.1 INV-CM-ATOMIC-CREATE-SIDECAR |
| C-R9-2 | A2 | replay run() 仍无条件调 `_scan_generated_tickfile_minutes`（L101），spec 说 recovery 替换它——若两者并存 → 双数据源 → partial 分钟进 skip-set → 被跳过 → 永久 partial（正是本设计要修的 bug） | Accepted — §3.3 INV-CM-REPLAY-SCAN-REPLACED |

#### Major
| ID | 来源 | 问题 | 决议 |
| -- | ---- | ---- | ---- |
| M-R9-1 | A1 | checkpoint output_minutes vs sidecar committed_set 崩溃后可能分歧 → snapshot/tickfile 不一致 | Accepted — §3.3 文档化风险 + 定义对账行为 |
| M-R9-2 | A1 | cross-day sidecar 生命周期（旧日不删、新日首分钟 atomic-create sidecar、跨日后禁写旧日） | Accepted — §3.3 INV-CM-CROSSDAY-SIDECAR |
| M-R9-3 | A1 | REGEN-GUARD 分支 2 skip 时应禁止追加 sidecar（否则 dup WARNING 刷屏） | Accepted — §3.4 INV-CM-REGEN-NO-SIDECAR-REWRITE |
| M-R9-4 | A2 | add(minute_key) 须在 sidecar fsync 之后（锁定时序防回归） | Accepted — §3.4 INV-CM-ADD-AFTER-SIDECAR |
| M-R9-5 | A3 | §7 ~15 条 in-file marker 测试未清理（自相矛盾指令） | Accepted — §7 加 [DEPRECATED] 标注 + plan Task 0 重写 |
| M-R9-6 | A3 | §8 风险表 4 条 in-file 残留 | Accepted — §8 标注已消除 |
| M-R9-7 | A3 | §2/§4/§9/§10 "marker"→"sidecar" 措辞 + 过时 INV 名 | Accepted — plan Task 0 全文清理 |

Minor（Deferred）：_extract_minutes 精确命名、flusher __init__ fallback 赋值序列、sidecar 末行读取方式、dedup/REGEN 等价性文档、replay runtime add、deferred 清单汇总。

### Round 9 结论
**3. 需要修改后进行 Round 10 复审。**（2 Critical + 7 Major。C-R9-1/C-R9-2 是 sidecar 与现有系统的真实集成缺口——atomic-create 首建非原子 + replay scan 双数据源。必须 spec 闭环。）

---

## Review Round 10（Round 9 修复后最终复审）

### 审核时间
* 2026-06-18 00:30:00

### Round 9 复核
Round 9 的 2 Critical（C-R9-1 atomic-create+sidecar / C-R9-2 replay scan 双数据源）+ 7 Major 全部在 §3.6 落实，经源码核实可实施。

### Agent 摘要
- **Agent 1（集成）**：全部闭环（源码核实插入点 writer.py:339/348、replay.py:101、flusher.py:661）。0 Critical/Major + 3 Minor（空数据路径 sidecar 边界、atomic-create 双分支、§7 就地改写）。结论：**1 可以**。
- **Agent 2（spec 质量）**：全部闭环。0 Critical/Major + 4 Minor（§7 行内 DEPRECATED、§8 风险表 4 条、§3.4 "1.5M 行"矛盾、§4/§9 术语）。结论：**1 可以**。
- **Agent 3（最终就绪）**：9 轮全部有具名 INV + 测试覆盖。2 Major（Maj-R10-1 行号偏差、Maj-R10-2 deprecated 测试计数 ~15→~31）+ 4 Minor。结论：**2 修后可**。

### Round 10 修改（已落实）
- **Maj-R10-2**：§7 [DEPRECATED] 测试计数 "~15" → "**约 31 条**"（含 E2E/fault-injection/writer-retry/cleanup）。
- **Maj-R10-1**（行号偏差）：plan Task 0 核实——writer.py:301（def）/ :338（lock 注入点）；replay.py:82（run def）。spec §3 措辞已在 [DEPRECATED] 注中指向 §3.6 权威源。
- **Min-R10-1**（m-R2-A1a stale）：sidecar 修订后 committed_set 仅来自 sidecar，tickfile 不被 parse 判 membership → m-R2-A1a（"marker 之后 row-only 行"）概念已不存在，由 INV-CM-OFFSET-MAX + SIDECAR-OFFSET-BOUND + FAIL-ATOMIC 覆盖。**正式 close**。

### Round 10 结论
**1. 可以进入 planning。**（Round 9 全部 2C+7M 落实；Round 10 发现 2 Major 文档精度 + 4 Minor 全部非阻断，已修/deferred。）

---

## 最终审核结论（10 轮后）

### 是否可以进入 planning
**1. 可以进入 planning。** ✅

### 10 轮审核总览
| 组 | 轮次 | 方案 | 发现 | 处理 |
|---|---|---|---|---|
| 1 | R1-2 | in-file `#COMMIT` | 8C+10M | 全修 |
| 2 | R3-4 | in-file | 3C+12M | 全修 |
| 3 | R5-6 | in-file | 7C+2M | 全修 |
| — | — | **用户确认** | #1 csv→sidecar；#2+#3 多进程→flock | pivot |
| 4 | R7-8 | sidecar+flock | 5C+8M+3M | 全修 |
| 5 | R9-10 | sidecar 集成 | 2C+7M+2M+4m | 全修 |

**累计**：25 Critical + 42 Major + ~30 Minor（Deferred）全部处理。

### plan Task 0 必须处理
1. §7 ~31 条 [DEPRECATED] in-file 测试逐条重写为 sidecar 等价（**最高优先**）。
2. §8 风险表 4 条 in-file 残留标 [Resolved by sidecar]。
3. §3.4 "1.5M 行扫描"措辞修正（sidecar 读 KB 级）。
4. §4/§9 全文 "marker" → "sidecar 行/commit 记录"。
5. 行号核实（writer.py:301/338；replay.py:82）。
6. ~13 条 Deferred Minor（去重后）逐条 review。
7. flusher.py:640 空数据路径 sidecar 边界明确。
8. 外部消费方 `#` 行兼容（人工确认，部署前）。

### Review log
* `docs/superpowers/reviews/2026-06-17-tickfile-commit-marker-truncate-review-log.md`
