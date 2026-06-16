# Tickfile Stale-Row 修复设计（shutdown 跳过 + replay 手术式补齐）

> **Date**: 2026-06-16
> **Status**: ✅ 已实施（commits a6f3c03→29439ce，7 任务 subagent-driven，459 passed / 4 预存在失败）
> **Parent / 前序**: `docs/superpowers/specs/2026-06-15-tickfile-shutdown-stale-persistence.md`（Q1 根因 + 验证）
> **类型**: 行为变更（shutdown/cross-day 不再写 stale 行）+ replay 增强（手术式 gap 补齐）

---

## 1. 背景与前序结论

Q1（详见前序 spec）：Engine 在 **order 落后 snapshot** 时停止，shutdown 的 `flush_all_remaining`
无条件为所有 `_tickfile_pending` 分钟生成 tickfile——包括 order 未到达的分钟，这些用
`latest_order_by_symbol` 的冻结 carry-forward 值，产生 **stale（陈旧）行**。前序验证证明：

- 自然 EOF 停止（order=snapshot）→ 0 stale（`CHECK 3 PASS`）
- order 落后停止 → gap 分钟 stale（`CHECK 3 WARN: 50-198 EXTRA`）
- stale 行因 tickfile append-only + 去重集不持久化 → **重启不修复，永久残留**

前序列出 3 个修复方向，本设计选定**方案 A（shutdown 跳过未到分钟）+ 配套 replay 手术式补齐**。

## 2. 关键发现：当前 replay 会腐败正确行（用户约束）

方案 A 让 gap 分钟"missing"（非 stale），需 replay 补齐。但审查 ReplayEngine 发现**当前 replay 不安全**：

- ReplayEngine 从源重流全天数据（无断点 offset），对每个分钟调 `write_tickfile_rows`（**append-only**，
  writer.py:346），**不跳过已生成分钟**（replay.py:250-263 只 `recover_tickfile_seqno` 续号）。
- 在 live 已生成 0800–1430（正确）+ 缺 1431–1530（gap）的目录上跑 replay：
  **0800–1430 被追加重复行（腐败）**，1431–1530 才被补上。

**故方案 A 必须配套让 replay 手术式补齐——只补 gap，不动正确行。**

**好消息**：tickfile 行的 `UpdateTime` 列直接由 `minute_key` 派生（tickfile.py:130），
replay 启动时**扫描 tickfile 即可精确得知哪些分钟已存在**，无需改 checkpoint schema。

## 3. 设计 Part 1：shutdown / cross-day 跳过 order 未到分钟

给两条**无条件强制生成**路径加 order watermark 门控（与 live 主路径 flusher.py:448 同源）。

### 3.1 shutdown 路径（`flush_all_remaining`, flusher.py:487-499）

当前：
```python
remaining_pending = sorted(self._state._tickfile_pending.keys())
for mk in remaining_pending:
    try:
        self._try_generate_tickfile(mk)
    except Exception:
        tickfile_errors += 1
```
改为（快照 order watermark 一次，线程已 join 无竞态）：
```python
with self._state.lock:
    remaining_pending = sorted(self._state._tickfile_pending.keys())
    order_wm = self._state.order_current_minute
generated, skipped, skipped_keys = 0, 0, []
for mk in remaining_pending:
    if order_wm and order_wm >= mk:           # order 已 flush mk → 有真实数据
        try:
            self._try_generate_tickfile(mk); generated += 1
        except Exception:
            tickfile_errors += 1; logger.exception(...)
    else:                                       # order 未到 → 跳过（否则 stale）
        skipped += 1; skipped_keys.append(mk)
if skipped:
    logger.warning(
        "Shutdown skipped %d tickfile minutes order hadn't reached (no stale rows written; "
        "fill via ReplayEngine --date=%s): %s",
        skipped, jst_now_yyyymmdd(), skipped_keys[:20],
    )
```

### 3.2 cross-day 路径（`_step1_cross_day_check`, flusher.py:225-234）

同样门控 + 日志（cross-day 较少见——需 order 跨日落后；但同一 bug，同一修复）。

### 3.3 门控条件精确选择（关键）

用 `order_current_minute >= mk`，**不是** live gate 的 `>`：
- `order_current_minute` = order 已 **flush 完成的最新分钟**（engine.py 在 `_flush_order_minute` 后更新）
- `mk <= order_current_minute` → order 已 flush mk（真实数据）→ 生成
- `mk > order_current_minute` → order 未到 → 跳过
- **为何不用 `>`**：自然 EOF 时 order=snapshot=1530，`>` 会跳过 1530（收盘！）——`>=` 才正确生成所有已 flush 分钟含最后一条
- **空 watermark**（order 从未 flush）→ `order_wm` 为空 → 全跳过（order 无数据，全跳避免全 stale）✓

## 4. 设计 Part 2：replay 手术式 gap 补齐

### 4.1 启动扫描

ReplayEngine `run()` 开始（或首次写 tickfile 前），扫描输出目录现有 tickfile，构建**已存在分钟集合**：

```python
def _scan_generated_tickfile_minutes(self, output_dir: str) -> set[str]:
    """Scan the day's tickfile; return set of minute_keys already present.
    tickfile is ONE file per day (tickfile_{date}.csv); UpdateTime col is minute_key-derived
    (tickfile.py:130), so each row's minute is recoverable."""
    from minute_bar.writer import get_tickfile_path
    sample_mk = f"{self._date}0000"          # any minute_key of the date → same per-day path
    path = get_tickfile_path(output_dir, sample_mk)
    if not os.path.exists(path):
        return set()                          # no tickfile yet → nothing generated
    return _extract_minutes_from_tickfile(path, self._date)
```
`_extract_minutes_from_tickfile`：读文件，取 `UpdateTime` 列（格式 `"YYYYMMDD HH:MM:00"`，列索引 16），
拼成 12-char minute_key（`YYYYMMDDHHMM`），distinct 入 set。大文件（~1.5M 行）离线扫描可接受，秒级。

**为何用 UpdateTime 而非 LocalTime**：
- `UpdateTime`（列 16）= **该行生成的分钟**，由 `minute_key` 派生（tickfile.py:127-131），carry-forward
  行也用正确分钟级 UpdateTime——是"分钟 mk 已生成"的**权威标记**。
- `LocalTime`（列 60）= 数据**真实时间戳**（`parse_17digit_time(primary_time)`），sub-minute、每行不同、
  carry-forward 行是旧值——**不能当分钟标记**。
故扫描用 UpdateTime。

### 4.2 回放时跳过已存在分钟

在 replay 的每分钟 tickfile 生成处（replay.py ~250-263）加守卫：
```python
if self._enable_tickfile:
    if minute_key in self._generated_tickfile_minutes:   # 已存在 → 跳过，不追加
        logger.debug("Replay skip already-generated tickfile minute=%s", minute_key)
    else:
        ... existing select + write_tickfile_rows (append, fills gap) ...
        self._generated_tickfile_minutes.add(minute_key)
```
seqno 用现有 `recover_tickfile_seqno` 续接（从文件最大 seqno+1），无碰撞。

### 4.3 效果

- live 已正确的分钟 → 在扫描集合中 → replay 跳过 → **不动正确行** ✓
- gap 分钟（missing）→ 不在集合 → replay 生成 → 补齐 ✓
- 无重复、无腐败 → **断点重续 + 不影响 live 正确记录** ✓
- **纯 replay（无 live tickfile）不受影响**：`os.path.exists(path)` False → 扫描返回空集合 →
  replay 生成所有分钟（无一跳过），**行为与当前 replay 完全一致**（全量生成），开销仅一次 exists 检查。
  "跳过"只在 tickfile 文件已存在时触发——gap 补齐场景才生效。

### 4.4 为何扫描而非持久化 `_generated_tickfile_minutes`

- 扫描反映**文件真实状态**（权威）：即便 live 上次 checkpoint 后崩溃，扫描仍看到崩溃前已写入的行
- 持久化有"上次 checkpoint 后崩溃"的集合缺口 → 可能少量重复
- 扫描自包含，**无 checkpoint schema 改动**

## 5. 行为变化

| 场景 | 之前 | 之后 |
|------|------|------|
| 自然 EOF 停止（order=snapshot） | 全生成，0 stale | 不变（所有分钟 `order_wm>=mk`，全生成）✓ |
| order 落后停止 | gap 分钟 **stale-wrong**（永久残留） | gap 分钟 **missing**（可检测）+ 日志 WARNING |
| 跑 replay 补 gap | **追加重复行腐败正确行** | 手术式只补 gap，正确行不动 ✓ |
| live 崩溃重启 | gap stale 永久残留 | gap missing，replay 补齐 |

## 6. INV-TF1 协调

INV-TF1「shutdown/cross-day 不丢已生产数据」本意是**已生产的数据要写出**。本设计跳过的是
**无 order 数据的伪造行**（carry-forward 冻结值），非真实数据。被跳过分钟的 **snapshot 真实数据
仍在 snapshot 输出文件中**；只是 tickfile 行省略，由 ReplayEngine 从源重放补齐（用真实 order 数据）。
故 INV-TF1 本意得到尊重——更新文档明确"伪造行不算已生产数据"。

## 7. 测试计划（TDD）

### 7.1 Part 1（shutdown/cross-day 跳过）
- `test_shutdown_skips_unreached_tickfile_minutes`：order_current_minute=1430，
  `_tickfile_pending`={1429,1500}，调 `flush_all_remaining` → 断言 1429 生成、1500 跳过、日志含 1500
- `test_shutdown_generates_all_on_natural_eof`：order_current_minute=1530，
  pending 含 1530 → 断言 1530 **生成**（验证 `>=` 非 `>`，不漏收盘）
- `test_shutdown_skips_all_when_order_never_flushed`：order_wm="" → 全跳过
- `test_cross_day_skips_unreached`：cross-day 变体

### 7.2 Part 2（replay 手术式补齐）
- `test_replay_skips_already_generated_minutes`：预写 tickfile 含 0900/0901 行 → replay → 断言
  0900/0901 不被重复追加（行数不变）、seqno 续接
- `test_replay_fills_missing_gap_minutes`：tickfile 缺 1000 → replay → 断言 1000 被生成补齐
- `test_replay_scan_extracts_minute_from_updatetime`：扫描单测（UpdateTime → minute_key）

### 7.3 回归
- 现有 shutdown / cross-day 测试更新期望（不再无条件生成）；自然 EOF 全生成
- replay 现有测试（test_replay.py）通过

## 8. 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| 扫描大 tickfile 慢 | 低 | 低 | 离线 replay，秒级可接受；可优化（只读 UpdateTime 列/distinct） |
| UpdateTime 解析脆弱 | 低 | 中 | 格式固定（tickfile.py:130 派生）；扫描单测覆盖 |
| cross-day 跳过导致跨日 gap 永久 missing | 低 | 低 | 跨日 order 落后罕见；replay 补齐 |
| 现有 shutdown 测试假设无条件生成 | 高 | 低 | 7.3 更新期望（预期行为变更） |

## 9. 范围外

- 不持久化 `_generated_tickfile_minutes`（扫描替代，§4.4）
- 不改 live gate（flusher.py:448 已正确）
- 不改 tickfile schema（无 flag 列）
- 不清理历史重复行（**假设无历史腐败行**——本修复前向生效；若历史确有重复，由全量 replay-to-fresh-dir 处理）

## 10. 相关文档

- `[[tickfile-shutdown-forcegen-orderless]]` — Q1 根因 + 全天验证
- 前序 spec `2026-06-15-tickfile-shutdown-stale-persistence.md`
- `[[minute-key-round-up]]` — 同期 minute_key 语义变更（独立）
