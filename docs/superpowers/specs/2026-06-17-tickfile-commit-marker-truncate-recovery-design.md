# Tickfile Commit-Marker + Truncate 恢复设计（mid-append 崩溃恢复）

> **Date**: 2026-06-17
> **Status**: ✅ 18 轮 review 完成（9 组 × 2 轮），sidecar + fcntl.flock + 跨输出对账 + 篡改防护 + cascade 防护 + 部署 runbook 方案可进入 planning。
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

## 3. 设计：sidecar commit 文件作为"提交点"（用户确认后修订）

> **⚠ 修订（6 轮 review 后，用户确认下游 csv/pandas 兼容 + 多进程）**：原 in-tickfile `#COMMIT` marker **破坏 csv 格式**（pandas 不跳 `#`、csv.reader 字段错位），已废弃。改为 **sidecar commit 文件**（tickfile 保持纯净）+ **fcntl.flock 跨进程锁**（live+replay 真并发）。本节（§3.1/§3.2）以此修订为准；§5 tail-check、§6 rollback 因此**大幅简化**（C1/C5 自动消除）。

### 3.1 Sidecar 写入（tickfile 纯净 + 边车提交记录）

**tickfile 保持纯净**——只有数据行（每 symbol 每分钟 1 行，65 字段），**无 marker 行**。下游 csv/pandas 零改动。

**提交点放在 sidecar 文件** `tickfile_{date}.csv.commit`（与 tickfile 同目录，下游不读）：
```
202605280931,1234567,4505,331
202605280932,2345678,4505,332
...
```
格式 `<minute_key>,<tickfile_byte_offset>,<rowcount>,<seqno>`（每行 = 一个已 commit 分钟）：
- `minute_key`：该分钟。
- `tickfile_byte_offset`：该分钟最后数据行写入后 tickfile 的字节大小（= commit 边界，truncate 锚点）。
- `rowcount`：实际写入 rows 数（`len(rows)`，诊断）。
- `seqno`：该分钟 seqno（recovery 直接取，免扫 tickfile）。

**写一个分钟（提交）的顺序**（INV-CM-ORDERED-TWO-FILE，取代原 INV-CM-BATCH）：
1. 向 `tickfile_{date}.csv` append 该分钟 ~4505 数据行 → flush + fsync。
2. 记录此刻 tickfile 字节大小 = `offset`。
3. 向 `tickfile_{date}.csv.commit` append `<minute>,<offset>,<rowcount>,<seqno>` → flush + fsync。
4. **sidecar 这行的 fsync = 提交点**（tickfile rows 已先 fsync）。

- **单一 chokepoint**：`write_tickfile_rows`（live `_try_generate_tickfile` + replay `_flush_snapshot_minute` 公共入口）在**同一 flock 临界区内**完成步骤 1-3（tickfile append + fstat offset + sidecar append）。**禁止**"调用方在 write 返回后锁外追加 sidecar"（见 INV-CM-SIDECAR-IN-LOCK）。
- **关键不变量 INV-CM-MONO（沿用）**：recovery 只依赖单调性——**sidecar 行落盘 ⟺ 该分钟 tickfile 数据完整落盘**（因步骤 1 fsync 先于步骤 3）；sidecar 行未落盘 → tickfile 该分钟数据视为未 commit → truncate。
- **INV-CM-ORDERED-TWO-FILE**：tickfile rows 的 append+fsync 必须**严格先于** sidecar 行的 append+fsync。禁止逆序（否则 sidecar 记录了未落盘的 offset）。
- **INV-CM-OFFSET-FSTAT**（C-R7-1a）：`offset` 必须在 `os.fsync(f.fileno())` 返回后**立即从 `os.fstat(f.fileno()).st_size` 读取**（同一 fd，非 `os.path.getsize` path-stat／非 `f.tell()`／非预计算 `len(content)`）。理由：NFS/网络 fs path-stat 可能返回 stale inode size；tail-newline-fix 会加未计字节 → 预算偏移漂移 → truncate 越界（截断过短破坏已 commit 分钟）。
- **INV-CM-SIDECAR-IN-LOCK**（C-R7-2a）：tickfile append（步骤1）+ offset 读取（步骤2）+ sidecar append（步骤3）必须在**同一 OS-flock 临界区**内（`with _get_write_lock(path)` + `flock(lockfile_fd)`）。**禁止**"write_tickfile_rows 写 tickfile 后由调用方在锁外追加 sidecar"（原"或调用方"漏洞）——否则并发 recovery truncate 与 sidecar append 交错 → tickfile/sidecar 不一致。即两文件提交是**单一原子临界区**。

### 3.2 恢复函数（共享）：`_recover_tickfile_to_last_commit(output_dir, date)`

新增于 writer.py（与 `recover_tickfile_seqno` 并列）。**读 sidecar（几 KB，毫秒级），不扫 tickfile 1.5M 行**。

**Path 构造**：tickfile `get_tickfile_path(output_dir, f"{date}0000")`；sidecar = `tickfile_path + ".commit"`。

**Sidecar 行校验** `_parse_commit_line(line) -> Optional[tuple]`（返回 `(minute, offset, rowcount, seqno)` 或 None）：split(",") 后恰好 4 字段；minute 12 位数字；offset/rowcount/seqno 非负整数。任一不满足 → 该行非法（partial 末行 / 损坏）→ 跳过。

**读 sidecar + truncate（在 `flock` + 进程内 `_get_write_lock` 内，INV-CM-LOCK）**：
- 读 sidecar 全部合法行 → 取**最大 offset 对应记录**（INV-CM-OFFSET-MAX，非字面末行）的 `offset` = truncate 锚点；`committed_set` = 所有合法行的 minute（**INV-CM-DATE-FILTER**：只收 `minute.startswith(date)`；跨日 WARNING 不纳入）。
- 若 tickfile 当前 size > `offset`（有未提交 partial 分钟）→ **truncate tickfile 到 `offset`**（INV-CM-FAIL-ATOMIC：读 sidecar 在 try 内，仅成功才 truncate）。sidecar 自身**不 truncate**（partial 末行由校验跳过，下次 append 续写）。
- **备份（C4/M-R3-3）**：truncate 前，被丢弃 `[offset, old_size)` 字节复制到 `tickfile_{date}.csv.truncated.{time_ns()}.{pid}`；**备份 IO 失败 → 不 truncate、CRITICAL、abort**（文件留 partial，降级，committed_set 仅含 sidecar 已记录分钟）。
- 返回 `(committed_set, last_seqno, had_sidecar)`：`last_seqno` = sidecar 最后一行 seqno（**M-R3-4**：免扫 tickfile；`recover_tickfile_seqno` 改薄包装/废弃）。覆盖 `_tickfile_seqno` 取 `max(file_last_seqno, 当前)`（**INV-CM-SEQNO-MONO-FILE**，M-R5-2）。

**Sidecar 丢失/降级（had_sidecar）**：
- sidecar 存在 → `had_sidecar=True`，committed_set 来自 sidecar，truncate 到最后 commit offset。
- sidecar **不存在**但 tickfile 存在（老文件/无 sidecar，或 sidecar 误删）→ `had_sidecar=False`，`committed_set=None` → 调用方降级 row-based `_extract_minutes`（**不 truncate**，保守）。记 WARNING `sidecar_missing_fallback`。可选：replay-to-fresh 重建 sidecar。
- tickfile 不存在 → `had_sidecar=False`，`committed_set=set()`，不 truncate（纯首跑）。

**一致性校验（M4）**：sidecar 中 minute 非严格递增 / dup → WARNING（跨日错位/重复写信号），**不阻断**。

**可观测性（M5/M-R3-8）**：recovery 完成后结构化 log + 持久 audit log `output_dir/tickfile/tickfile_recovery.log`（JSON 行 `{ts,date,pid,hostname,had_sidecar,committed_count,last_commit_minute,truncate_bytes,result}`，**INV-CM-AUDIT-BESTEFFORT** try/except 不阻断）。metric：`tickfile_recovery_truncate_bytes`/`_committed_minutes`/`_had_sidecar`/`_invocations`。

**跨进程锁（用户确认 #2+#3，Linux fcntl.flock）+ offset/降级闭环（Round 7）**：
- **INV-CM-FLOCK**（C-R7-1b 修正语义）：`fcntl.flock` 关联 **open-file-description（OFD）**，**非进程**。同进程线程互斥**仍须** `_get_write_lock`（RLock）保证；flock **仅**提供跨进程互斥。**acquisition 顺序严格**：先 `_get_write_lock(path)`（线程互斥），在其内 `open(lockfile)` → `flock(fd, LOCK_EX|LOCK_NB)`（进程互斥）。禁止逆序（否则同进程 flock-fd 冲突）。**禁止**误信"flock 同进程 no-op"而跳过 RLock。
- **INV-CM-FLOCK-NONBLOCK**（M-R7-1）：`LOCK_EX | LOCK_NB`（非阻塞）；`BlockingIOError`(EWOULDBLOCK) → 记 `flock_held_by_other_process` + abort（与 replay-guard 同退出）。**禁止阻塞式 flock**（recovery/health-check 热路径不能挂死——live 跨交易时段持锁，阻塞式 replay 会挂数小时）。
- **INV-CM-FLOCK-LIFETIME**（C-R7-2b）：lockfile fd 在 `with`/`try-finally` 内**覆盖整个 recovery 临界区**（read sidecar + truncate tickfile + sidecar append + audit log），一个 fd 全程，finally 释放。Windows `msvcrt.locking(fd, LK_NBLCK, 1)` 同理（fd 存活期=临界区）。
- **INV-CM-LOCKFILE-IMMORTAL**（M-R7-7）：lockfile = `tickfile_{date}.csv.lock`，首次写时 `open("a")` 创建，**永不删除**（跨日/关闭/崩溃均不删——删除致 inode 竞争：进程 A 持 inode-1 flock，进程 B 建 inode-2 → 无互斥）。跨日自然用新 `{date}` 名，旧 lockfile 成孤儿（无害，小文件）。
- **INV-CM-FLOCK-NOCACHE**（M-R7-7）：flock fd **不缓存/不重用**——每次临界区 `open(lockfile)` → flock → 工作 → 释放（with 关 fd）。兼容 `_prune_write_locks` 跨日清理。
- **部署假设（M-R7-6）**：output_dir 须**本地 fs**（ext4/xfs）；**不支持 CIFS/NFS**（CIFS≥5.5 flock 变强制锁致 IO EACCES；NFS 跨主机 flock 语义问题）。
- replay guard（pidfile O_EXCL + pid liveness）保留为**额外防线**，flock 是底层硬保证。

**offset 取值 + 降级闭环（Round 7）**：
- **INV-CM-OFFSET-MAX**（M-R7-3）：truncate offset = sidecar 所有合法记录中**最大的 offset**（非字面末行——防乱序/重复记录选错截断点）；`last_seqno` = 该最大-offset 记录的 seqno。
- **INV-CM-OFFSET-MONO**：`_parse_commit_line` 校验每条记录 offset > 上一条有效 offset；违反 → 跳过该记录 + WARNING（关闭 partial-4-field-valid-int 漏洞）。
- **INV-CM-SIDECAR-OFFSET-BOUND**（M-R7-4）：truncate 前 `if max_offset > tickfile_current_size → 不 truncate、CRITICAL、降级`（防 `os.truncate` 扩展致稀疏零字节空洞 → pandas NaN）。
- **INV-CM-SIDECAR-EMPTY-EQUIV-MISSING**（M-R7-5）：sidecar 存在但**无合法行**（空/全非法）≡ 不存在 → `had_sidecar=False` 降级。
- **INV-CM-FALLBACK-STRIP**（M-R7-2）：sidecar 丢失降级路径（had_sidecar=False）**必须**运行 tail-strip：若 tickfile 末行非完整 65 字段（partial mid-append）→ 截断到最后一个 `\n` 边界（剥离 partial 行）+ CRITICAL。防 partial 永久残留重引入原 bug。

### 3.3 调用方 + 时序不变量（C2）

**ReplayEngine.run() 启动**：调 `_recover_tickfile_to_last_commit`：
- `had_sidecar=True` → `self._generated_tickfile_minutes = committed_set`（marker 模式，替换原 `_extract_minutes` 扫描）。
- `had_sidecar=False` → 降级 `_extract_minutes`（老文件兼容）。
- 之后 gap-fill 跳过 committed 分钟、生成未 committed 的（truncate 已删掉的部分分钟 + gap 分钟）。

**Engine live 启动**（崩溃重启）：调同一函数 → truncate 上次崩溃留下的部分分钟 → live 从 checkpoint 恢复重处理该分钟（live feed 仍有数据则重生成 + 打 marker；无数据则缺失，由 replay 补——与 gap 一致）。

**调用时序不变量（C2，生产正确性）**——必须在 spec 闭环，不能推到 plan：
- **INV-CM-ORDER-1**（M-R5-1 修正插入点）：`_recover_tickfile_to_last_commit` 必须在 **`ClockWatermarkFlusher.__init__` 的 seqno 获取点（flusher.py:98 `recover_tickfile_seqno_lazy`）执行**——而非 `start()`。因 `__init__` 在 `Engine.__init__`（构造期）就**急切**取 seqno，早于 `start()`；若 recovery 放 start()，`__init__` 已从 partial 文件取到脏 seqno。**推荐**：用 `_recover_tickfile_to_last_commit`（返回 3-tuple 含 last_seqno）**替换** flusher.py:98 的 `recover_tickfile_seqno_lazy`，使 recovery 成为 seqno 唯一首步（同调用点，无时序窗口）。recovery 仍须早于 `_tickfile_writer_thread.start()`（start() 内）。
- **INV-CM-ORDER-2**：recovery（含 truncate）必须在所有 seqno 读取入口之前完成（由 INV-CM-ORDER-1 在 `__init__` 替换覆盖 + recovery 返回 last_seqno 消除 lazy 入口）。否则 seqno 取到被 truncate 分钟 → 倒退/跳号。
- **INV-CM-SEQNO-MONO-FILE**（M-R5-2）：recovery 的 `last_seqno` 覆盖 `_tickfile_seqno` 时**取 `max(file_last_seqno, 当前内存 seqno)`，绝不倒退**。原因：REGEN-GUARD 分支 2 skip 消费 seqno 但无行落盘 → 文件 last_seqno 可能 < 内存已消费值；直接覆盖会倒退 → 下次写重用 seqno（不同内容同 seqno → 下游去重误判）。取 max 保单调。
- **INV-CM-SKIPSET-LIVE**：recovery 返回的 `committed_set` 必须写入 live 引擎的 `self._state._generated_tickfile_minutes`（`aggregator.SharedState` 字段，flusher 读写同一 set）。否则 live writer 会重生已 commit 分钟 → 重复行。
- **INV-CM-SKIPSET-REPLAY**：replay 路径写入 ReplayEngine 自身的 `self._generated_tickfile_minutes`（replay.py 字段，**与 live 的 SharedState 字段是两个独立字段**）。两条路径各自必须设对应字段。
- **INV-CM-LOCK**：recovery 的 truncate 持 `_get_write_lock(path)`；writer 线程也持同一 lock → 互斥安全（且因 INV-CM-ORDER-1 时序，实际无并发）。
- **INV-CM-ORDER-RESUME**（cross-day resume）：`_tickfile_writer_resume()`（cross-day reset 后重建 writer 线程）**无需**调 recovery——因 cross-day 已 `_generated_tickfile_minutes.clear()` 且目标为**新 date 的 fresh 文件**（不存在/仅 header，无 partial）。若未来引入 same-day resume（同 date 文件续写），必须在该路径补 recovery + 时序同 INV-CM-ORDER-1。
- **INV-CM-ORDER-2**（扩展，M-R3-1 + M-R4-1b）：recovery 必须严格早于**所有** seqno 读取入口——含 `ClockWatermarkFlusher.__init__` 的 `recover_tickfile_seqno_lazy`（engine 构造时即取）+ `start()` 的 `_recover_tickfile_seqno` + **replay `_flush_snapshot_minute` 的 lazy seqno**（replay.py，第三入口）。推荐：recovery 返回的 `last_seqno` 直接覆盖 `self._tickfile_seqno`（消除三个 lazy 入口，recovery 成唯一首步）——最干净。

### 3.4 运行时正确性 / 失败安全 / 并发（Round 3 增补）

前两轮聚焦 `Engine.start()` 静态启动路径；Round 3 揭示**生产运行时**的三类风险，必须 spec 闭环：

**C-R3-1 — writer 线程 retry / health-check 重启 / resume 路径须自愈（防重生重复）**
- **场景（sidecar 语义）**：`_try_generate_tickfile` 在 tickfile rows append+fsync 后、**sidecar 行 append/fsync 前**抛 IOError（disk-full）→ tickfile 有该分钟 rows（已 fsync）但 sidecar 无记录 → recovery 视为未 commit → truncate 掉已 fsync 的 rows → writer loop retry re-insert → 重写。若 retry 又在 sidecar 写前崩 → 循环。`_tickfile_writer_health_check`（engine.py:1458）自动重启同样需 recovery。
- **INV-CM-REGEN-GUARD**（sidecar 修订）：`_try_generate_tickfile` 在**同一 flock 临界区内**（INV-CM-SIDECAR-IN-LOCK）append tickfile rows + 写 sidecar 行。precondition 携带 `current_minute_key`，读 **sidecar 末行**（`_parse_commit_line`，非 tickfile marker）分支处理：
  1. sidecar 末行 minute < `current_minute_key`（前一分钟已 commit）→ 正常 append tickfile + sidecar。
  2. sidecar 末行 minute == `current_minute_key`（**M-R4-1a**：tickfile rows + sidecar 都已 fsync 的 retry，或 tickfile 已 fsync 但 sidecar 未 fsync）→ 检查 tickfile size 是否 == sidecar 记录的 offset：是 → 已完整 commit → skip + 标 committed；否（sidecar 未记录当前，tickfile 有 partial/完整 rows）→ truncate tickfile 到 sidecar 最后 offset → 正常重写。
  3. sidecar 不存在/空 → 首次写入，正常 append tickfile + 建 sidecar。
  - 谓词复用 `_parse_commit_line`（sidecar 行解析）。使**每次 append 自愈**，覆盖 fsync 失败 retry。
- **INV-CM-ORDER-RESTART**：`_tickfile_writer_health_check` 与 same-day `_tickfile_writer_resume` 重启 writer 线程前，必须先调 `_recover_tickfile_to_last_commit`（同 INV-CM-ORDER-1 语义）。
- 测试：`test_writer_ioerror_retry_truncates_partial_no_duplicate`（sidecar 语义：tickfile rows fsync 后 sidecar 写前 IOError → retry → truncate 到 sidecar offset + 重写，无重复）、`test_writer_health_check_calls_recovery_before_restart`。

**C-R3-2 — recovery 自身必须 fail-atomic（防扫描中途异常误 truncate）**
- **场景**：`_recover_tickfile_to_last_commit` 扫描 1.5M 行中途 OOM/IO 错误/编码异常。若在部分计算的偏移上 `os.truncate` → 切掉完整分钟 → 永久丢失。recovery 比不 recovery 更危险。
- **INV-CM-FAIL-ATOMIC**：整个 recovery "全有或全无"——**扫描（只读）阶段**在 try 内计算 `(new_size, committed_set, last_seqno)`；**仅当扫描无异常返回合法元组**才在锁内 truncate。扫描异常 → 记 CRITICAL、**文件原样不动（不 truncate、不备份）**、降级 row-based presence、re-raise 让上游决策。绝不"半截 truncate"。
- 测试：`test_recovery_scan_io_error_aborts_without_truncate`（monkeypatch 文件迭代中途抛 OSError → 断言文件字节与崩溃前完全一致、无 `.truncated` 备份）。

**C-R3-3 — 跨进程并发：进程内 RLock 不互斥**
- **场景**：`_get_write_lock`（writer.py:31-35）是**进程内** `threading.RLock`，**不跨进程**。live 进程 append + replay 进程 recovery truncate 同一 output_dir → 截掉 live 刚 commit 的合法分钟 → 丢数据 + 内存/磁盘不一致。HA/灰度/live+replay 补数均触发。
- **INV-CM-SINGLEPROC**（部署假设，必须显式声明）：**tickfile 目录在任意时刻仅被一个引擎进程（live XOR replay）写入**。
- **replay guard**（M-R5-4 强化）：`ReplayEngine.run()` 启动时用 **原子创建作锁**防 TOCTOU + stale 死锁：`os.open(pidfile, O_CREAT|O_EXCL|O_WRONLY)`（EXCL 内核原子，无 check-then-write 窗口），写 `{pid},{start_time_ns}`。若文件已存在：查 pid liveness（POSIX `os.kill(pid,0)` / win32 `ctypes.OpenProcess+GetExitCodeProcess`）——pid 死 → stale pidfile → reclaim（EXCL 重用，记 WARNING `stale_pidfile_reclaimed`）；pid 活 → live 真在运行 → **abort with error**。
- **atomic-create 跨进程 rename 覆盖（M-R3-10，既有 bug）**：两进程同日首分钟 atomic-create 同名 `.tmp` → 覆盖丢数据。同样由 INV-CM-SINGLEPROC 声明覆盖；根治（`.tmp` 加 pid/uuid + replace 前 stat）为 future（Deferred）。
- 测试：`test_replay_guard_atomic_excl_rejects_concurrent_start`（两线程并发 EXCL open，只一成功）、`test_replay_guard_reclaims_stale_pidfile_after_live_crash`（stale pidfile + pid 死 → replay 成功 + WARNING）、`test_replay_guard_aborts_when_live_pid_alive`。**future**：若需多进程，用 OS 建议锁（`fcntl.flock`/`msvcrt.locking`）包 truncate+append 段——Deferred。

**M-R3-2 — `_cleanup_tickfile_tmp_files` 顺序 + .tmp marker**
- **INV-CM-CLEANUP-ORDER**：`_cleanup_tickfile_tmp_files`（engine.py:344）必须在 recovery **之前**执行（先回收 atomic-create 残留 .tmp，recovery 再统一 truncate）；其 `.tmp` 合法性校验扩展为"header 合法 **且** 末行合法 65-field 数据行（与 §5 一致）"才 `os.replace`，否则**删除该 .tmp**（让其重生）。因 atomic-create 的 .tmp 经 write_tickfile_rows 应含合法 65-field 数据行；若不含说明是旧代码残留或异常 → 删。

**M-R3-8 — 持久 audit log（metric 不跨崩溃存活）**
- recovery 完成后追加一行 JSON 到 `output_dir/tickfile/tickfile_recovery.log`。schema：`{ts, date, pid, hostname, had_sidecar, committed_count, last_commit_minute, truncate_bytes, result: truncate/noop/fallback/error}`（**M-R5-6**：加 pid/hostname 便多机取证）。**跨崩溃存活**，支持"重启循环检测"。
- **INV-CM-AUDIT-BESTEFFORT**（M-R5-6）：audit log 写**必须** try/except 包裹、best-effort、**绝不 raise / 不阻断 recovery**（磁盘满正是触发 recovery 的场景，audit 可丢，recovery 本身有进程内 metric + logger 兜底）。写前 `os.makedirs(dirname, exist_ok=True)`。单行 JSON < 4KB，`open("a")`（设 O_APPEND）单次 write 原子（POSIX PIPE_BUF 4KB 内 / win32 共享默认）。
- 测试：`test_recovery_writes_persistent_audit_log`、`test_recovery_audit_log_failure_does_not_abort`（monkeypatch audit open 抛 OSError → recovery 仍完成 truncate + metric 记录）。

### 3.5 Round 5 增补：新逻辑与现有 state 的接缝

**M-R5-3 — 分支 2 skip 职责切分（跨模块）**
- **INV-CM-SKIP-DELEGATION**：REGEN-GUARD 分支 2 的 "skip" 与 "add committed" 分离——`write_tickfile_rows`（writer.py 模块函数，持文件锁，**无 SharedState 访问权**）内分支 2 仅做 **file-level skip**（零字节 return，不写文件）；"add committed" 由 `_try_generate_tickfile` 成功路径（flusher.py:660-661）**统一**承担（分支 2 skip 也走此路径，与真实 append 共用同一 add 点，不重复、不跨锁）。
- 测试：`test_regen_guard_branch2_skip_delegates_add_to_flusher`（分支 2 零字节 return → flusher 仍 add committed + 文件无新行 + pending/order_buffers 已 pop）。

**M-R5-5 — Windows：REGEN-GUARD 分支 1 / recovery truncate 与 append fd 顺序**
- **INV-CM-TRUNCATE-BEFORE-OPEN**（win32 平台正确性）：truncate **必须**用 `os.truncate(path, new_size)`（path-based，独立短暂 fd）在 `_get_write_lock(path)` 内、**`with open(path,"a")` context 开始之前**执行。**禁止**用 append fd 的 `f.truncate()`（win32 上 append-fd truncate 后 fd 位置仍在旧 EOF → 下次 write 在新 EOF 与旧位置间留**稀疏空洞/零字节**）。POSIX append 模式每次 write seek 到 EOF，但 Python 文本模式 `"a"` open 时不保证定位 → truncate-before-open 是可移植安全模式。
- recovery（§3.2）的 truncate 同样 path-based（`os.truncate(path)`），不重用 reader fd。
- 若 `os.truncate` 在 win32 抛 sharing violation → 视为扫描异常 → INV-CM-FAIL-ATOMIC → 不 truncate。
- 测试：`test_regen_guard_truncate_before_append_fd_open_no_sparse_gap`（truncate 与 open fd 间插桩，断言无零字节间隙）、`test_recovery_truncate_sharing_violation_aborts_clean`（monkeypatch os.truncate 抛 PermissionError → 文件不变、无备份、re-raise）。

**M-R5-7 — writer restart 频率上限 + 永久死亡 metric + E2E feed seam**
- **restart 上限文档**：`_tickfile_writer_restart_count` 硬上限 = 1（engine.py:1464 现状）→ 每进程最多 2 次 recovery 全扫（启动 1 + health-check 1）后 writer **永久死亡**（CRITICAL 日志）+ 进程级监控告警。加 metric `tickfile_writer_perm_dead`（0/1）。health-check 路径的 recovery 用 REGEN-GUARD per-append precondition（只扫末尾 4096，不扫全文件）——因 REGEN-GUARD 已保证每次 append 自愈，health-check restart 无需全量 recovery（避免 tick 循环阻塞）。
- **E2E feed seam**（M-R5-7/A3）：`test_e2e_live_restart_recovers_partial_minute` 用**种子 csv_dir**（写 snapshot.csv+code.csv，复用 stale-fix 模式）+ 构造 `Engine` + `.start()` + **轮询 `engine._tickfile_dequeue_count`**（无锁引擎计数器，测试可读）直到处理完 0902 → `.stop()`。不新增生产 test-only 注入点（零侵入）。spec 明确此 seam。

### 3.6 Round 9 集成交互增补（sidecar 与现有系统接缝）

**C-R9-1 — atomic-create 首次写 + sidecar 首建必须原子**
- **INV-CM-ATOMIC-CREATE-SIDECAR**：`write_tickfile_rows` 的 atomic-create 分支（`not os.path.exists(path)` / header-rewrite empty）在**同一 flock 临界区内**、`os.replace(tmp, path)` 成功后，立即 `fstat` 取 offset + append sidecar 行 + fsync。即 atomic-create 的 `os.replace` = tickfile 诞生点，但**提交点仍是 sidecar 行 fsync**（replace 前 tmp 的 fsync 满足 INV-CM-ORDERED-TWO-FILE 先序）。若 replace 后 sidecar 创建失败 → 下次 recovery `had_sidecar=False`（INV-CM-SIDECAR-EMPTY-EQUIV-MISSING）→ **不是保守保留 tickfile**，而是 tickfile 有新数据但 sidecar 空 = 创建非原子残留 = **truncate tickfile 到 header-only（0 数据行）+ 重生首分钟 + 建 sidecar**。测试 `test_atomic_create_sidecar_fail_truncates_to_header`。

**C-R9-2 — replay run() 必须用 recovery 替换 `_scan`，禁止双数据源**
- **INV-CM-REPLAY-SCAN-REPLACED**：`ReplayEngine.run()` 中对 `_scan_generated_tickfile_minutes`（replay.py:101）的调用必须**替换为** `_recover_tickfile_to_last_commit`：`had_sidecar=True → _generated_tickfile_minutes = committed_set`（sidecar 权威）；`had_sidecar=False → _generated_tickfile_minutes = _scan_generated_tickfile_minutes(...)`（降级）。**严禁两者并存**——`_scan`（基于 tickfile UpdateTime）会拾取 partial 未 commit 行 → skip-set 污染 → 正是本设计要修的 bug。`_scan` 仅作为 `had_sidecar=False` 降级路径（legacy 兼容）保留。测试 `test_replay_uses_sidecar_recovery_not_scan`。

**M-R9-1 — checkpoint vs sidecar 分歧（文档化 + 对账）**
- 正常流程：snapshot flush → checkpoint → tickfile generate → sidecar → 故 sidecar committed ⊆ checkpoint output_minutes（tickfile 后于 snapshot）。但 crash 在 checkpoint 周期窗口内 → 可能分歧。
- **INV-CM-CHECKPOINT-RECONCILE**：recovery 完成后对 `committed_set`（sidecar）与 checkpoint `output_minutes` 对账：`snapshot_only = output_minutes − committed_set`（snapshot 有 tickfile 缺）→ CRITICAL + **注入 gap 补齐**（replay 或 live 重跑这些分钟）；`tickfile_only`（tickfile 有 snapshot 缺）→ CRITICAL WARNING（理论上不应出现，checkpoint/tickfile 顺序 bug 信号）。测试 `test_checkpoint_reconcile_snapshot_only_injects_gap`。

**M-R9-2 — cross-day sidecar 生命周期**
- **INV-CM-CROSSDAY-SIDECAR**：(a) 旧日 sidecar/lockfile 跨日**不删**（保留供历史 replay/recovery）；(b) 跨日 force-gen 新日首分钟若走 atomic-create，必须满足 INV-CM-ATOMIC-CREATE-SIDECAR；(c) 同进程跨日后**禁止再写旧日** tickfile/sidecar（`_write_locks` 条目已 prune，再写建新 RLock 破坏互斥）。

**M-R9-3 — REGEN-GUARD 分支 2 禁止追加 sidecar**
- **INV-CM-REGEN-NO-SIDECAR-REWRITE**：REGEN-GUARD 分支 2（sidecar 末行 == current，已 commit）skip 时**只**零字节 return + flusher add committed（INV-CM-SKIP-DELEGATION），**禁止 append sidecar 行**（该分钟 sidecar 已有记录，重写 = dup → WARNING 刷屏）。sidecar append 只发生在分支 1（新分钟）+ 分支 3（首建）。

**M-R9-4 — add(minute_key) 须在 sidecar fsync 之后**
- **INV-CM-ADD-AFTER-SIDECAR**：`_state._generated_tickfile_minutes.add(minute_key)`（flusher.py:660）必须在 `write_tickfile_rows` 返回后执行，且 `write_tickfile_rows` 内 sidecar append+fsync 在 `add` 之前完成（INV-CM-SIDECAR-IN-LOCK）。禁止 `add` 在 sidecar fsync 之前（否则 restart 时内存 set 有但 sidecar 无 → 当 gap 重生 → 重复）。**当前代码已遵守**（add 在 write 返回后），此 INV 锁定时序防回归。

### 3.7 Round 11 运维 / 性能 / GIL / 部署增补

前 10 轮聚焦正确性/IO/并发；Round 11 从**性能影响、Rust 去 GIL 集成、部署/升级工作流**三个全新角度审查，发现以下运维层面风险。

**C-R11-1 — 首次部署须引擎停止状态**
- **INV-CM-DEPLOY-STOPPED**：commit-marker 代码首次部署必须 **stop → upgrade → start**（绝不能 live 运行时 rsync）。`fiu-minute-bar.service`（Restart=on-failure）的 systemd 路径安全（restart = 完全停止后重启）。但手动 `setup.sh`/rsync 必须先 `systemctl stop`。原因：`__init__` eager recovery（INV-CM-ORDER-1）在引擎启动瞬间跑——若旧进程仍在写 tickfile + 新代码 `__init__` recovery 并发 → 无 flock 保护（旧代码无 flock）→ 数据损坏。旧进程停止后新 `__init__` recovery 才安全。

**M-R11-3 — config 开关 enable_tickfile_commit_marker**
- 新增 `RecoveryConfig.enable_tickfile_commit_marker: bool = True`。False 时：`write_tickfile_rows` 跳过 sidecar append + flock；`_recover_tickfile_to_last_commit` 总返回 `had_sidecar=False`（降级 row-based）。
- 符合项目惯例（`enable_order_accel` pattern）。运维 kill-switch：改 config + restart（无需 full code rollback）。

**GIL baseline（Maj-R11-1/2/3）**
- **tickfile-writer 线程不在 Phase 21 去 GIL 范围**——Phase 21 仅 order/snapshot 两条管道去 GIL（`py.allow_threads`）；tickfile 路径本就全程持 GIL（Python `write_tickfile_rows` 或 Rust `tickfile_generate` 全程持 GIL，lib.rs 注释明写 "GIL held ~50ms"）。**sidecar append 不引入新 GIL 回归**——它叠加在已持 GIL 的临界区上。
- **sidecar 第 2 次 fsync GIL 增量**：每分钟多持 GIL ~5-20ms（SSD/HDD fsync 延迟）。order/snapshot Rust 线程在 tickfile-writer 持 GIL 的微秒级 flock/fstat 期间不受阻（NB + syscall 级）。实时峰值 12.7K lines/s、6.9x 余量下可吸收（memory phase21-status Q2）。§8 风险表加一行。
- **Rust tickfile_generate 兼容**：sidecar 设计同时兼容 (a) 当前 Python per-row append + (b) 未来 Rust whole-string write——两路径的 `fsync→fstat→sidecar append+fsync` 序列不变（INV-CM-ORDERED-TWO-FILE / INV-CM-SIDECAR-IN-LOCK 路径无关）。

**性能（M-R11-1/2 + minors）**
- **sidecar 末行读取**：REGEN-GUARD precondition 读 sidecar 末行——用 **tail-read**（`f.seek(-min(size,4096),2)`，复用 writer.py:386-391 模式），**禁止 `readlines()`**。sidecar 全天 ~16KB，tail-read 毫秒级。
- **writer retry 是 queue-serial bounded**（非 hot-loop）：每次重试 = queue.get(timeout=0.5) → 处理一分钟 → 失败 re-insert → 下次 iteration。连续 N 次后线程终止（engine.py:1299-1305）。flock/sidecar 开销随重试线性增加，但相对于失败的 fsync + ~4505 行重建可忽略。

**容器化（M-R11-4）**
- **INV-CM-CONTAINER**：不支持共享可写卷上多 FIU pod/容器。容器化须 `RecP=1`（单副本）或独占 per-pod volume。K8s `ReadWriteMany` PVC HA 打破 flock + SINGLEPROC。K8s rolling update 须 preStop hook + terminationGracePeriod > engine stop time。

**监控告警（M-R11-5）**
- metrics 是进程内（内存 int），硬崩溃丢失。**唯一持久信号 = audit log**（`tickfile_recovery.log` JSON-lines）。
- 最小告警查询（运维 runbook）：`tail -F tickfile_recovery.log | jq 'select(.result=="error" or .truncate_bytes>0 or .had_sidecar==false)'`。
- `tickfile_writer_perm_dead`（M-R5-7）须路由到 `logger.critical`（→ rotated `errors.log`），不能仅存内存 metric（硬崩溃前 writer 已死 → 无 CHECK 路径 → 运维看不到）。

### 3.8 Round 13 跨输出一致性 + sidecar 篡改防护 + spec 可读性（系统级增补）

> 前 12 轮聚焦 tickfile 自身 sidecar/flock/recovery 正确性；Round 13 首次从**系统级**角度审查——tickfile 与 order/snapshot 的跨输出一致性 + sidecar 外部篡改 + spec 对新实现者的可操作性。

**C-R13-1 — 跨输出分歧：order/snapshot 存在但 tickfile 被 truncate → 永久缺失**
- **场景**：crash 在 snapshot/order flush（per-minute rename 成功）后、tickfile sidecar 写入前 → recovery truncate 掉 tickfile 该分钟。重启后：`flushed_snapshot_minutes` / `_flushed_order_minutes` 含该分钟（从文件重建）→ snapshot/order 路径不重生成 → tickfile 永久缺。**INV-CM-CHECKPOINT-RECONCILE 只对账 snapshot↔tickfile，不含 order。**
- **INV-CM-RECONCILE-THREE-WAY（扩展 C-R9-1）**：recovery 完成后，对账**三路**：
  - `tickfile_missing = (output_minutes ∪ _flushed_order_minutes) − committed_set`（snapshot/order 有、tickfile 缺）→ CRITICAL + **从 order/snapshot 分钟文件重建 tickfile 行**（读 `order_minute_*` + `snapshot_minute_*` → `select_tickfile_records` → `write_tickfile_rows` + sidecar append）。
  - `tickfile_only = committed_set − (output_minutes ∪ _flushed_order_minutes)`（tickfile 有、snapshot/order 缺）→ CRITICAL + 从 tickfile 行**反向重建** snapshot/order 文件（tickfile 65 字段含足够信息），或 FAIL FAST 停止下游消费。
  - **INV-CM-RECONCILE-ORDER**：对账在 (a) tickfile recovery 返回 committed_set + (b) checkpoint `output_minutes` 加载 + (c) `_flushed_order_minutes` 从文件重建——三者都完成后运行。engine 启动中的时序点：`_restore_from_checkpoint`（加载 output_minutes + recover_flushed_minutes）→ flusher `__init__` recovery（committed_set）→ 对账。

**C-R13-2 — sidecar 外部截断/误清 → live engine 无 _scan fallback → 全量重生百万行重复**
- **场景**：运维操作（backup/rsync/logrotate/echo "" > sidecar）误截断 sidecar → `had_sidecar=False`。**replay 降级走 `_scan` 重建 skip-set（stale-fix 有）；但 live engine 的 `had_sidecar=False` 降级路径未定义 _scan fallback**（INV-CM-SKIPSET-LIVE 只说 committed_set 写入，committed_set=None 时 live 的 `_generated_tickfile_minutes` 是空集？→ live 认为全部分钟是 gap → 全量重生 → 百万行重复）。**spec 自身的 recovery 机制成为破坏源。**
- **INV-CM-SKIPSET-LIVE-FALLBACK**：live engine `had_sidecar=False` 时，**也必须**像 replay 一样运行 `_extract_minutes_from_tickfile` 填充 `_state._generated_tickfile_minutes`（不依赖 sidecar，从 tickfile 数据行 UpdateTime 列重建）。与 replay 降级路径对称。记 WARNING `live_skipset_reconstructed_from_rows`。
- **INV-CM-SIDECAR-TAMPER-DETECT**：recovery 时若 `had_sidecar=False` 但 tickfile 非平凡（size >> header），对比 audit log 上次记录的 `committed_count`——若上次 >0 而本次 sidecar 空 → CRITICAL `sidecar_tamper_detected` + 从 tickfile 行重建 sidecar（scan UpdateTime → 每分钟 offset → 写 sidecar 行）。
- 测试：`test_sidecar_truncated_live_rebuilds_skipset_from_rows`、`test_sidecar_tamper_detected_from_audit_log`、`test_reconcile_three_way_order_snapshot_tickfile`。

**Mj-R13-1 — INV 总表（实现者完成 checklist）**
- plan Task 0 须在 §3 开头加 INV 总表：`| ID | 一句话 | 所在小节 | 状态（active/deprecated/superseded-by-X） | 对应测试 |`。~55 条 INV 散布 §3.1-3.8 + §4 + §5，当前无法逐条 verify。过时 INV（INV-CM-BATCH 被 ORDERED-TWO-FILE 取代、INV-CM-LAST 被 OFFSET-MAX 取代）须标 `superseded`。

**Mj-R13-2 — §3 版本地图（新读者导航）**
- §3 开头加："§3.1/3.2 = sidecar 终版（覆盖所有前序 in-file 描述）；§3.4-3.8 = 增补，冲突时以 §3.1/3.2 为准。"

**Mj-R13-3 — 调用点接缝表**
- §3.3 加：`| 调用点 | 调用语句 | 写入字段 | had_sidecar=False 分支 |`，覆盖 flusher __init__ / replay run / health-check restart / cross-day resume（N/A）。

### 3.9 Round 15 故障传播链 + plan 可操作性 + 极端边界（系统级增补）

> 前 14 轮覆盖正确性/IO/并发/集成/运维；Round 15 从**故障传播链（cascade）、plan 执行者可操作性、极端边界组合**三个角度审查，找到 spec 未覆盖的灾难级传播路径。

**C-R15-1 — disk-full + REGEN-GUARD 分支 1/2 死区 → 逐分钟回退式数据销毁（最危险传播链）**
- **场景**：tickfile rows append+fsync 成功（占空间）→ sidecar append 抛 IOError（disk-full）→ re-insert → retry。precondition 读 sidecar 末行=N-1（sidecar 未记录 N）→ 走分支 1（`<`）→ **不检测 tickfile 已含 N rows** → append N 第二份 → 重复。若持续 disk-full + offset 漂移（newline-fix `\n`）→ 每次 truncate 到旧 offset 可能切掉已 commit 分钟的末行 → **逐分钟回退销毁合法数据，不可恢复**。
- **根因**：spec §3.4 REGEN-GUARD 分支仅基于 sidecar 末行 minute（一元），未结合 **tickfile size vs sidecar offset**（二元）。分支 1 场景"sidecar 未 fsync → 走分支 2 `==`"与分支定义"分支 2 要求 sidecar 末行==current"**自相矛盾**（sidecar 未写时末行是 N-1≠N）。
- **修正 INV-CM-REGEN-GUARD 分支为二元组四分支**：
  - (末行 `< current`, size==offset) → 纯净 append（1a）
  - (末行 `< current`, size>offset) → **未 commit 残留 → truncate 到 offset + 重写**（1b，**新增**）
  - (末行 `== current`, size==offset) → 已 commit → skip（2a）
  - (末行 `== current`, size>offset) → 重复残留 → truncate + 重写（2b）
  - sidecar 不存在 → 首建（3）
- **INV-CM-REGEN-TRUNCATE-IDEMPOTENT**：分支 1b/2b truncate 锚点 = sidecar offset；truncate 后 tail-check 若触发 newline-fix（说明 offset 与实际不一致）→ CRITICAL + abort。测试 `test_regen_branch1b_truncates_before_append`、`test_disk_full_cascade_no_committed_data_loss`。

**C-R15-2 — 跨天 pause 未栅栏旧日 sidecar 提交**
- **场景**：cross-day `_tickfile_writer_pause` join writer 线程 → 但 join 可能在一个分钟 commit 中途返回（tickfile fsync 后 sidecar fsync 前）→ 状态 clear（`_generated_tickfile_minutes.clear()`）→ 旧日 sidecar 缺末分钟 → resume 新日 → 旧日停在不一致状态。
- **INV-CM-CROSSDAY-FLUSH-BARRIER**：`_tickfile_writer_pause` join 后、跨天状态 clear 前，对**旧日**调 `_recover_tickfile_to_last_commit`（确保旧日 sidecar 一致 + truncate partial）。即跨天 clear 前 = 前一日期的 recovery 栅栏。与 INV-CM-ORDER-RESUME 的"新日期无需 recovery"呼应——被 recovered 的是**旧**日期。测试 `test_crossday_pause_flushes_old_date_before_clear`。

**C-R15-3 — sidecar 篡改检测无 audit log 基准时失效**
- **场景**：首次部署后 sidecar 被外部截断（运维 rsync/echo），audit log 不存在（首次运行无历史）→ INV-CM-SIDECAR-TAMPER-DETECT 无基准 → 静默降级 → 全量重生。
- **INV-CM-TAMPER-HEURISTIC-TICKFILE-NONTRIVIAL**：`had_sidecar=False` 且 tickfile 非平凡（size > header + 10KB）且 fallback scan 返回非空 → CRITICAL `sidecar_missing_nontrivial_tickfile`（".commit 缺失但 .csv 有数据 = 异常信号"）。与 audit log 检测互补。测试 `test_sidecar_missing_with_nontrivial_tickfile_critical`。
- **INV-CM-TAMPER-MULTI-BASIS**：tamper 检测不仅依赖 audit log，还与 **checkpoint `output_minutes` size** 交叉验证：sidecar 空但 checkpoint output_minutes 非空 → CRITICAL（checkpoint 独立文件，同时被清概率低）。

**C-OP-2 — §M-R3-2 "末行合法 65-field 数据行（与 §5 一致）" 与 §5 矛盾**
- sidecar 修订后 tickfile 无 marker 行。§M-R3-2 INV-CM-CLEANUP-ORDER 仍说"`.tmp` 末行合法 65-field 数据行（与 §5 一致）"——应改为"末行合法 65-field 数据行（与 §5 一致）"。**已修正。**

**M-R15-1 — flock fd 异常路径泄漏 → retry 自锁 → writer 永久死亡**
- **INV-CM-FLOCK-FINALLY**：flock fd close/unlock **必须**在 `try/finally` 的 finally 块（或 `with` 嵌套），确保 IOError 后释放。**禁止**只放成功路径末尾。测试 `test_flock_fd_released_on_ioerror`。

**M-R15-2 — NFS/非本地 fs 无运行时检测**
- **INV-CM-FS-CHECK-RUNTIME**：engine/replay 启动时检测 output_dir fs 类型（Linux `/proc/mounts`；Windows `GetVolumeInformation`）→ nfs/cifs/9p → 拒绝启动。这是部署假设的运行时强制。测试 `test_engine_rejects_nfs_output_dir`。

**M-R15-7 — sidecar 无 size 上限 → 外部垃圾追加致 recovery O(n) 慢**
- **INV-CM-SIDECAR-MAXSIZE**：recovery 前检查 `os.path.getsize(sidecar)`；若 > MAX_SIDECAR_SIZE（1MB，正常 ~16KB）→ CRITICAL + tail-read 最后 4096B 找合法行 / 降级。测试 `test_sidecar_huge_garbage_tail_read_fallback`。

**M-R15-5 — 午休无数据分钟不写 sidecar → 应文档化正常**
- **INV-CM-SESSION-GAP-OK**：交易时段缺口（TSE 11:30-12:30）不产生 sidecar 行——`last_commit_minute` 正常跨缺口（1130→1230）。recovery 单调性校验跳过缺口。非篡改。测试 `test_lunch_gap_recovery_ok`。

**M-R15-6 — lockfile 截断/替换也致 inode 竞争（非仅删除）**
- 扩展 INV-CM-LOCKFILE-IMMORTAL：lockfile 以 `open("a")` 打开（追加模式，**不截断**）。运维 runbook：lockfile **不得**被 backup/restore/rclone 截断或原子替换；`touch`（保留 inode）安全。

**M-R15-4 — replay 幂等二次跑静默 → 应 INFO 可观测**
- replay recovery 返回 `committed_set` ⊇ 全部输入分钟 → INFO `replay_noop_all_committed` + `replay_summary` 加 `tickfile_noop_skip_minutes`。

**plan Task 0 前置（Agent 2 发现）**
- **C-OP-1**：§7 ~31 条 [DEPRECATED] 必须在 plan Task 0 重写为 sidecar 等价（第 5 次确认，recurring）。
- **M-OP-1**：显示 flusher.py:98 的确切替换代码（`int` → 3-tuple 解包）。
- **M-OP-2**：命名纯辅助函数（`_classify_append_precondition` REGEN-GUARD 谓词、`_reconstruct_sidecar_from_tickfile_rows` 篡改重建）。
- **M-OP-3**：`select_tickfile_records`（tickfile.py:204）签名链接。
- **M-OP-4**：验证 flusher.py:660 分支 2 skip 时 seqno 是否真的被消耗（INV-CM-SEQNO-MONO-FILE 前提）。
- **M-OP-5**：`_extract_minutes_from_tickfile`（replay.py:62 static）是否需提升为模块函数（SKIPSET-LIVE-FALLBACK 需 live 调用）。

## 4. 崩溃场景全覆盖（C7 修订：不依赖 rows/marker 精确区分）

recovery 只认**单调性**：marker 落盘 ⟺ 该分钟完整。kernel 按 page 刷回，"写 rows 中途"与"写 marker 中途"不可靠区分，但恢复动作一致，故合并：

| 崩溃时机（合并后） | 文件状态 | 恢复动作 |
|---------|---------|---------|
| **marker 未落盘**（含 rows 部分写 / marker 部分写 / marker 完全没写） | 末尾是部分 rows 或截断/损坏 marker，**无合法末 marker** | truncate 到上个合法 marker，重生该分钟 ✓ |
| **marker 已 fsync** | 完整 rows + 合法 marker | 保留，replay/live 跳过 ✓ |
| **两分钟之间**（上分钟已 commit，下分钟未开始） | 上分钟完整+marker，文件正常结尾 | 无需 truncate ✓ |

关键不变量（重申 INV-CM-MONO / INV-CM-LAST）：**文件有效内容 = 截到最后一个合法 marker**；truncate 后末行必为合法 marker。marker 之后的一切都是未提交的，可安全丢弃重生。

### 3.10 Round 17 文件耦合 / 测试策略 / 部署 runbook（实施级增补）

> 前 16 轮覆盖设计正确性；Round 17 从**文件改动耦合、测试可执行性、部署 runbook 实操性**三个角度审查，找到从"设计对"到"实施/部署不走偏"的 gap。

**C-R17-1 — pandas 依赖（项目零三方依赖 → C-R7-3 核心实证形同虚设）**
- `requirements.txt` 明确"No third-party packages"。spec 的 `test_tickfile_csv_pandas_empirical` 的 skip-guard = 永久 skip。
- **方案**：新建 `requirements-dev.txt` 加 `pandas`（仅开发依赖，不影响运行时零依赖）；无 pandas 时退化 csv 版兜底（`csv.reader` 断言 65 字段 + 无 `#` + 无空行），**不永久 skip**。双保险：CI 装 pandas 跑强断言；本地零依赖跑弱断言。

**C-R17-2 — 无可执行部署 step list（§3.7 只给原则，未对齐 start.sh/systemd 双路径）**
- **§3.10.1 部署 runbook**（7 步）：
  1. 预检：`df -h output` 余量 > 3GB（sidecar + truncated 备份 + audit log）；`stat -f -c %T output` = ext4/xfs（非 nfs）；`ps aux | grep main.py` 确认单进程。
  2. 停止：`deploy/stop.sh`（手动 nohup 路径，kill -TERM + PID 文件）或 `systemctl stop fiu-minute-bar`（systemd 路径）。
  3. 备份：`tar czf ~/fiu_backup_pre_cm_$(date +%Y%m%d).tar.gz output/tickfile src`。
  4. 升级代码：`rsync -av --exclude='test/' --exclude='docs/' ./ prod:~/fiu/` 或 `git pull`。
  5. config：`production.ini` 加 `[recovery] enable_tickfile_commit_marker = true`；验证 `PYTHONPATH=src python -c "from minute_bar.config import load_config; print(load_config('config/production.ini').recovery.enable_tickfile_commit_marker)"`。
  6. 启动：`deploy/start.sh`；首跑观察 5 分钟 `tail -F logs/*errors.log`。
  7. 验证 sidecar：首个分钟边界后 `ls output/tickfile/*/*/tickfile_*.csv.commit` 存在 + `wc -l` ≈ 已过分钟数 - 午休。

**C-R17-3 — `.truncated.*` 备份回滚安全**
- **INV-CM-ROLLBACK-TRUNCATED-ISOLATED**：`.truncated.*` 备份文件名含 `.truncated.` 中缀 + time_ns + pid，任何 tickfile 目录扫描须 `glob('tickfile_*.csv')` 精确前缀（当前 `_cleanup_tickfile_tmp_files` glob `*.tmp` → 不匹配 → **已验证安全**）。旧代码回滚：`.truncated.*`/`.commit`/`.lock` 均不匹配 `tickfile_*.csv` → 安全忽略。

**C-R17-4 — 监控告警（三种生产拓扑方案）**
- **§3.10.2 监控 runbook**：
  - **最小（无 Prometheus）**：cron 每分钟 `grep -E '"result":"(error|fallback)"|"truncate_bytes":[1-9]|"had_sidecar":false' output/tickfile/tickfile_recovery.log | tail -1 | mail -s "FIU alert" ops@`。
  - **journald + Loki**：promtail scrape `tickfile_recovery.log` + Loki alert `count_over_time({filename="tickfile_recovery.log"} | json | result="error" [5m]) > 0`。
  - **Prometheus textfile**：recovery 后写 `output/tickfile/tickfile_recovery.prom`（node_exporter `--collector.textfile.directory`），AlertManager rule `tickfile_writer_perm_dead == 1`。
  - **audit log 路径明确**：`output_dir/tickfile/tickfile_recovery.log`（**顶层 tickfile 目录**，非嵌套 `{YYYY}/{MMDD}/`——跨日单文件累积；首次写时 `os.makedirs(output_dir/tickfile, exist_ok=True)`）。

**C-R17-5 — 故障演练 runbook（4 场景）**
- **§3.10.4 故障演练**（部署后非交易时段执行）：
  1. **sidecar tamper**：`echo "" > ...csv.commit` → 重启 → 验证 CRITICAL `sidecar_tamper_detected`。
  2. **hard crash recovery**：运行中 `kill -9` → 重启 → 验证 `.truncated.*` 备份 + sidecar 最后 commit 正确 + tickfile truncated。
  3. **disk-full cascade**：`fallocate` 填满磁盘留 1MB → 观察 sidecar append IOError → 验证无重复行 + 无逐分钟回退。
  4. **lockfile inode 竞争**：`cp + rm + cp` 替换 lockfile → 重启 → 验证 flock 互斥失效 → CRITICAL。

**M-R17-A1 — flock 位置**
- **INV-CM-FLOCK-LOCATION**：flock 仅在 `write_tickfile_rows`（L338 `_get_write_lock` 内、三分支拆分前）+ `_recover_tickfile_to_last_commit` 中获取。**禁止**在 `_get_write_lock` / `atomic_write` / `append_order_records` / `append_snapshot_records` 中加 flock（否则 snapshot/kline 写入也尝试 flock 不存在的 lockfile）。

**M-R17-A2 — `_extract_minutes_from_tickfile` 循环导入**
- **必须**提升为 `writer.py` 模块级函数（不能 defer M-OP-5）。`replay.py:62` 改为 `from minute_bar.writer import extract_minutes_from_tickfile` 薄委托。理由：`_recover_tickfile_to_last_commit`（writer.py）和 `SKIPSET-LIVE-FALLBACK`（flusher → writer）都需调用 → writer 导入 replay 会循环导入（replay 已导入 writer）。

**M-R17-A3 — 跨天 recovery 时序**
- **INV-CM-CROSSDAY-BARRIER-BEFORE-PRUNE**：跨天 recovery 须在 force-gen（L224-252）之后、状态 clear（L264）+ `_prune_write_locks`（L304）之前，且**完全返回后才执行 prune**（recovery 的 flock fd 在 with 内释放 → prune 删 `_write_locks` 条目安全）。

**M-R17-A4 — 三 lazy seqno 入口 MUST 删除**
- 强化 M-OP-1：**MUST**（非"推荐"）删除 `flusher.py:39` `recover_tickfile_seqno_lazy` + `flusher.py:702` `_recover_tickfile_seqno` + `replay.py:311` lazy seqno。`__init__` recovery 成为唯一 seqno 源。测试 `test_no_lazy_seqno_paths_remain`（grep 断言源码无 `recover_tickfile_seqno(` 调用者）。

**M-R17-A6 — sidecar fsync 受 skip_fsync 控制**
- **INV-CM-SIDECAR-SKIP-FSYNC**：sidecar append 的 fsync 同样受 `skip_fsync` 参数控制（与 tickfile rows fsync 一致）。live 路径 `skip_fsync=True` 时 sidecar 也不 fsync（否则每分钟多一次 fsync → 侵蚀 6.9x 余量 + `TestNoFsyncLiveEngine` break）。

**M-R17-A8 — 首次升级推荐路径**
- **RECOMMENDED**：接受旧分钟无 sidecar（recovery 降级 `_scan` + `INV-CM-FALLBACK-STRIP` 自愈）。replay-to-fresh 仅当旧 tickfile 已知损坏时可选，**非升级前置**。

**M-R17-A5 — 测试 markers + layer**
- plan Task 0 注册 pyproject markers（unit / integration / e2e / slow / requires_pandas / requires_fcntl）+ §7 加测试→layer 映射表。

**M-R17-A7 — config 改动清单**
- `config.py RecoveryConfig` 加 `enable_tickfile_commit_marker: bool = True`（L73 附近）+ `load_config` 加 `s.getboolean("enable_tickfile_commit_marker", ...)`（L160 附近）+ `production.ini` 等 7 个 ini 文件加 `[recovery] enable_tickfile_commit_marker = true`。

## 5. tail-check / newline-fix（sidecar 修订后：C1 自动消除）

**sidecar 修订后，tickfile 只有数据行（65 字段），无 marker 行**。现有 `write_tickfile_rows` append 路径的 tail-check（`len(fields)!=65` → 补 `\n`）**无需修改**——健康文件末行永远是 65 字段数据行 → `len==65` → 合法 → 不触发 fix。

**原 C1（marker 行 3 字段误判）自动消除**——不再有 marker 行进 tickfile。原 §5 的 `_is_legal_last_line` 谓词、C-R2-1 截断 marker 处理**全部不再需要**（已随 in-file marker 废弃）。

newline-fix 仍保留原有职责（防上一次 append mid-截断的 partial 数据行残留），与 sidecar recovery 协同：recovery 在启动/重启时按 sidecar offset 做精确 truncate；newline-fix 在每次 append 前做尾部保护。两者互不干扰（tickfile 无 marker，谓词不变）。

## 6. 向后兼容 + 回滚安全（C5/M2/m3）

**新代码读老文件（前向）**：
- 新文件（新代码写）：有 sidecar → sidecar 模式 + truncate。
- 老文件（部署前生成，无 sidecar，有数据行）：`had_sidecar=False` → 降级 row-based presence，不 truncate。或一次性 replay-to-fresh-dir 重生（同时重建 sidecar）。
- 空文件（仅 header）：`had_sidecar=False`，`committed_set=set()`，不 truncate。

**混存文件（滚动升级）**：新代码首次写老 tickfile → 写数据行 + 建 sidecar。之后 recovery 读 sidecar（committed = sidecar 分钟）。**老 row-only 分钟（sidecar 建立前的）**：因 sidecar 不含它们 → 不在 committed_set → replay 会当 gap 重生 → **重复**。缓解：首次升级时一次性 replay-to-fresh 重建 tickfile + sidecar（用户确认源 csv 不滚动删除，#4，replay-to-fresh 永远可行）；或 recovery 检测"tickfile 有数据但 sidecar 首 offset > 0"→ WARNING + 把 offset 之前的数据行分钟纳入 committed_set（保守，接受首段 partial 风险）。

**老代码读新文件（回滚，C5——sidecar 修订后自动消除）**：
- tickfile **纯净**（只 65 字段数据行，无 marker）→ 旧代码读它**完全正常**（tail-check `len==65` 合法、csv/pandas 正常）。**原 C5 空行污染风险消除**。
- sidecar `tickfile_{date}.csv.commit` 是新文件，旧代码不读它（忽略）→ 无影响。回滚后 sidecar 残留无用（可删，下次新代码重建）。
- **Rollback playbook**（简化）：
  0. 验证源数据存在（用户确认 #4 不滚动删除 → 始终可行）。
  1. 回滚后若需清理：旧代码正常读写纯净 tickfile；sidecar 残留可忽略或删。
  2. 无需 `grep` 清空行（tickfile 本就纯净）。

## 7. 测试计划（TDD，C6 补强）

> **⚠ sidecar 修订注**：以下"in-file marker"相关测试（marker 写入/截断 marker/混存 marker/tail-check marker 等）**随 in-file marker 废弃**，改为 sidecar 等价测试。仍适用者（fail-atomic、seqno-monotonic、writer retry 四分支、guard、restart、feed seam、audit）保留。**新增 sidecar 专项**：
>
> **⚠ Round 9 [DEPRECATED] 标注**：下方 §7 正文（Marker 写入 / Recovery 正确性 / Tail-check 协同 / 混存 / Live restart 等**约 31 条**，含 E2E / fault-injection / writer-retry / cleanup 等）仍保留**原始 in-file marker 断言**（如"末行是 `#COMMIT`"、"无 `#COMMIT` 行"）。这些条目**全部 [DEPRECATED]——plan Task 0 必须逐条重写为 sidecar 等价**（断言对象从"tickfile 末行 marker"改为"sidecar `.commit` 行数/末行/offset"）。在重写前，**实现者不得照搬这些断言**——照写会与 §3 sidecar 设计直接冲突（tickfile 无 marker 行，所有 `#COMMIT` 断言必失败）。以 §7 顶部 sidecar 专项测试 + §3.6 Round 9 集成增补中的测试名为准。
> - `test_sidecar_write_after_tickfile_fsync`（INV-CM-ORDERED-TWO-FILE：tickfile fsync 先、sidecar fsync 后）。
> - `test_recover_reads_sidecar_truncates_tickfile_to_offset`（读 sidecar → truncate tickfile 到 offset）。
> - `test_sidecar_partial_last_line_skipped`（sidecar 末行截断 → 校验跳过，不污染 committed_set）。
> - `test_sidecar_missing_falls_back_row_based`（sidecar 不存在 → 降级，不 truncate）。
> - `test_sidecar_records_seqno_recovery_no_tickfile_scan`（last_seqno 来自 sidecar，免扫 tickfile）。
> - `test_flock_excludes_cross_process`（fcntl.flock 跨进程互斥；两进程同 lockfile → 第二个阻塞/失败）。
> - `test_tickfile_pure_no_marker_rows`（csv/pandas 读 tickfile 无 marker 行、无 `#`、65 字段一致——证 #1 兼容）。
> - **`test_tickfile_csv_pandas_empirical`（C-R7-3，#1 核心实证）**：跑真实 `ReplayEngine`/`write_tickfile_rows` 生成 3 分钟 tickfile + sidecar → `pd.read_csv(tickfile)` 断言 `shape[1]==65`、`isna().sum().sum()==0`、无 `Unnamed:` 列、无 `#` 行；`pd.read_csv(sidecar)` 4 列。**真实 pandas，非字符串断言**（skip-if-pandas-absent）。
> - `test_flock_excludes_cross_process`（**Maj-R7-2，须 subprocess**）：fcntl.flock per-OFD，threading 测不出互斥 → 主进程持 flock + `subprocess` 起子进程取 LOCK_EX|LOCK_NB → 断言子进程 BlockingIOError/超时失败（@pytest.mark.slow）。
> - `test_partial_tickfile_with_live_sidecar_recovers_before_append`（Maj-R7-1）：tickfile=[header+0901 rows+0902 partial 3-field] + sidecar 含 0901 不含 0902 → recovery truncate 到 0901 末、无垃圾行。
> - `test_sidecar_offset_exceeds_size_aborts_no_sparse_gap`（M-R7-4）：tickfile=100B、sidecar offset=500 → 不 truncate、不扩展、降级。
> - `test_sidecar_empty_equivalent_to_missing`（M-R7-5）：sidecar 存在但全非法行 → had_sidecar=False 降级。
> - `test_sidecar_missing_strips_partial_last_line`（M-R7-2）：sidecar 缺 + tickfile 末 partial → tail-strip 剥离。
> - `test_recover_truncate_uses_max_offset_not_last_line`（M-R7-3）：乱序 sidecar → truncate 到 max offset。
> - `test_flock_nb_aborts_when_held`（M-R7-1）：LOCK_NB 非阻塞，held → BlockingIOError → abort。
> - `test_offset_via_fstat_not_path_stat`（C-R7-1a）：monkeypatch 证 offset 来自 fstat-on-fd 非 path.getsize。
> - `test_audit_log_fields_match_outcome`（Maj-R7-6）：audit JSON 字段与 recovery 实际一致；命名统一 had_sidecar。

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

**Tail-check 协同（C1 + C-R2-1）**：
- `test_tail_check_recognizes_marker_as_legal_last_line`：文件末行是合法 `#COMMIT,...` → append 下一分钟不插多余 `\n`。
- `test_tail_check_truncated_marker_triggers_newline_fix`（C-R2-1）：末行是截断 marker（`#COMMIT,2026052809`，2 字段）→ `_is_legal_last_line` 返回 False → `need_newline_fix=True`（隔离成独立行，待 recovery 清）。

**混存 + 兼容**：
- `test_recover_legacy_mix_includes_row_only_minutes`：[老 0930 rows][老 0931 rows][新 0932 rows+marker] → committed_set ⊇ {0930,0931,0932}（M2）。
- `test_legacy_file_no_markers_falls_back`：无 marker 有数据 → row-based，不 truncate。
- `test_legacy_empty_file_no_markers`：仅 header → had_sidecar=False，committed_set=set()，不 truncate（m3）。

**Live restart 路径（C2/M3）**：
- `test_engine_start_truncates_uncommitted_before_writer_thread`：构造含部分分钟的 tickfile，Engine.start() → 断言 writer 线程启动时文件已 truncate（INV-CM-ORDER-1）。
- `test_seqno_recovery_after_truncate_excludes_dropped_minute`：truncate 后 seqno 不指向已删分钟（INV-CM-ORDER-2）。
- `test_live_recovery_populates_skipset`：recovery 后 `_state._generated_tickfile_minutes` 含 committed（INV-CM-SKIPSET）。

**端到端（C6 + M-R2-2，核心价值证明，双路径）**：
- `test_e2e_mid_append_crash_recovery`（replay 路径）：helper 写 `[header + 0901 rows + #COMMIT,0901,N + 0902 partial rows (no marker)]`，调 `ReplayEngine.run()` with snapshot 含 0901+0902 → 断言：最终 `#COMMIT` 数=2、0901 行数不变、0902 行数=预期、无空行无重复。模板：stale-fix `test_replay_fills_gap_without_corrupting_correct_rows`。
- `test_e2e_live_restart_recovers_partial_minute`（**live restart 路径，M-R2-2，强制不退化半闭环**）：helper 写 `[header + 0901 rows + #COMMIT,0901,N + 0902 partial rows]` → 种子 csv_dir（snapshot.csv+code.csv 喂 0902，复用 stale-fix 模式）→ 构造 `Engine` + `.start()` + 轮询 `engine._tickfile_dequeue_count` 直到处理完 0902 → `.stop()` → 断言：文件末行 `#COMMIT,0902,M`、0901 行数不变、0902 行数=预期、无重复无空行、`.truncated.*` 备份存在（若 truncate 发生）。**这是生产硬崩恢复的真实路径，必须强制闭环**（feed seam = seed csv_dir + poll，零生产侵入）。

**外部 consumer 兼容（C5）**：
- `test_external_csv_reader_skips_hash_lines`：标准 csv.reader 解析含 `#COMMIT` 行的 tickfile，断言跳过/显式处理。

**Fault-injection / 真实崩溃字节（M-R3-7，拦 mid-append 错误）**：
- `test_write_tickfile_rows_mid_append_exception_no_partial_marker`：monkeypatch `os.fsync` 在 N 次 write 后抛 OSError → 断言无 `#COMMIT` 行存活（证 INV-CM-BATCH）。
- `test_recovery_scan_io_error_aborts_without_truncate`（C-R3-2）：文件迭代中途抛 OSError → 文件字节完全不变、无 `.truncated` 备份（INV-CM-FAIL-ATOMIC）。
- `test_recover_handles_byte_truncated_marker_at_page_boundary`：`path.write_bytes(b"...#COMMIT,202605")`（无 `\n`、page 边界截断）→ 非法 marker，正确处理。
- `test_recover_handles_no_trailing_newline_on_partial_rows`：partial rows 无尾 `\n` → 不误判。

**Writer retry / health-check / 跨进程（Round 3）**：
- `test_writer_ioerror_retry_truncates_partial_no_duplicate`（C-R3-1）：write 中途 IOError + partial rows → re-insert → retry → 无重复 + 末行合法 65-field 数据行（与 §5 一致）。
- `test_writer_ioerror_after_marker_write_no_duplicate`（**M-R4-1a**）：rows+marker 已写（page cache），`os.fsync` 抛 IOError → re-insert → retry → precondition 见末尾合法 marker 且 minute==current → **skip append + 标 committed** → 无重复。
- `test_writer_health_check_calls_recovery_before_restart`（C-R3-1）：writer 死亡 → health_check → recovery 被调 + skip-set 同步。
- `test_replay_rejects_concurrent_live_date`（C-R3-3）：pidfile 占用 → replay abort。
- `test_replay_guard_atomic_excl_rejects_concurrent_start`（**M-R5-4/Maj-R6-2**）：两线程并发 EXCL open 只一成功（实现提示：用预创建文件模拟"已存在 → FileExistsError"，纯 Python 真并发不可靠）。
- `test_replay_guard_reclaims_stale_pidfile_after_live_crash`（**M-R5-4/Maj-R6-2**）：stale pidfile + pid 死（如 999999）→ replay 成功 + WARNING `stale_pidfile_reclaimed`（实现提示：monkeypatch `os.kill`/win32 ctypes 模拟 pid liveness）。
- `test_replay_guard_aborts_when_live_pid_alive`（**M-R5-4/Maj-R6-2**）：pidfile + pid 活 → abort。
- `test_recovery_returns_seqno_from_committed_only`（M-R3-4）：partial 行 seqno 不进返回值。
- `test_recovery_seqno_override_takes_max_never_regresses`（**M-R5-2/Maj-R6-1**）：构造内存 seqno=10（分支 2 skip 消耗）+ 文件 last_seqno=9 → recovery 覆盖后 `_tickfile_seqno == max(9,10) == 10`，下一分钟写 seqno=11（非 10 重用）。证 INV-CM-SEQNO-MONO-FILE。
- `test_flusher_init_runs_recovery_before_eager_seqno_fetch`（**M-R5-1/Maj-R6-1**）：构造含 partial 分钟 tickfile → `_make_flusher(...)`（不调 start，触发 `__init__` eager）→ 断言文件已 truncate + `state._tickfile_seqno` 取自干净文件（非 partial）。证 INV-CM-ORDER-1 init 点。
- `test_recover_filters_out_wrong_date_markers`（M-R3-5）：跨日 marker → WARNING，不进 committed_set。
- `test_recovery_backup_no_collision_rapid_double`（M-R3-3）：同秒双 truncate → 两份备份（time_ns+pid）。
- `test_cleanup_tmp_requires_marker_before_replace`（M-R3-2）：.tmp 无 marker → 删，不 replace。
- `test_recovery_writes_persistent_audit_log`（M-R3-8）：双崩溃重启 → audit log 2 条。

**Live restart E2E（M-R2-2 收紧，无半闭环 escape）**：
- `test_e2e_live_restart_recovers_partial_minute`：**强制**（不退化半闭环）——复用 stale-fix `test_replay_fills_gap_without_corrupting_correct_rows` 的 snapshot+code CSV mock 模式喂 0902 数据，断言最终 0902 行数正确、无重复、末行合法 65-field 数据行（与 §5 一致）。

回归：现有 tickfile/replay/seqno-recovery 测试更新期望（marker 行存在；tail-check 谓词变化；recovery 返回 3-tuple）。

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
| **writer retry/health-check 重生不 truncate → 重复（C-R3-1）** | 高 | 高 | INV-CM-REGEN-GUARD（每次 append 自愈）+ ORDER-RESTART；disk-full/IOError 高频 |
| **recovery 自身扫描异常误 truncate → 丢数据（C-R3-2）** | 低 | 高 | INV-CM-FAIL-ATOMIC（扫描异常不 truncate） |
| **跨进程 live+replay 同目录 → 腐败（C-R3-3）** | 中 | 高 | INV-CM-SINGLEPROC 部署假设 + replay guard；多进程需 OS 锁（future） |
| atomic-create 跨进程 rename 覆盖（M-R3-10，既有） | 低 | 高 | INV-CM-SINGLEPROC 覆盖；根治 .tmp+pid（future） |
| writer-retry carry-forward 用旧 snapshot 略过时（M-R3-9） | 中 | 低 | §8 文档化（既有行为，marker 放大；可接受） |
| rollback 源数据已删 → 无法 replay-to-fresh（M-R3-6） | 中 | 中 | §6 playbook Step 0 验证源存在 |

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
