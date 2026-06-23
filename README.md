# FIU Minute Bar Generator

日股分钟级行情数据生成器。读取 FIU 接收服务实时写入的 `snapshot.csv` / `order.csv` / `code.csv` 文件，按分钟生成全市场行情快照、OHLCV K 线、order 分钟文件，以及**每天一个的 tickfile**（每 symbol 每分钟一行，65 字段，供下游量化消费）。

tickfile 是 per-day append-only（不像 snapshot/order 每分钟一个原子文件），因此内置 **sidecar commit + truncate 恢复**机制，保证硬崩溃 mid-append 后重启能干净恢复、不重不缺。

---

## 功能

- **Live 模式**：多线程实时处理，数据驱动 watermark flush；崩溃后重启自愈（急切 recovery）
- **Replay 模式**：离线回放历史数据，流式处理（峰值内存 ~120MB）；启动 recovery 修复 partial + 补 gap
- **Tickfile 崩溃恢复**：sidecar 提交点 + truncate-to-last-commit，硬崩溃 mid-append 不丢已提交分钟、不重复
- **Rust 加速**（Phase 20-21）：order/snapshot 解析 + tickfile 生成可选 Rust 化，去 GIL，实时峰值 ~7x 余量
- **数据完整性**：迟到记录 append、per-minute snapshot、carry-forward 时间过滤、double-flush 防护、seqno 单调
- **异常安全**：stall-triggered flush、checkpoint 断点恢复、跨天自动重置 + 旧日 recovery

---

## 快速开始

### 依赖

- 运行时：Python >= 3.8，**仅标准库**（无第三方依赖）
- 开发：`pip install -r requirements-dev.txt`（加 `pandas`，仅用于 csv 兼容实证测试）

### 环境与构建（Rust 加速扩展）

本项目订单解析 / 快照聚合 / tickfile 生成用 **PyO3 + setuptools-rust** 实现为原生扩展 `_order_accel`（cdylib，**非 maturin**）。

**运行环境**：

| 组件 | 要求 |
|---|---|
| Python | >= 3.8（仓库内预编译产物 `_order_accel.cp312-win_amd64.pyd`） |
| Rust 工具链 | rustup + cargo **1.84**（`order_accel/rust-toolchain.toml` + `Cargo.toml` `rust-version` 锁定；含 rustfmt/clippy） |
| Rust 依赖 | pyo3 0.23 / indexmap 1.9 / crc32fast 1 / lazy_static 1.4（见 `order_accel/Cargo.toml`） |
| Python 构建工具 | `setuptools>=68.0` + `setuptools-rust` |
| 系统 | C 工具链（Windows: MSVC；Linux: gcc） |
| 运行时三方库 | **零**（stdlib-only） |

**构建**（权威命令，与 `engine.py` 错误提示一致）：

```bash
pip install setuptools-rust
pip install .                         # 或开发：pip install -e .
python setup.py build_ext --inplace   # PEP 660 绕过 setup.py 时回退
```

- **产物**：`_order_accel`（`Cargo.toml [lib] name`），落在 `src/minute_bar/` 内。Windows: `_order_accel.cp312-win_amd64.pyd`；Linux: `_order_accel.cpython-3xx-x86_64-linux-gnu.so`。
- **不纳入 git**（`.gitignore` 忽略 `*.pyd`/`*.so`）——clone 后必须本地构建，或以纯 Python 兜底运行。
- **加载与兜底**：导入点 `csv_parser.py` / `engine.py`（`parse_order_batch` / `process_order_batch` / `parse_snapshot_batch` / `aggregate_snapshot_batch` / `tickfile_generate` 等 11 个函数），均包在 `try/except ImportError` 中 → 缺失自动回退纯 Python。运行时开关 `[input] enable_order_accel`（默认 false；置 true 但扩展缺失 → 启动 fail-fast）。细粒度：`enable_rust_order_full_batch` / `enable_rust_snapshot_batch` / `enable_rust_tickfile`。

### Live 模式（实时生成当天）

```bash
PYTHONPATH=src python main.py --config config/production.ini
```

### Replay 模式（离线回放/补齐历史某天）

```bash
PYTHONPATH=src python main.py --config config/production.ini --replay 20260528
```

CLI 参数：
- `--config <ini>`（必填）：配置文件路径
- `--replay YYYYMMDD`（可选）：给出则进 replay 模式处理该历史日；不给则进 live 模式

### 数据模拟器（端到端测试）

```bash
# 终端 1：启动 minute_bar（live）
PYTHONPATH=src python main.py --config config/test.ini

# 终端 2：启动模拟器（100 倍速，保留乱序 + 半行 + late record）
PYTHONPATH=src python -m data_simulator --speed 100 --file-types order,snapshot,code \
    --date 20260528 --source-dir input --output-dir test/output
```

### 测试

```bash
python -m pytest tests/ -v                 # 全量
python -m pytest tests/test_tickfile_commit_marker.py -v   # commit-marker 单元/集成
python -m pytest tests/e2e_tickfile_restart_recovery.py -v # 真 E2E 重启恢复（@slow）
python -m pytest tests/ -m "not slow"      # 跳过慢测试（live 引擎 wall-clock）
```

---

## 输出文件

按日期归档在 `output_dir` 下。snapshot/order/kline **每分钟一个原子文件**（tmp + rename）；tickfile **每天一个 append 文件** + sidecar。

```
output/
├── snapshot/{YYYY}/{DATE}/snapshot_minute_{DATE}_{HHMM}.csv   # 每分钟，每 symbol 一行
├── order/{YYYY}/{DATE}/order_minute_{DATE}_{HHMM}.csv         # 每分钟，该分钟所有 order 记录
├── kline/{YYYY}/{DATE}/kline_minute_{DATE}_{HHMM}.csv         # 每分钟 OHLCV（enable_kline=true 时）
├── tickfile/{YYYY}/{DATE}/tickfile_{DATE}.csv                 # 每天一个，每 symbol 每分钟 1 行（65 字段）
├── tickfile/{YYYY}/{DATE}/tickfile_{DATE}.csv.commit          # ⭐ sidecar 提交日志（commit-marker 核心）
├── tickfile/{YYYY}/{DATE}/tickfile_{DATE}.csv.lock            # flock 跨进程锁文件（0 字节，永不删）
├── tickfile/{YYYY}/{DATE}/tickfile_{DATE}.csv.truncated.{ns}.{pid}  # 截断备份（recovery 时生成，保留最近 10 份）
├── tickfile/tickfile_recovery.log                             # recovery 审计 JSONL（跨日单文件，跨崩溃存活）
└── checkpoint.json                                            # 断点（文件读取 offset + 已输出分钟）
```

**tickfile 数据行**：65 个逗号字段。关键列：`InstrumentID`(0)、`TradingDay`(1)、`LastPrice`(2)、`Volume`(9)、`UpdateTime`(16, `YYYYMMDD HH:MM:00`)、`Seqno`(59)。下游 csv/pandas 直接读，**无 `#` 注释行、无空行**（sidecar 是独立文件，tickfile 保持纯净）。

**sidecar (`.commit`)**：每已提交分钟一行 `<minute>,<offset>,<rowcount>,<seqno>`，例：
```
202605280931,1329339,4505,3
202605280932,2662587,4505,4
```
- `offset` = 该分钟数据行 append+fsync 后 tickfile 的字节大小（= 截断锚点，由 `os.fstat` 精确读取）
- `rowcount` = 该分钟实际写入行数（≈ symbol 数）
- `seqno` = 该分钟 seqno（recovery 直接取，免扫 tickfile）

下游消费方**只读 `.csv`，不读 `.commit`/`.lock`/`.truncated.*`**。

---

## Tickfile 与崩溃恢复（核心机制）

### 为什么需要

snapshot/order 是每分钟一个文件（tmp+rename，原子）；tickfile 是**每天一个 append 文件**（非原子——POSIX 无"原子追加"）。硬崩溃（kill -9 / OOM / 断电）恰好发生在某分钟 tickfile append 中途 → 该分钟部分写入（部分 symbol 行 + 可能截断尾行）。没有恢复机制的话，replay 的二值扫描会误判"有合法行=完整"→ 跳过该分钟 → **永久部分缺失**；而 append-only 决定重生会 append → **重复**。

### sidecar 提交点（方案 B）

- **写一个分钟的顺序**（`write_tickfile_rows`，在 `_get_write_lock` RLock + `fcntl.flock` 双锁内）：
  1. 向 tickfile append 该分钟数据行 → flush + **fsync**
  2. 读 `offset = os.fstat(fd).st_size`（落盘后字节大小）
  3. 向 sidecar append `<minute>,<offset>,<rowcount>,<seqno>` → flush + **fsync** ← **这一步 = 提交点**
- 关键顺序：**tickfile fsync 严格先于 sidecar fsync**（INV-CM-ORDERED-TWO-FILE），所以"sidecar 有 N" ⟺ "N 的数据已安全落盘"（INV-CM-MONO）。

### 恢复（`_recover_tickfile_to_last_commit`）

启动/重启时调用（读 sidecar 几 KB，**不扫 1.5M 行 tickfile**）：
1. 读 sidecar 所有合法行 → 取**最大 offset** = 截断锚点；`committed_set` = 所有合法分钟
2. 若 tickfile 当前 size > max offset（有未提交 partial 尾）→ 备份尾字节到 `.truncated.*` → `os.truncate` tickfile 到 max offset
3. 返回 `(committed_set, last_seqno, had_sidecar)`；调用方写入 skip-set `_generated_tickfile_minutes`

**三处调用点**：
- **flusher `__init__`**（急切，live 重启）—— 替代了原 lazy seqno 获取，是 live 自愈的入口
- **`_run_tickfile_recovery`**（runtime，被 engine health-check / drain / pause 调用）
- **replay `run()` 启动** —— replay 补历史的入口

### 重启恢复拓扑

| 场景 | 做法 | 机制 |
|---|---|---|
| 引擎当天崩、当天发现 | 重启 live | `__init__` 急切 recovery 恢复今天 + 续跑填缺口 |
| 崩了几天后才发现（已跨天） | `--replay <旧日期>` | live 只认今天（jst_now），旧天只能 replay 回补 |
| 边跑今天 live、边补历史 | live + replay(不同日期) 并行 | 不同 per-day tickfile 文件，lockfile 不争用 |
| 同一天 live+replay 并行 | ❌ 别这么设计 | 同一 lockfile，第二个 `BlockingIOError` abort（非损坏） |

---

## 边界情况与故障恢复

| 边界情况 | 处理 |
|---|---|
| **硬崩溃 mid-append**（部分分钟） | sidecar 提交点 + recovery truncate 到最后 commit offset，重生该分钟，不重不缺 |
| **重启重复风险**（live+live / live+replay） | skip-set（`_generated_tickfile_minutes`）：已 commit 分钟跳过不重写；REGEN-GUARD 在 `write_tickfile_rows` 内二次保险 |
| **孤儿重试**（tickfile rows 已 fsync 但 sidecar 写前崩，sidecar 空） | `_classify_append_precondition` sidecar 空时查 tickfile 末行 minute：==current 截断孤儿块重写；!=current（legacy）正常 append 保老行（REGEN 2A 修复） |
| **partial 尾**（截断行 / 字段不全 / 完整行但无 sidecar） | recovery 截到 commit offset；fallback 路径 tail-strip 末尾 partial 行 |
| **sidecar 损坏**（截断末行 / 垃圾行 / 非法字段） | 逐行校验跳过非法行，不污染 committed_set |
| **sidecar 空 / 缺失**（老文件 / 误删） | 降级单遍 row-scan（不 truncate）；tickfile 非平凡 + 无 sidecar → CRITICAL `tamper` 告警 |
| **sidecar offset > 文件大小** | 不 truncate（防稀疏零字节空洞），降级 row-scan |
| **sidecar 错日期记录** | 按 date 前缀过滤，WARNING 不阻断 |
| **disk-full cascade** | 备份失败不 truncate（降级）；REGEN 防逐分钟回退销毁 |
| **跨天** | `_step1_cross_day_check` clear 前显式 recover **旧日**（discard 返回 set，不污染 live skip-set）；新日 fresh 文件无需 recovery；force-gen 失败重试一次 |
| **迟到记录**（late records） | append 到 late queue，不覆盖已 flush 分钟（double-flush 防护） |
| **stale carry-forward**（stale-fix） | shutdown/cross-day 跳过 order 未到的分钟；replay 扫描补 gap 不腐败正确行 |
| **minute-key 边界**（左开右闭） | 收盘 15:30:00 → 1530；order/snapshot 统一 round-up（floor+1） |
| **seqno 回退** | recovery 用 `max(file, mem)`（INV-CM-SEQNO-MONO-FILE），绝不重用 seqno |
| **网络文件系统** | 启动 `check_output_fs_local` 拒绝 nfs/cifs/9p（破坏 flock + 崩溃一致性） |
| **多进程同写同一日期目录** | flock `LOCK_EX\|LOCK_NB`：第二写者 abort（INV-CM-SINGLEPROC 单进程前提） |
| **recovery 自身异常**（扫描/IO 中途错） | INV-CM-FAIL-ATOMIC：扫描在 try 内，仅成功才 truncate；异常不半截截断 |
| **audit log 写失败**（磁盘满） | best-effort try/except，绝不阻断 recovery |

### 监控信号

- **持久**：`output/tickfile/tickfile_recovery.log`（JSONL，跨崩溃存活）。告警查询：
  ```bash
  tail -F output/tickfile/tickfile_recovery.log | jq 'select(.result=="error" or .result=="tamper" or .truncate_bytes>0 or .had_sidecar==false)'
  ```
- **进程内**：metric（硬崩溃丢失）；`tickfile_writer_perm_dead` writer 永久死亡 → CRITICAL 路由到 errors.log

---

## 核心配置项

`[input]`：
| 配置项 | 默认 | 说明 |
|---|---|---|
| `csv_dir` | （必填） | 输入目录（snapshot/order/code CSV） |
| `enable_order_accel` | false | Rust 加速 order 解析（生产建议 true） |
| `enable_rust_order_full_batch` | false | Rust 全管线 order（parse+group+buffer） |
| `enable_rust_snapshot_batch` | false | Rust 全管线 snapshot |
| `enable_rust_tickfile` | false | Rust tickfile 生成 |
| `order_chunk_size_bytes` | 65536 | Order chunk（生产建议 524288） |

`[output]`：
| 配置项 | 默认 | 说明 |
|---|---|---|
| `output_dir` | （必填） | 输出目录 |
| `enable_order` | true | 输出 order 分钟文件 |
| `enable_tickfile` | false | 输出 tickfile（**需 `enable_order=true`**） |
| `enable_kline` | true | 输出 OHLCV K 线 |

`[recovery]`：
| 配置项 | 默认 | 说明 |
|---|---|---|
| `enable_tickfile_commit_marker` | **true** | ⭐ tickfile sidecar commit + flock + truncate recovery 总开关（false 降级 legacy row-scan，无 sidecar） |
| `enable_time_fallback` | true | 真实时钟兜底（data-driven 为主） |
| `data_flush_delay_minutes` | 1 | 数据推进后延迟几分钟 flush（测试/E2E 设 0 = 纯 watermark 驱动） |
| `stall_flush_sec` | 300 | Watermark 停滞多少秒后触发 flush |
| `code_refresh_sec` | 30 | Code table 刷新间隔 |

> `enable_tickfile_commit_marker` 是**进程级静态 flag**（`__init__` 读一次，改需 restart）。运维 kill-switch：改 config + restart 即可关闭 sidecar/flock，无需代码回滚。

---

## 生产部署

```bash
cd /path/to/fiu && bash deploy/setup.sh
bash deploy/start.sh
bash deploy/stop.sh    # SIGTERM 优雅关闭，flush 最后分钟 + drain tickfile writer
```

**停机即升级（commit-marker 强制）**：`stop.sh → 升级代码/重建 _order_accel → start.sh`。`setup.sh` 现已含 Rust 扩展构建（`pip install .`）。应用层已注册 SIGTERM→KeyboardInterrupt→graceful stop（flush + tickfile drain + commit-marker 最终落盘）；务必经 SIGTERM 停机，SIGKILL 强杀会跳过最终落盘。仅 Linux 生产（ext4/xfs + fcntl.flock）。

详见 [deploy/](deploy/) 和 [config/production.ini](config/production.ini)。

**commit-marker 首次部署**：必须 `stop → upgrade → start`（绝不能 live 运行时 rsync）。原因：`__init__` 急切 recovery 在引擎启动瞬间跑，若旧进程仍在写 tickfile + 新代码并发 recovery（旧代码无 flock）→ 数据损坏。systemd `Restart=on-failure` 路径安全（= 完全停止后重启）。**生产仅 Linux**（ext4/xfs + fcntl.flock）；Windows 仅开发/测试。

---

## 架构与流程

下列三张图共同描述 FIU 日股分钟级行情生成器的数据管线、tickfile 提交点与崩溃恢复语义。

### 端到端数据管线

数据线程 / 订单线程 / 时钟线程通过 SharedState 协作，tickfile-writer 后台线程串行落盘并保证崩溃可恢复。

```
┌──────────────────────────────────────────────────────────────────────────┐
│   INPUT  (appended by upstream FIU receiver)                             │
│   snapshot.csv.{date}      order.csv.{date}      code.csv.{date}         │
└───────────────┬──────────────────────┬─────────────────────┬─────────────┘
                │ snapshot+code        │ order               │
                ▼                      ▼                     │
┌──────────────────────────┐  ┌──────────────────────────┐  │
│ DATA-THREAD              │  │ ORDER-THREAD             │  │
│ FileTailer (snap+code)   │  │ FileTailer (order)       │  │
│ ► parse ► validate       │  │ ► parse ► late-detect    │  │
│ ► aggregate              │  │ ► buffer                 │  │
│ ► write to SharedState   │  │ ► record + watermark     │  │
│ ◆ Rust accel (parse)     │  │ ◆ Rust accel (parse/wr)  │  │
└────────────┬─────────────┘  └────────────┬─────────────┘  │
             │                             │                │
             │  ┌──────────────────────────┴──────────┐     │
             │  │ CLOCK-THREAD  (Flusher.tick)        │     │
             │  │ step1 cross-day / step3 minute out  │     │
             │  │ step4 late-records / step5 ckpt     │     │
             │  └──────────────────┬───────────────────┘     │
             ▼                     ▼                         │
┌────────────────────────────────────────────────────────────┴────────┐
│ SHAREDSTATE  (RLock)                                                │
│  latest_snapshot │ ohlcv_buffers │ raw_snapshot_buffers             │
│  _tickfile_pending │ _generated_tickfile_minutes (skip-set)         │
│  _tickfile_seqno                                                    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ pending minutes (queue)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│ TICKFILE-WRITER  (separate bg thread; per-day serialize via queue)  │
│ ► REGEN-GUARD classify: new / append / truncate_rewrite / committed │
│ ► write_tickfile_rows  under _get_write_lock RLock + flock (per-date)│
│     step1 append rows ► tickfile.csv ► flush+fsync                  │
│     step2 offset = os.fstat(fd).st_size                             │
│     step3 append "<minute>,<offset>,<rowcount>,<seqno>" ► .commit   │
│            ► flush+fsync  (✓ COMMIT; tickfile fsync BEFORE sidecar) │
│ ► _recover_tickfile_to_last_commit (reads SIDECAR, not tickfile):   │
│     max-offset = truncate anchor; tail ► .truncated.* + os.truncate │
│     callers: flusher __init__ │ _run_tickfile_recovery │ replay.run │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
   ┌───────────────────────────┼────────────────────┬─────────────────┐
   ▼                           ▼                    ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐
│ PER-MIN OUT  │  │ PER-MIN OUT  │  │ PER-MIN OUT  │  │ PER-DAY TICKFILE    │
│ snapshot/    │  │ order/       │  │ kline/       │  │ tickfile_{D}.csv    │
│  {Y}/{D}/    │  │  {Y}/{D}/    │  │  {Y}/{D}/    │  │  + .commit (sidecar)│
│ snapshot_min │  │ order_minute │  │ kline_minute │  │  + .lock   (flock)  │
└──────────────┘  └──────────────┘  └──────────────┘  └─────────────────────┘
        checkpoint.json            tickfile/tickfile_recovery.log (audit)

  ◆ = Rust acceleration point   ✓ COMMIT POINT   ✗ partial-write (recoverable)
```

### Tickfile 提交点与崩溃恢复

写提交顺序保证 sidecar fsync 严格晚于 tickfile fsync；重启时只读 sidecar 决定截断锚点并重建 skip-set。

```text
┌──────────────────────────────────────────────────────────────────────────┐
│ (A) WRITE-COMMIT SEQUENCE — write_tickfile_rows (per-date lockfile+flock) │
│     Guard: _get_write_lock RLock  +  fcntl.flock(tickfile_{DATE}.lock)    │
├──────────────────────────────────────────────────────────────────────────┤
│  tickfile-writer ◄── SharedState._tickfile_pending (queue)               │
│        ▼                                                                 │
│   ┌──────────────────── REGEN-GUARD classify (BEFORE append) ─────────┐  │
│   │  precondition = sidecar-last-minute vs current   AND               │  │
│   │                tickfile-size       vs sidecar-offset               │  │
│   │   ├─► new              ─ first minute of the day                   │  │
│   │   ├─► append           ─ current==last+1, size==offset  ✓          │  │
│   │   ├─► truncate_rewrite ─ size > offset (torn tail) ✗               │  │
│   │   └─► committed-skip   ─ minute already in sidecar  ✓ (drop)       │  │
│   └────────────────────────────────────────────────────────────────────┘  │
│        ▼                                                                 │
│   step1  append 65-field rows ──► tickfile_{DATE}.csv                    │
│          flush + fsync   (tickfile durable 1st)                          │
│   step2  offset = os.fstat(fd).st_size     ◄── durable byte anchor       │
│   step3  append "<minute>,<offset>,<rowcount>,<seqno>"                   │
│          ──► tickfile_{DATE}.commit (sidecar)                            │
│          flush + fsync   ◄═══════╗ COMMIT POINT                          │
│   step4  add minute ──► SharedState._generated_tickfile_minutes (skip)   │
│                                                                          │
│   INVARIANT ══════════════════════════════════════════════════════════   │
│   sidecar has minute N  ⟺  minute N fully durable in tickfile.csv       │
│   (sidecar fsync ALWAYS strictly AFTER tickfile fsync)                   │
└──────────────────────────────────────────────────────────────────────────┘

                              │  process restart │
                              ▼ hard crash / stop
┌──────────────────────────────────────────────────────────────────────────┐
│ (B) RESTART-RECOVERY — _recover_tickfile_to_last_commit (reads SIDECAR)  │
│     Callers: flusher __init__ (live restart) │ _run_tickfile_recovery    │
│              (runtime: health/drain/pause) │ replay run()                │
│     Returns: (committed_set, last_seqno, had_sidecar)                    │
├──────────────────────────────────────────────────────────────────────────┤
│   read tickfile_{DATE}.commit ──► committed_set + last_seqno             │
│        ▼                                                                 │
│   max_offset = highest offset line in sidecar  (truncate anchor)         │
│        ▼                                                                 │
│   ┌──────────────────────────────────────────────────┐                   │
│   │  size == max_offset  ──► ✓ clean, nothing to do  │                   │
│   │  size  >  max_offset ──► ✗ torn tail (partial)   │                   │
│   │     backup tail ──► tickfile_{DATE}.truncated.*  │                   │
│   │     os.truncate(tickfile.csv, max_offset)        │                   │
│   │     log ──► tickfile/tickfile_recovery.log       │                   │
│   └──────────────────────────────────────────────────┘                   │
│        ▼                                                                 │
│   seed SharedState._generated_tickfile_minutes ◄── committed_set (skip)  │
│        ▼                                                                 │
│   resume ──► engine skips committed minutes (no dup), generates only NEW │
└──────────────────────────────────────────────────────────────────────────┘
```

### 重启恢复场景

live+live 用 truncate + skip-set 防重；live+replay 读全量源跳过已提交并补齐缺口。

```
┌──────────────────────────────────────────────────────────────────────────┐
│  3-FILE TICKFILE LAYOUT (per date D = YYYYMMDD)                          │
│  tickfile/{YYYY}/{D}/                                                    │
│    tickfile_{D}.csv       ◄── pure 65-field data rows                    │
│    tickfile_{D}.commit    ◄── sidecar: "<minute>,<offset>,<row>,<seqno>" │
│    tickfile_{D}.lock      ◄── fcntl.flock, 0 bytes, NEVER deleted        │
└──────────────────────────────────────────────────────────────────────────┘

   flock PER-DATE rule (lockfile = tickfile_{D}.lock):
   ├─ different dates ► no contention (live today + replay yesterday ► safe)
   └─ same date        ► 2nd writer ► BlockingIOError abort

┌───────────────────────────[ live + live ]───────────────────────────────┐
│  engine#1  commits 0900..0925  (csv fsync ► .commit fsync ◄ COMMIT)     │
│  mid 0926 append: csv has rows, .commit NOT yet written  ◄── partial    │
│  STOP / crash                                                            │
│        on disk: tickfile_D.csv = [..0905][0910]..[0925] + 0926 partial   │
│                  tickfile_D.commit = 0900..0925  (no 0926)               │
│                                                                          │
│  engine#2 (fresh) ► Flusher.__init__                                     │
│  ► _recover_tickfile_to_last_commit (reads .commit, NOT the .csv)        │
│      ├─ max_offset = truncate anchor                                     │
│      ├─ csv_size > max_offset ► tail ► .truncated.* + os.truncate        │
│      ├─ rebuild skip-set = {0900..0925}                                  │
│      └─ last_seqno restored                                              │
│  ► resume threads; REGEN-GUARD: 0900..0925 ► committed-skip ✓            │
│  ► generate 0926, 0927 ...  (only NEW minutes appended)                  │
│  RESULT ► no duplicate rows, no lost commits  ✓                          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────[ live + replay ]──────────────────────────────┐
│  live commits 0900..0925;  mid 0926 partial csv tail (no .commit line)   │
│  STOP                                                                    │
│        on disk: tickfile_D.csv = 0900..0925 + 0926 tail                  │
│                  tickfile_D.commit = 0900..0925                          │
│                                                                          │
│  ReplayEngine.run() (reads FULL source for the date)                     │
│  ► _recover_tickfile_to_last_commit (reads .commit)                      │
│      ├─ max_offset = truncate anchor ► drop 0926 partial tail            │
│      └─ skip-set = {0900..0925}                                          │
│  ► stream source 0900 ► 0930 (full replay)                               │
│  ► REGEN-GUARD: 0900..0925 ► committed-skip ✓  0926..0930 ► new ► append │
│  RESULT ► no duplicate rows, gap closed  ✓                               │
└──────────────────────────────────────────────────────────────────────────┘
```

### 关键机制

- **Data-driven watermark**：flush 时机由 `current_minute`（数据进度）决定，非真实时钟
- **Per-minute snapshot**：分钟推进前捕获快照，carry-forward 不含未来数据
- **Stall-triggered flush / Double-flush 防护**：watermark 停滞自动 flush 残余；已 flush 分钟的竞态数据路由到 late queue（append 不覆盖）
- **Drain loop / Order 独立线程**：有数据连续读，无数据走配置间隔；streaming write

---

## 测试

**528 个测试**覆盖全部模块 + 故障注入 + 真 E2E：

- commit-marker：sidecar 解析、flock、REGEN 四分支、recovery（truncate/backup/audit/fallback/fail-atomic）、tamper 检测、fs-check、retention、kill-switch
- 故障注入（adversarial，19 场景）：mid-append partial、REGEN 崩溃重试、sidecar 损坏/篡改、跨天 + seqno 单调
- **真 E2E**（`tests/e2e_tickfile_restart_recovery.py`）：real dataSimulator + real Engine + real ReplayEngine，**live+replay** 与 **live+live** 重启恢复各 3× PASS
- 数据驱动 watermark、per-minute snapshot、carry-forward 过滤、stall/double-flush、迟到记录、drain loop、streaming write、集成测试

---

## 相关文档

- 设计：[docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md](docs/superpowers/specs/2026-06-17-tickfile-commit-marker-truncate-recovery-design.md)（sidecar 终版，26 轮审阅）
- 实施计划：[docs/superpowers/plans/2026-06-18-tickfile-commit-marker-truncate-recovery.md](docs/superpowers/plans/2026-06-18-tickfile-commit-marker-truncate-recovery.md)
- 项目规划：[task_plan.md](task_plan.md) / [findings.md](findings.md) / [progress.md](progress.md)

## 许可

内部项目
