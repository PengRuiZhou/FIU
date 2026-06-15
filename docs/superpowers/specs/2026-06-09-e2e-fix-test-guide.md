# Phase 19 E2E Fix 测试指引

> **Date**: 2026-06-09
> **Scope**: P0 Tickfile 缺失 + P1 Order 丢失修复验证
> **Fix Design**: `docs/superpowers/specs/2026-06-08-e2e-fix-design.md`

---

## 1. 快速验证：单元测试（~5 秒）

```bash
cd D:/FIU
python -m pytest tests/test_tickfile_sync.py tests/test_tickfile_bg_writer.py -v
```

**期望**: ~100 passed, 0 failed

### 关键测试类（Phase 19 新增/修改）

| 测试类 | 验证的 Fix | 检查要点 |
|--------|-----------|---------|
| `TestReroutePreservesTickfilePending` | Fix-A | reroute 后 `_tickfile_pending` 保留 |
| `TestReroutePreservesTickfilePendingThenGenerates` | Fix-A | reroute 后仍可正常生成 tickfile |
| `TestRerouteDoesNotMutatePendingData` | Fix-A | reroute 不改变 pending 数据内容 |
| `TestSkipLoggingPerMinuteKey` | Fix-B | 首次 skip → WARNING, 后续 → DEBUG |
| `TestCrossDayForceGenerationLogsFailures` | Fix-G | 跨日 force-generate 失败 → CRITICAL |
| `TestGeneratedTickfileMinutesPreventsDuplicateWrite` | Fix-F | 去重 guard 阻止第二次写入 |
| `TestNotSelectedMarksAsGenerated` | Fix-F | 无记录可选时也标记为已处理 |
| `TestIOErrorReinstallProtection` | Fix-F | IO 失败时检查 generated set |
| `TestOverflowDirectIOThenDrainSafe` | Fix-A2 + Fix-F | overflow + drain 不重复 |
| `TestGoldenPathTickfileUnchangedByFix` | 回归 | 正常路径不受影响 |
| `TestLateOrderCapDropLoggingFinalBatch` | Fix-E | final batch drop 统计日志 |
| `TestLateOrderCapConfigReadFromIni` | Fix-D | 从 ini 读取 cap 值 |
| `TestShutdownTickfileCompletenessCheck` | Fix-C | 3-layer shutdown check |

---

## 2. 全量回归测试（~5 秒）

```bash
cd D:/FIU
python -m pytest tests/ -v --ignore=tests/test_e2e_tickfile_completeness.py
```

**期望**: 375+ passed, 0 failed

> `test_order_drain` 可能有 1 个 pre-existing timing failure（与 Phase 19 无关）

---

## 3. E2E Live 测试（~8-10 分钟）

### 3.1 准备

```bash
# 确认输入数据存在
ls input/order.csv.20260528 input/snapshot.csv.20260528 input/code.csv.20260528
```

### 3.2 终端 1: 启动 minute_bar

```bash
cd D:/FIU
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini
```

### 3.3 终端 2: 启动 data_simulator

```bash
cd D:/FIU
PYTHONPATH=src python -m data_simulator \
  --source-dir input \
  --output-dir test/output \
  --speed 100 \
  --file-types order,snapshot,code \
  --date 20260528
```

### 3.4 等待完成

data_simulator 会输出写入进度，约 8-10 分钟完成全天数据。minute_bar 在 simulator 结束后继续处理残留 buffer，最终输出 `Engine stopped` 日志。

---

## 4. E2E 自动化验证

### 4.1 运行 E2E 测试

```bash
cd D:/FIU
E2E_OUTPUT_DIR=test/tickfile_live_output \
E2E_DATE=20260528 \
E2E_SOURCE_ORDER_COUNT=87521294 \
  python -m pytest tests/test_e2e_tickfile_completeness.py -v
```

### 4.2 期望结果

| 测试 | 验证内容 | 期望 |
|------|---------|------|
| `test_tickfile_minutes_match_order_and_snapshot` | tickfile 分钟数 == snapshot ∩ order 分钟数 | **PASSED** |
| `test_order_output_count_matches_source` | order 输出 == 源数据 87,521,294 | **PASSED** |

如果失败，查看日志中的具体 missing 分钟列表或 order count delta。

---

## 5. 日志检查清单

minute_bar 运行结束后，检查 `test/errors/` 日志和控制台输出：

### 5.1 ✅ 必须通过的检查

| # | 检查项 | 日志关键词 | 期望 |
|---|--------|-----------|------|
| 1 | Tickfile shutdown CHECK 3 | `Shutdown CHECK 3 PASS` | 出现且 PASS |
| 2 | 无 tickfile MISSING | `Shutdown CHECK 3 FAIL` | **不出现** |
| 3 | Tickfile generated 成功 | `Tickfile generated minute=` | 每分钟一条 |
| 4 | 无 order late cap drop | `Order late cap` | **不出现**（1M cap 足够） |
| 5 | cross-day 无 CRITICAL | `Cross-day CHECK` + `FAILED` | **不出现**（仅 `Cross-day cleared` 正常） |
| 6 | Tickfile 无 skip | `Tickfile skipped` | 可少量（double-enqueue 正常），但不应大量 |

### 5.2 ⚠️ 可接受的警告

| 日志 | 含义 | 行动 |
|------|------|------|
| `Tickfile skipped (already warned)` | double-enqueue 第二次 dequeue，正常 | 无需行动 |
| `Tickfile queue depth N exceeds warning` | 短暂积压，writer 会消化 | 观察，不超过 critical threshold |
| `Order watermark stalled` | 开盘 rush 导致短暂延迟，stall flush 会兜底 | 正常（0900 分钟 ~750K records） |
| `Tickfile generation slow` | 单分钟生成 >200ms | 非阻塞，writer 异步处理 |

### 5.3 ❌ 需要调查的错误

| 日志 | 可能原因 | 下一步 |
|------|---------|--------|
| `Shutdown CHECK 1 FAIL` | `_tickfile_pending` 在 flush_all_remaining 后仍有残留 | 检查是否有 IO error |
| `Shutdown CHECK 2 FAIL` | tickfile queue drain 未清空 | 检查 writer thread 是否正常退出 |
| `Shutdown CHECK 3 FAIL` | tickfile 缺失分钟 | 检查 Fix-A 是否正确应用 |
| `Order late cap FINAL` | cap 仍然不足 | 检查 ini 配置值 |
| `Cross-day CHECK ... FAILED` | 跨日 force-generate 失败 | 检查磁盘空间/权限 |

---

## 6. 手动数据完整性检查（可选）

### 6.1 Tickfile 分钟计数

```bash
# 统计 tickfile 中有数据行的分钟数
cd D:/FIU
python -c "
import csv
from pathlib import Path

tf = Path('test/tickfile_live_output/tickfile/2026/20260528/tickfile_20260528.csv')
if not tf.exists():
    print('Tickfile not found'); exit(1)

with open(tf, encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    time_idx = header.index('UpdateTime')
    minutes = set()
    for row in reader:
        if len(row) > time_idx:
            minutes.add(row[time_idx].split(' ')[1].replace(':', '')[:4])
print(f'Tickfile minutes: {len(minutes)}')
print(f'Range: {min(minutes)} - {max(minutes)}')
"
```

**期望**: ~329 分钟（取决于 snap_mins ∩ order_mins）

### 6.2 Order 文件总记录数

```bash
cd D:/FIU
python -c "
from pathlib import Path
total = 0
for f in sorted(Path('test/tickfile_live_output/order').rglob('*.csv')):
    with open(f, encoding='utf-8') as fh:
        first = True
        for line in fh:
            if first: first = False
            else: total += 1
print(f'Total order records: {total}')
"
```

**期望**: 87,521,294（与源数据完全一致）

### 6.3 Snapshot 文件数

```bash
ls test/tickfile_live_output/snapshot/2026/20260528/*.csv | wc -l
```

**期望**: 329

---

## 7. Troubleshooting

### 问题: `Tickfile not found`

**原因**: minute_bar 可能未开启 tickfile 模式
**检查**: `config/test-tickfile-live.ini` 中 `enable_tickfile = true`

### 问题: E2E test 报 `Snapshot directory not found`

**原因**: minute_bar 未输出到期望目录
**检查**: `E2E_OUTPUT_DIR` 环境变量与 `config/test-tickfile-live.ini` 的 `output_dir` 一致

### 问题: `Order count mismatch`

**可能原因**:
1. data_simulator 未完整写入（检查 simulator 日志最后一行）
2. `max_late_order_records_per_minute` 仍不足（检查 ini 配置）
3. minute_bar 未正常 shutdown（检查 `Engine stopped` 日志）

### 问题: `Tickfile skipped` 大量出现

**可能原因**:
1. Fix-A 未正确应用（`_tickfile_pending` 仍被 reroute pop）
2. 验证方法: 在 `test_tickfile_sync.py` 中运行 `TestReroutePreservesTickfilePending`

---

## 8. 命令速查

```bash
# 单元测试
python -m pytest tests/test_tickfile_sync.py tests/test_tickfile_bg_writer.py -v

# 全量回归
python -m pytest tests/ -v --ignore=tests/test_e2e_tickfile_completeness.py

# E2E live test — 终端 1
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini

# E2E live test — 终端 2
PYTHONPATH=src python -m data_simulator --source-dir input --output-dir test/output --speed 100 --file-types order,snapshot,code --date 20260528

# E2E 自动化验证
E2E_OUTPUT_DIR=test/tickfile_live_output E2E_DATE=20260528 E2E_SOURCE_ORDER_COUNT=87521294 \
  python -m pytest tests/test_e2e_tickfile_completeness.py -v
```
