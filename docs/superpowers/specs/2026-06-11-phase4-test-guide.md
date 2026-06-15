# Phase 4 Test Guide — Rust Order Acceleration Validation

> **Date**: 2026-06-11
> **Status**: Test Guide — for Phase 4 validation execution
> **Parent Spec**: `docs/superpowers/specs/2026-06-10-rust-order-accel-design.md`
> **Prerequisites**: Phase 0-3 complete（Rust extension built, 389 tests passed, release build verified）

---

## Overview

Phase 4 验证 Rust order acceleration 的生产就绪性。所有测试必须使用 **release build** 的 Rust extension。

### Build Preparation

```bash
# 确保使用 release build
PYO3_PYTHON="/c/Users/rzpeng/anaconda3/python.exe" cargo build --release --manifest-path order_accel/Cargo.toml
cp order_accel/target/release/_order_accel.dll src/minute_bar/_order_accel.cp312-win_amd64.pyd

# 验证
python -c "from minute_bar._order_accel import is_available; print(is_available())"  # → True
```

---

## Test 1: Engine-Level Integration Test

### 目的
验证 engine.py 在 `enable_order_accel=true` 时完整运行 Rust path，包括：
- Rust parse → `_process_parsed_record` → batch-scoped state-lock → buffer → flush
- Cross-day flush 在 Rust path 下正常工作
- Late order detection 在 Rust path 下正常工作
- Seqno 在 date check 之后正确递增

### 测试代码

```python
# tests/test_order_accel.py — 新增测试类

import pytest
import threading
import time
from unittest.mock import patch, MagicMock
from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig
from minute_bar.models import OrderRecord

@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
class TestEngineRustIntegration:
    """Engine-level integration: exercise Rust path through _order_loop."""

    def _make_config(self, enable_accel=True):
        return AppConfig(
            input=InputConfig(
                csv_dir="/tmp/input",
                enable_order_accel=enable_accel,
                order_chunk_size_bytes=524288,
            ),
            output=OutputConfig(output_dir="/tmp/output", enable_order=True),
            recovery=RecoveryConfig(
                data_flush_delay_minutes=0,
                stall_flush_sec=300,
            ),
        )

    def test_rust_path_processes_records_correctly(self):
        """Verify Rust path through engine produces correct OrderRecords."""
        from minute_bar.engine import Engine, _OrderMinuteBuffer
        import tempfile
        import os

        # Create temp output dir
        with tempfile.TemporaryDirectory() as tmpdir:
            config = AppConfig(
                input=InputConfig(
                    csv_dir="/tmp/input",
                    enable_order_accel=True,
                ),
                output=OutputConfig(output_dir=tmpdir, enable_order=True),
                recovery=RecoveryConfig(data_flush_delay_minutes=0),
            )

            with patch("minute_bar.engine.ClockWatermarkFlusher"), \
                 patch("minute_bar.engine.CodeTable"), \
                 patch("minute_bar.engine.FileTailer"), \
                 patch("minute_bar.engine.CheckpointManager"):
                engine = Engine(config)

            # Simulate _process_parsed_record with a Rust-parsed record
            record = OrderRecord(
                symbol="7203", seqno=1, time=20260528090000123,
                bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200,
                decimal=2, rcvtime=0,
            )
            buffers = {}
            pending_shared_orders = []
            late_order_per_minute = {}
            late_dropped_per_minute = {}

            seqno, total_late, cur_date, cur_min = engine._process_parsed_record(
                record=record,
                today_str="20260528",
                seqno=0,
                minute_key="202605280900",
                buffers=buffers,
                current_date=None,
                current_minute=None,
                pending_shared_orders=pending_shared_orders,
                late_order_per_minute=late_order_per_minute,
                late_dropped_per_minute=late_dropped_per_minute,
                output_dir=tmpdir,
                total_late_dropped=0,
            )

            assert seqno == 1
            assert cur_date == "20260528"
            assert cur_min == "202605280900"
            assert "202605280900" in buffers
            assert len(buffers["202605280900"].records) == 1
            assert buffers["202605280900"].records[0].symbol == "7203"

    def test_rust_path_cross_day_flush(self):
        """Cross-day records trigger flush in Rust path."""
        from minute_bar.engine import Engine, _OrderMinuteBuffer

        with tempfile.TemporaryDirectory() as tmpdir:
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp/input", enable_order_accel=True),
                output=OutputConfig(output_dir=tmpdir, enable_order=True),
                recovery=RecoveryConfig(data_flush_delay_minutes=0),
            )

            with patch("minute_bar.engine.ClockWatermarkFlusher"), \
                 patch("minute_bar.engine.CodeTable"), \
                 patch("minute_bar.engine.FileTailer"), \
                 patch("minute_bar.engine.CheckpointManager"):
                engine = Engine(config)

            # Pre-fill buffer with day 20260528
            buffers = {"202605280900": _OrderMinuteBuffer()}
            buffers["202605280900"].records.append(
                OrderRecord("7203", 1, 20260528090000123, 4580000, 100, 4590000, 200, 2, 0)
            )

            # Process cross-day record (20260529)
            record = OrderRecord("7203", 2, 20260529090000123, 4600000, 100, 4610000, 200, 2, 0)

            seqno, total_late, cur_date, cur_min = engine._process_parsed_record(
                record=record,
                today_str="20260529",
                seqno=1,
                minute_key="202605290900",
                buffers=buffers,
                current_date="20260528",
                current_minute="202605280900",
                pending_shared_orders=[],
                late_order_per_minute={},
                late_dropped_per_minute={},
                output_dir=tmpdir,
                total_late_dropped=0,
            )

            assert cur_date == "20260529"
            # Cross-day flush should have cleared old buffers
            assert "202605280900" not in buffers
```

### 验证标准
- [ ] `test_rust_path_processes_records_correctly` PASS
- [ ] `test_rust_path_cross_day_flush` PASS
- [ ] 全量 389+ regression tests 仍 PASS

---

## Test 2: Corrupted .pyd Rollback Test

### 目的
验证当 `_order_accel.pyd` 损坏时，系统能正常降级到 Python fallback，不崩溃。

### 测试代码

```python
# tests/test_order_accel.py — 新增测试

def test_corrupted_pyd_rollback():
    """Verify system degrades gracefully when .pyd is corrupted."""
    import importlib
    import minute_bar.csv_parser as csv_mod

    # Simulate corrupted .pyd by reloading with broken import
    # Save original state
    original_available = csv_mod._RUST_ACCEL_AVAILABLE

    # Simulate ImportError scenario
    csv_mod._RUST_ACCEL_AVAILABLE = False
    csv_mod._RUST_ACCEL_LOADED = False

    # Verify fallback works
    assert not csv_mod.use_rust_accel()
    assert csv_mod.has_rust_accel() == False

    # Verify Python parsing still works
    from minute_bar.csv_parser import parse_order_record
    line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    record = parse_order_record(line, 1, "utf-8")
    assert record is not None
    assert record.symbol == "7203"

    # Restore
    csv_mod._RUST_ACCEL_AVAILABLE = original_available


def test_engine_startsWithoutRust():
    """Engine starts successfully when Rust accel is unavailable."""
    from minute_bar.engine import Engine
    from minute_bar.config import AppConfig, InputConfig, OutputConfig

    config = AppConfig(
        input=InputConfig(csv_dir="/tmp/input", enable_order_accel=False),
        output=OutputConfig(output_dir="/tmp/output"),
    )

    with patch("minute_bar.engine.ClockWatermarkFlusher"), \
         patch("minute_bar.engine.CodeTable"), \
         patch("minute_bar.engine.FileTailer"), \
         patch("minute_bar.engine.CheckpointManager"):
        engine = Engine(config)
        # Should NOT raise — enable_order_accel=false, graceful degradation
        assert engine is not None
```

### 验证标准
- [ ] `test_corrupted_pyd_rollback` PASS
- [ ] `test_engine_startsWithoutRust` PASS
- [ ] 确认 startup log 输出 "DISABLED" 状态

### Manual Rollback Test

```bash
# 1. 重命名 .pyd 模拟损坏
mv src/minute_bar/_order_accel.cp312-win_amd64.pyd src/minute_bar/_order_accel.cp312-win_amd64.pyd.bak

# 2. 验证 import fallback
python -c "
from minute_bar.csv_parser import _RUST_ACCEL_AVAILABLE, use_rust_accel
print(f'Rust available: {_RUST_ACCEL_AVAILABLE}')
print(f'use_rust_accel: {use_rust_accel()}')
"  # → Rust available: False, use_rust_accel: False

# 3. 运行 Python-only regression
python -m pytest tests/ -q --ignore=tests/test_e2e_tickfile_completeness.py --ignore=tests/test_order_accel.py -k "not test_rust" -x

# 4. 恢复 .pyd
mv src/minute_bar/_order_accel.cp312-win_amd64.pyd.bak src/minute_bar/_order_accel.cp312-win_amd64.pyd

# 5. 验证恢复
python -c "from minute_bar._order_accel import is_available; print(is_available())"  # → True
```

---

## Test 3: Concurrent Benchmark

### 目的
验证 order + snapshot 线程并发运行时，Rust acceleration 提供足够的 GIL 释放使 peak minute <60s。

### 测试配置

```ini
# config/test-concurrent-bench.ini
[input]
csv_dir = D:/FIU/test/output
target_date = 20260528
order_chunk_size_bytes = 524288
file_encoding = utf-8
enable_order_accel = true
poll_interval_ms = 1
idle_poll_interval_ms = 10
buffer_poll_interval_ms = 10

[output]
output_dir = D:/FIU/test/concurrent_bench_output
enable_order = true
enable_tickfile = false

[recovery]
output_delay_sec = 0
data_flush_delay_minutes = 0
enable_time_fallback = false
stall_flush_sec = 300
max_late_order_records_per_minute = 1000000

[logging]
log_level = INFO
```

### 测试步骤

```bash
# Step 1: 准备测试数据（data_simulator speed=100）
cd /d/FIU
PYTHONPATH=src python -m data_simulator \
    --speed 100 \
    --file-types order,snapshot,code \
    --date 20260528 \
    --source-dir input \
    --output-dir test/output

# Step 2: 运行 minute_bar with Rust accel enabled
PYTHONPATH=src python main.py --config config/test-concurrent-bench.ini

# Step 3: 观察日志
# 期望看到：
#   "Order acceleration: ENABLED (Rust _order_accel loaded, self-test passed)"
#   0900 分钟处理时间 <60s
#   无 "Order watermark stalled" 错误
```

### 验证标准

| 检查项 | 通过标准 |
|--------|----------|
| 0900 minute gap | **<60s**（wall-clock from first to last record of 0900 minute） |
| Order output files | 417 分钟文件（0800-1530） |
| Snapshot output files | 329 分钟文件 |
| Rust parse skip rate | <1%（check log "Rust parse batch: N lines, N skipped"） |
| "Order watermark stalled" | 0 occurrences |
| "Order acceleration: ENABLED" | 1 occurrence at startup |

### Sustained Load Benchmark（人工持续负载）

Spec Section 8.3 item 3 要求使用**人工持续负载**，不能仅依赖 speed=100 的 bursty 行为：

```python
# tests/test_order_accel_performance.py — 新增

import pytest
import threading
import time

@pytest.mark.slow
@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_concurrent_parse_sustained():
    """Simulate sustained concurrent order + snapshot parsing for 10 seconds."""
    from minute_bar._order_accel import parse_order_batch

    # Generate 100K lines (representing ~1 second of peak data)
    lines = []
    for i in range(100_000):
        sym = ['7203', '6501', '9984', '6758', '8306', '7974'][i % 6]
        time_val = 20260528090000000 + (i * 100) % 1000000
        lines.append(f'{sym},{time_val},{4500000+i},{100+i},{4600000+i},{200+i},2,{time_val-23}'.encode())

    # Simulate sustained parsing: both threads parse concurrently for 10 seconds
    results = {'order': [], 'snapshot': []}
    stop_event = threading.Event()

    def order_thread():
        count = 0
        while not stop_event.is_set():
            batch, _ = parse_order_batch(lines, 'utf-8')
            count += len(batch)
        results['order'] = count

    def snapshot_thread():
        # Snapshot lines are simpler (fewer fields, fewer records)
        snap_lines = lines[:1000]  # Snapshot has ~8x fewer records
        count = 0
        while not stop_event.is_set():
            batch, _ = parse_order_batch(snap_lines, 'utf-8')
            count += len(batch)
        results['snapshot'] = count

    t1 = threading.Thread(target=order_thread)
    t2 = threading.Thread(target=snapshot_thread)
    t1.start()
    t2.start()

    time.sleep(10)
    stop_event.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    # Order thread should achieve >100K records/s sustained
    order_throughput = results['order'] / 10.0
    print(f"Order throughput: {order_throughput:,.0f} rec/s")
    print(f"Snapshot throughput: {results['snapshot'] / 10.0:,.0f} rec/s")

    assert order_throughput > 100_000, f"Order throughput too low: {order_throughput:,.0f} rec/s"
```

---

## Test 4: E2E Performance Test

### 目的
完整的 speed=100 tickfile live E2E 测试，验证 Rust acceleration 不影响 tickfile 生成。

### 测试配置

```ini
# config/test-e2e-rust.ini
[input]
csv_dir = D:/FIU/test/output
target_date = 20260528
order_chunk_size_bytes = 524288
file_encoding = utf-8
enable_order_accel = true
poll_interval_ms = 1
idle_poll_interval_ms = 10
buffer_poll_interval_ms = 10

[output]
output_dir = D:/FIU/test/tickfile_live_output
enable_order = true
enable_tickfile = true

[recovery]
output_delay_sec = 0
data_flush_delay_minutes = 0
enable_time_fallback = false
stall_flush_sec = 30
max_late_order_records_per_minute = 1000000

[logging]
log_level = INFO
```

### 测试步骤

```bash
# Step 1: Clean previous output
rm -rf test/tickfile_live_output test/checkpoint_tickfile_json

# Step 2: Generate test data
PYTHONPATH=src python -m data_simulator \
    --speed 100 \
    --file-types order,snapshot,code \
    --date 20260528 \
    --source-dir input \
    --output-dir test/output

# Step 3: Run minute_bar with Rust accel + tickfile
PYTHONPATH=src python main.py --config config/test-e2e-rust.ini

# Step 4: Wait for completion, then validate
```

### 验证标准

| 检查项 | 通过标准 |
|--------|----------|
| Rust accel status log | "Order acceleration: ENABLED" |
| Order files | 417 分钟文件 |
| Snapshot files | 329 分钟文件 |
| Tickfile files | 存在且有内容 |
| 0900 order processing | <60s wall-clock |
| Tickfile seqno | 单调递增 |
| Rust skip rate | <1% |

### 与 Python-only 对比（可选）

```bash
# 用 Python-only path 运行同一数据
# 修改 config: enable_order_accel = false
# 对比 order CSV 内容 byte-identical

# Quick diff:
diff -rq test/tickfile_live_output_rust/order test/tickfile_live_output_python/order
# 期望: 无差异
```

---

## Test 5: Warmup Self-Test

### 目的
在 `Engine.__init__` 中添加 Rust extension warmup，确保 PyO3 return path 在启动时即被验证。

### 实现位置

```python
# engine.py — 在 startup log 之前添加

# Warmup: exercise Rust extension PyO3 return path
if _RUST_ACCEL_AVAILABLE:
    try:
        from minute_bar._order_accel import parse_order_batch
        warmup_lines = [b"7203,20260528090000123,4580000,100,4590000,200,2,0"] * 1000
        batch, skipped = parse_order_batch(warmup_lines, "utf-8")
        if len(batch) != 1000 or skipped != 0:
            logger.error(
                "Rust warmup self-test FAILED: got %d records, %d skipped (expected 1000, 0)",
                len(batch), skipped
            )
            from minute_bar.csv_parser import set_rust_available
            set_rust_available(False)
    except Exception as e:
        logger.error("Rust warmup self-test exception: %s: %s", type(e).__name__, e)
        from minute_bar.csv_parser import set_rust_available
        set_rust_available(False)
```

### 验证标准
- [ ] Warmup 在 startup log 之前执行
- [ ] Warmup 失败时 `_RUST_ACCEL_AVAILABLE` 设为 False
- [ ] Startup log 输出正确的状态（warmup 失败 → "DISABLED"，不是 "ENABLED"）

---

## Execution Checklist

| # | Test | Priority | Status |
|---|------|----------|--------|
| 1 | Engine-level integration test | P1 | ☐ |
| 2 | Corrupted .pyd rollback test | P1 | ☐ |
| 3 | Concurrent benchmark（sustained load） | P0 | ☐ |
| 4 | E2E performance test（speed=100 tickfile live） | P0 | ☐ |
| 5 | Warmup self-test | P2 | ☐ |

**建议执行顺序**: 1 → 2 → 5 → 3 → 4

- Test 1-2 和 5 是代码实现，可先完成
- Test 3 是 BLOCKING concurrent benchmark
- Test 4 是最终 E2E 验证

---

## Production Enablement Checklist

Phase 4 全部通过后：

```ini
# config/production.ini
[input]
enable_order_accel = true   ; ← 从 false 改为 true
```

验证步骤：
1. `enable_order_accel = true`
2. 重启服务
3. 确认 startup log 显示 "ENABLED"
4. 观察 0900 分钟处理时间
5. 确认无 "Rust parse_order_batch failed" WARNING
