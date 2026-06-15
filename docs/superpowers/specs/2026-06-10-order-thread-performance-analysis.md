# Order Thread Performance Analysis & GIL Bottleneck

> **Date**: 2026-06-10
> **Status**: Phase 1 Complete — Python optimization applied, GIL bypass needed for production requirement
> **Related**: Phase 19 E2E Fix (`2026-06-09-e2e-fix-design.md`)

---

## 1. Problem Statement

**Requirement**: 每分钟 order 数据必须在 1 分钟内生成（order CSV file written within 60s of minute end）

**Observed**: At speed=100 E2E test, peak minutes (0900: 737K records) take **147-183 seconds** to generate, exceeding the 60s limit by 2-3x.

---

## 2. Root Cause Analysis

### 2.1 Performance Profile (87.5M records benchmark, 564s total)

```
Parse + Build   ████████████████████████████████████████  83.6%   ← CPU hot path
Lock (state)    █████                                    9.5%
Read (tailer)   ███                                      6.3%
Drain tickfile  █                                        0.6%
```

### 2.2 GIL Contention: The Hard Bottleneck

minute_bar runs **6 Python threads** simultaneously:

| Thread | Role | CPU Pattern |
|--------|------|-------------|
| order-thread | Parse + build + write order files | **CPU-bound** (parse loop) |
| snapshot-thread | Parse + aggregate + write snapshots | **CPU-bound** (parse loop) |
| flusher-thread | Data-driven flush + tickfile enqueue | Mixed |
| tickfile-writer | Generate tickfile rows | Mixed |
| clock-thread | Time checks | Mostly idle |
| main | Coordination | Mostly idle |

**Key finding**: order-thread and snapshot-thread are **both CPU-bound**, competing for the GIL. Python's GIL allows only one thread to execute Python bytecode at a time. Even with optimized parse code, the order thread can only obtain ~50% of CPU time during peak minutes.

### 2.3 Multi-Thread Throughput Degradation

| Scenario | Throughput | vs Single-Thread |
|----------|-----------|-----------------|
| Single-thread benchmark | 155K → 808K rec/s | 1.0x → 5.2x (after optimization) |
| Multi-thread (E2E) | 11K → 8.4K rec/s | **14-18x degradation** from GIL |

The degradation factor is consistent regardless of parse optimization — it's a function of thread count and CPU work ratio, not per-record speed.

### 2.4 Why Small Minutes Are Fine But Peak Minutes Fail

- **Small minutes** (0800-0859, 1K-72K records): parse+write completes in <1s, well within GIL time slices
- **Peak minutes** (0900-0910, 230K-747K records): parse+write needs sustained CPU time that GIL sharing cannot provide

Production peak: 0900 = 737K records in 60 seconds = **12.3K rec/s arrival rate**.
Multi-thread order throughput: **~8.4K rec/s** — below the arrival rate.

---

## 3. Python Optimization Applied (2026-06-10)

### 3.1 Changes

| File | Change | Rationale |
|------|--------|-----------|
| `src/minute_bar/models.py` | `OrderRecord`: `@dataclass(frozen=True)` → `NamedTuple`, `float` → `int` | 1.59x faster creation, no float roundtrip |
| `src/minute_bar/csv_parser.py` | Added `parse_order_record()`: bytes → OrderRecord in one call | Eliminates ParsedOrder + strip() + float() |
| `src/minute_bar/engine.py` | Order loop uses `parse_order_record` instead of `parse_order_line` + `build_order_record` | Skip intermediate objects |
| `src/minute_bar/tickfile.py` | `str(int(round(order.bidsize)))` → `str(order.bidsize)` | Already int, no roundtrip |
| `src/minute_bar/aggregator.py` | `float(parsed.bidprice)` → `parsed.bidprice` | Keep as int |
| `tests/test_tickfile.py` | Float literals → int literals | Match new type |
| `tests/test_order_drain.py` | Mock `parse_order_line` → mock `parse_order_record` | Match new call site |

### 3.2 Benchmark Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Single-thread parse+build | 251K rec/s (6.0μs/rec) | 808K rec/s (1.2μs/rec) | **3.22x** |
| Small minutes (0800-0859) | ~11K rec/s | **54.8K rec/s** | **5.0x** |
| Peak minute 0900 (747K rec) | 183s | 147s | 1.2x |
| Regression tests | 379 passed | 379 passed | ✅ No regressions |

### 3.3 Conclusion

Python optimization **dramatically improves** single-thread and small-minute performance, but **cannot solve** the GIL contention problem for peak minutes. The GIL is a fundamental CPython limitation that no amount of Python code optimization can overcome.

---

## 4. Next Steps: GIL Bypass Strategies

### 4.1 Option A: Rust/PyO3 Extension (Recommended)

Write `parse_order_record` + `write_order_file` as a Rust C extension using [PyO3](https://pyo3.rs).

**How it bypasses GIL**:
```rust
// In Rust, explicitly release the GIL during CPU work
fn parse_batch(py: Python, lines: Vec<&[u8]>) -> PyResult<Vec<OrderRecord>> {
    py.allow_threads(|| {
        // This code runs WITHOUT holding the GIL
        // Other Python threads can run freely
        lines.iter().map(|line| parse_line_fast(line)).collect()
    })
}
```

**Expected performance**:
- Parse: 5-10M rec/s (Rust native speed)
- write_order_file: Release GIL during format+write loop
- Multi-thread order throughput: **500K+ rec/s** (no GIL contention)

**Requirements**: `cargo`, `maturin` or `setuptools-rust`, PyO3 bindings

**Estimated effort**: 2-3 days

### 4.2 Option B: multiprocessing

Move order parse+build to a separate process. Communicate via `multiprocessing.Queue` or shared memory.

**Pros**: Pure Python, no new toolchain
**Cons**: Serialization overhead (pickle ~10μs/record), architecture refactor needed
**Estimated effort**: 1-2 days

### 4.3 Option C: Cython/C Extension

Write the hot path in Cython or C with explicit GIL release (`with nogil:`).

**Pros**: Simpler than Rust, good Python integration
**Cons**: Less safe than Rust, similar effort
**Estimated effort**: 1-2 days

---

## 5. Production Impact Assessment

At production speed (speed=1), the data arrival rate is:
- Peak: 737K records / 60s = **12.3K rec/s** (0900 minute)
- Average: 87.5M / 27000s = **3.2K rec/s**

With current Python optimization:
- Small minutes: ✅ **54.8K rec/s** >> 12.3K (4.5x margin)
- Peak minutes: ❌ **8.4K rec/s** < 12.3K (0.68x — insufficient)

With GIL bypass (Rust):
- Peak minutes: ✅ **500K+ rec/s** >> 12.3K (40x+ margin)

---

## 6. Key Benchmark Commands

```bash
# Single-thread benchmark
cd D:/FIU && PYTHONPATH=src python -c "
from minute_bar.csv_parser import parse_order_record
import time
sample = b'7203,20260528090000123,4580000,100,4590000,200,2,20260528090000100'
N = 500000
start = time.perf_counter()
for i in range(N): parse_order_record(sample, i)
print(f'{N/(time.perf_counter()-start):,.0f} rec/s')
"

# Full E2E test (no simulator — data already available)
cd D:/FIU && rm -rf test/tickfile_live_output test/checkpoint_tickfile.json
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini
```
