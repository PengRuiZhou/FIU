# Rust Order Parse Extension Design — GIL Bypass for Peak Minutes

> **Date**: 2026-06-10
> **Status**: Design — revised after two rounds of 3-agent review
> **Parent Spec**: `2026-06-10-order-thread-performance-analysis.md`
> **Approach**: A — Rust/PyO3 batch parse extension

---

## 1. Problem Summary

Order thread peak-minute performance (0900: 747K records) takes **147s**, exceeding the 60s requirement.
Root cause: Python GIL contention between order-thread and snapshot-thread.
Parse+build consumes **83.6%** of CPU time while holding the GIL.

## 2. Solution Overview

Move the order parse hot path to a **Rust native extension** that releases the GIL during batch parsing.
Keep all other logic (buffering, locking, minute detection, file writing) in Python.

**Expected result**: parse GIL time drops from ~0.9s (current optimized Python) to ~0.06s (Rust) per peak minute → peak minute completes in **<10s** under concurrent load (conservative estimate, validated by mandatory benchmark).

> **Note on estimates**: All performance estimates are single-thread component timings. A concurrent benchmark (order thread + snapshot thread running simultaneously) MUST be run to validate the actual wall-clock improvement. See Section 8.1 for the per-batch math breakdown.

---

## 3. Architecture

```
Order Thread (Python)
│
├── tailer.read_lines() → list[bytes] (batch varies by order_chunk_size_bytes)
│
├── Rust: parse_order_batch(lines, encoding) ← GIL RELEASED (~0.56ms/6400 lines)
│   │  • decode each line (UTF-8 only)
│   │  • split by ","
│   │  • trim whitespace, parse int fields
│   │  • skip header/invalid lines
│   │  • return Vec<8-tuple> (NO seqno — Python assigns it)
│   │
│
├── Python: for each parsed 8-tuple ← GIL held
│   │  • date check FIRST: record.time // 1_000_000_000 vs today_int
│   │  • seqno += 1 only AFTER date check passes (matches engine.py)
│   │  • OrderRecord(symbol=fields[0], seqno=seqno, time=fields[1], ...)
│   │  • minute_key, buffer append, state lock
│   │  • ~5-8ms per 6400 records (estimated)
│
├── Python: write_order_file (UNCHANGED — streaming write) ← GIL interleaved
│   │  • Current streaming write releases GIL during I/O syscalls
│   │  • NO batch join in this PR — see Section 5.3
```

**Recommended config when Rust accel enabled**: `order_chunk_size_bytes = 524288` (yields ~6400 lines/batch, ~117 batches for 747K records). Reduces GIL re-acquire points from 934 to 117 (8x fewer preemption opportunities).

Snapshot thread runs freely during Rust parse phase (GIL released).

### 3.1 GIL Coverage Map

| Phase | GIL State | Estimated Time (747K rec peak) | Notes |
|-------|-----------|-------------------------------|-------|
| `tailer.read_lines()` | Interleaved | ~100ms | I/O releases GIL during syscall; line splitting is GIL-held |
| **Input conversion** (`list[bytes]` → `Vec<Vec<u8>>`) | Held | ~37-50ms | PyO3 copies ~60MB across 117 batches. NOTE: `PyBackedBytes` is NOT viable here because it borrows Python references which are `!Send` — incompatible with `allow_threads()`. Copy is unavoidable. |
| Rust `parse_order_batch()` | **Released** | ~65ms | 117 batches × ~0.56ms (524288 chunk, recommended) |
| **GIL handoff overhead** (117 batch boundaries) | Held | **0-585ms** | 117 batches × 0-5ms per GIL re-acquire (CPython default `sys.getswitchinterval()` = 5ms). Worst-case: snapshot thread holds GIL at each batch boundary. |
| PyO3 return conversion | Held | ~350-600ms | 747K tuples × 8 elements → **5.97M Python objects** (microbenchmark required, see §8.3) |
| `OrderRecord` construction + date check | Held | ~400-600ms | 747K NamedTuple + `today_int` comparison |
| `time_to_minute_key` (integer division + str) | Held | **~75-150ms** | 747K × `str(record.time // 100_000)` — eliminates one of two string allocations (no intermediate 17-char string). Down from ~150-300ms with `str(int)` + slice. |
| Buffer append + state lock | Held | ~200-300ms | 747K buffer appends + 117 lock acquisitions |
| `write_order_file` (unchanged) | Held | ~200-400ms (**~90-95% GIL-held**) | Streaming write; GIL released only during C buffer flush (~every 12K records). Most `f.write()` calls are GIL-held memcpy into C buffer. GIL-held portion: ~180-380ms. |
| GC overhead (Python cyclic GC) | Held | ~60-250ms | ~7.2M total Python object allocations trigger ~10K gen0 scans at default threshold 700. Reduced from 8.7M by integer `time_to_minute_key`. Consider `gc.disable()` during batch loop (see §5.2). |
| **Total GIL-held** | — | **~1,302-2,590ms** | Down from ~5,600ms GIL-held in pure Python. Includes write GIL-held + GC + handoff overhead |
| **Total Python object allocations** | — | **~7.9M objects** | 5.97M PyO3 + 747K NamedTuple + 747K minute_key strings + 747K buffer + misc (reduced from 8.7M by eliminating intermediate str(int)) |
| **Total wall-clock (est.)** | — | **~1,390-2,680ms** | Single-thread compute; concurrent wall-clock ~2-4x with GIL sharing → ~3-10s |

> **Contention model**: With 2 CPU-bound threads (order + snapshot), each gets ~50% of GIL time. Wall-clock multiplier is ~2-4x, NOT the 18x throughput degradation factor. The 18x was measured as records/second degradation, not wall-clock time expansion. Actual wall-clock under concurrent load: **~3-8s** (well within 60s SLA). **Must be validated by mandatory concurrent benchmark** (Section 8.3 item 3).
>
> **⚠️ speed=100 vs speed=1 contention**: At speed=100, data arrives in concentrated bursts where order/snapshot threads may not overlap significantly. At production speed=1, both threads run continuously at ~12K rec/s for 60s, creating **sustained** GIL contention. The concurrent benchmark (Section 8.3 item 3) must use **artificial sustained load** (both threads parsing simultaneously), not rely solely on speed=100 E2E burst behavior.

---

## 4. Rust Extension API

### 4.1 File Structure

```
order_accel/
├── Cargo.lock       ← COMMIT THIS for reproducible builds
├── Cargo.toml
├── rust-toolchain.toml
└── src/
    └── lib.rs
```

**rust-toolchain.toml** (pins Rust version for reproducibility):

```toml
[toolchain]
channel = "1.84"
components = ["rustfmt", "clippy"]
```

### 4.2 Cargo.toml

```toml
[package]
name = "order-accel"
version = "0.1.0"
edition = "2021"
rust-version = "1.84"

[lib]
name = "_order_accel"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.23.0", features = ["extension-module"] }  # Pin to patch version for reproducible builds

[profile.release]
panic = "abort"  # Terminate process on panic — safer than undefined state for financial data
```

> **Why PyO3 0.23+**: PyO3 0.22 has known issues with Python 3.12+ in edge cases. 0.23+ has better performance and broader Python version support. The `Bound<'_, PyModule>` API used in the code requires PyO3 0.21+ which is satisfied by 0.23.

### 4.3 lib.rs — Public API

```rust
use pyo3::prelude::*;

/// Parse a batch of raw order CSV lines into 8-tuples (NO seqno).
///
/// Returns: (Vec<(symbol: str, time: i64, bidprice: i64, bidsize: i64,
///            askprice: i64, asksize: i64, decimal: i64, rcvtime: i64)>,
///           skipped_count: u64)
///
/// IMPORTANT: seqno is NOT included in the return value.
/// Python assigns seqno sequentially for each valid record,
/// matching the current engine.py behavior where seqno increments
/// only for valid records that pass all filters (including date check).
///
/// Invalid lines (header, decode error, parse error) are silently skipped.
/// The skip count is returned separately for observability.
/// GIL is released for the entire parsing phase.
///
/// MAX_BATCH_SIZE: Prevent OOM from pathological inputs. If exceeded, raises
/// PyValueError — Python engine.py can catch this and fall back to per-line parsing.
const MAX_BATCH_SIZE: usize = 1_000_000;

#[pyfunction]
fn parse_order_batch(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<(String, i64, i64, i64, i64, i64, i64, i64)>, u64)> {
    // Guard: reject oversized batches to prevent OOM (panic=abort would kill process)
    if lines.len() > MAX_BATCH_SIZE {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Batch too large: {} lines (max {})", lines.len(), MAX_BATCH_SIZE)
        ));
    }

    // IMPORTANT: Convert encoding to owned String BEFORE allow_threads.
    // The `encoding: &str` parameter borrows from a Python str object,
    // which is !Send. The allow_threads closure requires Send, so we must
    // create an owned copy that can be sent across the GIL boundary.
    let encoding_owned = encoding.to_string();

    // Phase 1: Parse all lines WITHOUT GIL (the hot path)
    let (parsed, skipped): (Vec<_>, u64) = py.allow_threads(|| {
        let mut skipped: u64 = 0;
        let results: Vec<(String, i64, i64, i64, i64, i64, i64, i64)> = lines
            .iter()
            .filter_map(|line| {
                match parse_one_line(line, &encoding_owned) {
                    Some(t) => Some(t),
                    None => {
                        skipped += 1;
                        None
                    }
                }
            })
            .collect();
        (results, skipped)
    });

    // Phase 2: Return parsed results + skip count (GIL held, but trivial)
    Ok((parsed, skipped))
}

/// Check if the Rust extension is available and functional.
/// Performs a minimal self-test by parsing one known-good line.
#[pyfunction]
fn is_available() -> bool {
    // Self-test: parse a known-good line
    let test_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
    parse_one_line(test_line, "utf-8").is_some()
}

/// Parse a single line. Returns None for headers, invalid, or unparseable lines.
///
/// Field parsing uses .trim() to match Python int()'s whitespace tolerance.
/// All numeric fields are i64 (including time) to match Python's arbitrary-precision int.
/// Only UTF-8 encoding is supported. Non-UTF-8 data will cause the line to be skipped.
fn parse_one_line(
    line: &[u8],
    encoding: &str,
) -> Option<(String, i64, i64, i64, i64, i64, i64, i64)> {
    // Decode — UTF-8 only. Non-UTF-8 encodings return None immediately.
    // The caller (engine.py) should check encoding before calling Rust,
    // but this is defense-in-depth.
    let line_str = match encoding {
        "utf-8" | "utf8" => std::str::from_utf8(line).ok()?,
        _ => return None, // Non-UTF-8: do NOT attempt decode, return None
    };

    // Strip trailing \r (CR-only line endings from Windows tailer).
    // For CRLF (\r\n): strip_suffix('\r') won't match, but trim() on the
    // last numeric field strips \r\n anyway. This defense-in-depth handles
    // both CR-only and CRLF cases.
    let line_str = line_str.strip_suffix('\r').unwrap_or(line_str);

    // Skip header — must match Python parse_order_record exactly:
    // Python checks only lowercase "symbol,", so Rust must not check more.
    if line_str.starts_with("symbol,") {
        return None;
    }

    // Skip empty lines
    if line_str.is_empty() {
        return None;
    }

    // Split — use stack-allocated array for 6-8 fields
    let fields: Vec<&str> = line_str.split(',').collect();
    let n = fields.len();
    if n < 6 || n > 8 {
        return None;
    }

    // Skip empty/whitespace-only symbol. This is a PRE-EXISTING bug fix:
    // parse_order_line (slow path) correctly rejects empty symbols,
    // but parse_order_record (hot path) does NOT. The Python hot path
    // must ALSO add this check as a blocking prerequisite (Phase 0 step 2).
    // Using trim() to reject whitespace-only symbols like "   " as well.
    if fields[0].trim().is_empty() {
        return None;
    }

    // Validate time field is 17-digit (production timestamps are always
    // 17 digits like 20260528090000123). Non-17-digit values would cause
    // the date extraction to diverge between str[:8] and //1_000_000_000.
    let time_val = fields[1].trim().parse::<i64>().ok()?;
    if !(10_000_000_000_000_000..100_000_000_000_000_000).contains(&time_val) {
        return None; // Reject malformed timestamp
    }

    // Parse fields with trim() to match Python int() whitespace tolerance
    Some((
        fields[0].to_string(),                              // symbol
        time_val,                                              // time (validated 17-digit)
        fields[2].trim().parse::<i64>().ok()?,               // bidprice
        fields[3].trim().parse::<i64>().ok()?,               // bidsize
        fields[4].trim().parse::<i64>().ok()?,               // askprice
        fields[5].trim().parse::<i64>().ok()?,               // asksize
        if n > 6 { fields[6].trim().parse::<i64>().ok()? } else { 2 },  // decimal
        if n > 7 { fields[7].trim().parse::<i64>().ok()? } else { 0 },  // rcvtime
    ))
}

#[pymodule]
fn _order_accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_order_batch, m)?)?;
    m.add_function(wrap_pyfunction!(is_available, m)?)?;
    Ok(())
}
```

### 4.4 Key Design Decisions (post-review fixes)

| Decision | Rationale |
|----------|-----------|
| **Seqno NOT in Rust return** | Python assigns seqno sequentially for valid records only, matching `engine.py` behavior where `seqno += 1` happens only after `parse_order_record` succeeds AND date check passes. |
| **Seqno increment AFTER date check** | In engine.py Rust path, `seqno += 1` runs after date filter, exactly matching the current Python behavior. This prevents seqno drift when cross-day records are present. |
| **`time` field is `i64`, not `u64`** | Python `int()` accepts negative values; Rust `u64` does not. Using `i64` matches Python semantics. |
| **17-digit timestamp validation** | Rust validates `10^16 <= time < 10^17` before accepting. Non-17-digit timestamps would cause date extraction divergence between `str[:8]` and `//10^9`. |
| **`.trim()` on all numeric fields** | Python `int(" 100")` = 100; Rust `" 100".parse::<i64>()` fails. `.trim()` ensures parity. |
| **`.strip_suffix('\r')` + trim()** | Defense-in-depth for line endings: `strip_suffix('\r')` handles CR-only; `trim()` on last numeric field handles CRLF (\r\n). Note: FileTailer already strips `\r` before passing to parser — Rust handles this for non-tailer data sources. |
| **`encoding` converted to owned `String`** | `encoding: &str` borrows from Python `str` (!Send). `allow_threads()` requires `Send` closure. Must convert to `encoding.to_string()` before entering the closure. |
| **Encoding guard uses `.lower()`** | ConfigParser does not lowercase values. Guard `encoding.lower() in ("utf-8", "utf8")` prevents "UTF-8" from silently disabling Rust. |
| **try/except around Rust batch in engine.py** | If `_rust_parse_batch` raises (e.g., MAX_BATCH_SIZE exceeded), fall back to Python per-line parsing for that batch. No data loss. |
| **Shared `_process_parsed_record` function** | ALL paths (Rust success, Rust fallback, Python) feed records into ONE shared per-record function. Prevents 3-copy divergence for cross-day flush, late-order, tickfile triggers, buffer management. |
| **Rust path record_date uses int for check, str for downstream** | `fields[1] // 10^9` (int) for date check; `str(record.time)[:8]` (str) in shared processing function. Ensures type consistency with existing cross-day flush logic (which uses string comparison). |
| **Flat binary `'<H>'` (unsigned short) for symbol length** | Rust `u16` → Python `'<H>'` (unsigned). Previous `<h>` (signed) was a type mismatch. |
| **Empty symbol check — BLOCKING PREREQUISITE** | Python `parse_order_record` must be fixed FIRST (step 1 of implementation). Both Rust and Python must skip empty symbols. This is not optional. |
| **Only lowercase "symbol," header check** | Python `parse_order_record` only checks `startswith("symbol,")` (lowercase). Rust must match exactly — no uppercase "Symbol," check. |
| **Encoding: return None immediately for non-UTF-8** | Rust returns `None` for any non-UTF-8 encoding without attempting decode. This prevents garbled data from Shift-JIS or other encodings. |
| **Encoding guard at call site** | Engine.py checks `encoding in ("utf-8", "utf8")` before calling Rust. Non-UTF-8 encodings fall back to Python path automatically. |
| **Skip count returned** | Returns `(Vec<tuple>, skipped_count)` for observability. Production monitoring can log unexpected skip rates. |
| **`today_int` computed from config** | `today_int = int(self._get_target_date())` computed once per outer loop for Rust path. Integer comparison `record.time // 1_000_000_000` avoids string allocation. |
| **Python fallback is EXACT original code** | The `else` branch in engine.py uses the original `str(record.time)[:8]` string comparison. No semantic changes in fallback path. |
| **`is_available()` self-test** | Parses one known-good line to verify runtime functionality, not just "module loaded". |
| **`panic = "abort"` in Cargo.toml** | For financial data system, process termination on panic is safer than undefined Python state. |
| **`enable_order_accel` defaults to `False`** | Prevents startup crash on existing deployments without Rust. Must explicitly opt-in. |
| **Fix pyproject.toml build-backend FIRST** | Current `setuptools.backends._legacy:_Backend` does NOT exist in setuptools 82+ (empirically verified). Must change to `setuptools.build_meta:__legacy__` as Phase 0 step 1. |
| **Do NOT add setuptools-rust to build-system requires** | Would force setuptools-rust installation on machines without Rust, causing build failures. Use optional manual install instead. |
| **time_to_minute_key uses integer division + str** | `str(record.time // 100_000)` produces same result as `str(time)[:12]` for 17-digit timestamps, eliminating one of two string allocations per record (~75-150ms GIL-held savings). Still returns `str` — downstream code (writer.py, flusher.py, tickfile.py) all expect `str` minute keys and perform slicing. |
| **gc.disable() during batch loop** | Disables Python cyclic GC during batch processing to amortize ~10K gen0 scans. Re-enables and collects after each minute's batches complete. Reduces unpredictable GC pauses under GIL contention. **Note**: `gc.disable()` is process-wide — snapshot/clock/tickfile threads also lose automatic GC during the disable window. The `try/finally` ensures timely re-enablement (~2-3s window per peak minute). |
| **GIL handoff overhead modeled** | 117 batch boundaries × 0-5ms per GIL re-acquire = 0-585ms worst-case. Included in GIL coverage map and per-batch math. |
| **write_order_file ~90-95% GIL-held** | C buffer only flushes every ~12K records. Most `f.write()` calls are GIL-held memcpy. GIL-held portion: ~180-380ms, not ~187ms. |
| **enable_order_accel in [input] section** | Rust acceleration is a parse feature, co-located with `order_chunk_size_bytes` and `file_encoding` in `[input]` config section. |
| **setup.py warns on missing setuptools-rust** | When `order_accel/` exists but `setuptools_rust` not installed, prints warning to stderr instead of silently skipping. |
| **Memory budget ~230-410MB peak** | Must validate with `tracemalloc` in Phase 3. Components: NamedTuples ~80MB + buffer ~107MB + GC promotions ~20-100MB. |
| **Concurrent benchmark uses sustained load** | Speed=100 creates bursty contention; production speed=1 creates sustained contention. Concurrent benchmark must use artificial sustained load, not rely solely on speed=100 E2E. |

---

## 5. Python Integration

### 5.1 csv_parser.py — Import with Fallback + Startup Log

```python
# At module level:
import logging
logger = logging.getLogger(__name__)

_RUST_ACCEL_LOADED = False
_RUST_ACCEL_AVAILABLE = False

try:
    from minute_bar._order_accel import parse_order_batch as _rust_parse_batch
    from minute_bar._order_accel import is_available as _rust_available
    _RUST_ACCEL_LOADED = True
    _RUST_ACCEL_AVAILABLE = _rust_available()
except (ImportError, AttributeError, RuntimeError, OSError) as e:
    _RUST_ACCEL_AVAILABLE = False
    logger.warning("Rust order accel not available, using Python fallback: %s: %s", type(e).__name__, e)


def has_rust_accel() -> bool:
    """Check if Rust acceleration is available AND enabled."""
    return _RUST_ACCEL_AVAILABLE


def use_rust_accel(config=None) -> bool:
    """Check if Rust acceleration should be used based on availability and config."""
    if not _RUST_ACCEL_AVAILABLE:
        return False
    if config is not None and not getattr(config.input, 'enable_order_accel', False):
        return False
    return True
```

### 5.2 engine.py — Order Loop Integration

```python
# Before (current):
for line in lines:
    record = parse_order_record(line, seqno + 1, encoding)
    if record is None:
        continue
    record_date = str(record.time)[:8]
    if record_date != today:
        continue
    seqno += 1  # seqno increments ONLY after date check passes
    minute_key = time_to_minute_key(record.time)
    ...

# After:
# NOTE: Compute both str and int versions of today's date ONCE per outer loop.
# Guard runs BEFORE the Rust/Python branch to protect BOTH paths.
today = self._get_target_date()  # str "YYYYMMDD" — used by downstream comparisons
if not (today and len(today) == 8 and today.isdigit()):
    raise RuntimeError(f"Invalid target date: {today!r}")
today_int = int(today)  # int — used by Rust date check only

# ARCHITECTURE: Shared per-record processing function.
# ALL paths (Rust success, Rust fallback, original Python) feed parsed
# records into ONE shared function. This prevents logic divergence for
# cross-day flush, late-order detection, buffer management, tickfile triggers.
# The `...` in the original design has been replaced by this shared function.
#
# EXPLICIT PARAMETER LIST (highest-risk refactoring point):
# - record: OrderRecord
# - today_str: str — "YYYYMMDD" from _get_target_date()
# - seqno: int — current seqno (returned as updated value)
# - minute_key: str — from str(record.time // 100_000)
# - buffers: dict[str, _OrderMinuteBuffer] — per-minute record buffers
# - current_date: Optional[str] — tracks date for cross-day detection
# - current_minute: Optional[str] — tracks minute for flush triggers
# - pending_shared_orders: list[tuple[str, OrderRecord]] — tickfile queue (APPENDED to, not processed)
# - late_order_per_minute: dict[str, int] — late order counter per minute
# - late_dropped_per_minute: dict[str, int] — late order drop counter
# - output_dir: str — order CSV output directory
# - total_late_dropped: int — running total (returned as updated)
# Returns: (seqno, total_late_dropped, current_date, current_minute)
#
# IMPORTANT: This function handles per-record logic ONLY:
#   - Cross-day flush (engine.py lines 681-700)
#   - Late-order detection + append (lines 705-725)
#   - Record-driven flush (lines 727-731)
#   - Buffer append (lines 733-741)
#   - Watermark update (lines 743-746)
#   - pending_shared_orders APPEND (line 741 — append only, no lock)
#
# BATCH-SCOPED logic that must remain OUTSIDE this function:
#   - pending_shared_orders state-lock processing (lines 748-763):
#     This acquires self._state.lock for ALL accumulated orders.
#     Placing it inside per-record function would cause 747K lock
#     acquisitions instead of ~117. It MUST be called once per batch.
#   - drain_count increment (line 666): incremented once per batch, not per record
#   - Periodic flush check (lines 767-782): runs once per batch
#
def _process_parsed_record(self, record, today_str, seqno, minute_key,
                           buffers, current_date, current_minute,
                           pending_shared_orders, late_order_per_minute,
                           late_dropped_per_minute, output_dir,
                           total_late_dropped):
    """Shared per-record processing: cross-day, late-order, buffer, watermark.

    NOTE: pending_shared_orders is APPENDED to (line 741 equivalent) but
    the state-lock processing (lines 748-763) is NOT included here.
    The caller must process pending_shared_orders once per batch AFTER
    all records in the batch have been processed through this function.
    """
    record_date = str(record.time)[:8]  # ALWAYS string for downstream comparisons
    # ... cross-day flush logic (lines 681-700 of current engine.py)
    # ... late-order detection (lines 705-725)
    # ... record-driven flush (lines 727-731)
    # ... buffer append (lines 733-741, including pending_shared_orders.append)
    # ... watermark update (lines 743-746)
    return (seqno, total_late_dropped, current_date, current_minute)

# GC optimization: disable cyclic GC during batch processing to amortize collection cost.
# With ~7.2M Python object allocations, default gen0 threshold (700) triggers ~10K gen0 scans.
# Disabling during the batch loop defers all GC to one explicit collect after the minute completes.
# This reduces unpredictable multi-millisecond GC pauses under GIL contention.
import gc
gc.disable()
try:
    # ... batch processing loop below ...
finally:
    gc.enable()
    gc.collect()  # Explicit collection after minute's batches complete

# NOTE: encoding_guard: Rust only supports UTF-8; fallback to Python for other encodings.
# Use .lower() because ConfigParser does NOT lowercase values — "UTF-8" must still match.
if use_rust_accel(self._config) and encoding.lower() in ("utf-8", "utf8"):
    try:
        batch, skipped = _rust_parse_batch(lines, encoding)
    except Exception as e:
        # Rust batch parse failed (e.g., MAX_BATCH_SIZE exceeded, PyO3 error).
        # Fall back to Python per-line parsing for this batch — NO DATA LOSS.
        logger.warning(
            "Rust parse_order_batch failed (%s: %s), falling back to Python for %d lines",
            type(e).__name__, e, len(lines)
        )
        batch = None  # Fall through to Python path below

    if batch is not None:
        if skipped > 0:
            log_level = logging.WARNING if skipped > len(lines) // 2 else logging.DEBUG
            logger.log(log_level, "Rust parse batch: %d lines, %d skipped", len(lines), skipped)
        for fields in batch:
            # Date check BEFORE seqno assignment — matches engine.py behavior
            record_date_int = fields[1] // 1_000_000_000  # fields[1] = time
            if record_date_int != today_int:
                continue
            seqno += 1  # seqno assigned HERE, after date check passes
            # Use keyword args for safety — catches field-order regressions
            record = OrderRecord(
                symbol=fields[0], seqno=seqno, time=fields[1],
                bidprice=fields[2], bidsize=fields[3],
                askprice=fields[4], asksize=fields[5],
                decimal=fields[6], rcvtime=fields[7],
            )
            # time_to_minute_key: use integer division + str() instead of str(int)[:12]
            # Verified: 20260528090000123 // 100_000 == 202605280900 == str(20260528090000123)[:12]
            # Eliminates one of two string allocations per record (no intermediate 17-char string).
            # Still produces str minute_key — downstream code (writer.py, flusher.py, tickfile.py)
            # all expect str and perform slicing (minute_key[:8], minute_key[8:12]).
            # Savings: ~75-150ms for 747K records (down from ~150-300ms).
            minute_key = str(record.time // 100_000)
            # Feed to SHARED processing function — single source of truth
            seqno, total_late_dropped, current_date, current_minute = \
                self._process_parsed_record(
                    record, today, seqno, minute_key,
                    buffers, current_date, current_minute,
                    pending_shared_orders, late_order_per_minute,
                    late_dropped_per_minute, output_dir,
                    total_late_dropped)
    else:
        # Rust failed — use Python per-line fallback for this batch
        for line in lines:
            record = parse_order_record(line, seqno + 1, encoding)
            if record is None:
                continue
            record_date = str(record.time)[:8]
            if record_date != today:
                continue
            seqno += 1
            # Feed to SAME shared processing function
            seqno, total_late_dropped, current_date, current_minute = \
                self._process_parsed_record(
                    record, today, seqno, str(record.time // 100_000),
                    buffers, current_date, current_minute,
                    pending_shared_orders, late_order_per_minute,
                    late_dropped_per_minute, output_dir,
                    total_late_dropped)
else:
    # Fallback: EXACT original Python path — no semantic changes
    for line in lines:
        record = parse_order_record(line, seqno + 1, encoding)
        if record is None:
            continue
        record_date = str(record.time)[:8]  # Original string slice — unchanged
        if record_date != today:            # today is str, computed above
            continue
        seqno += 1
        # Feed to SAME shared processing function
        seqno, total_late_dropped, current_date, current_minute = \
            self._process_parsed_record(
                record, today, seqno, str(record.time // 100_000),
                buffers, current_date, current_minute,
                pending_shared_orders, late_order_per_minute,
                late_dropped_per_minute, output_dir,
                total_late_dropped)
```

**Critical constraint**: Both parse paths (Rust and Python) produce `OrderRecord` objects that are fed into the **same** `_process_parsed_record` function. This ensures cross-day, late-order, buffer, and tickfile logic exists in exactly ONE place. The integration test in Section 7.2 validates this.

> **⚠️ Implementation note**: `_process_parsed_record` must be factored out from the existing per-record logic in `_order_loop` (engine.py lines 681-765). This refactoring is a prerequisite for the Rust integration and MUST be validated by the existing 380+ test suite BEFORE adding Rust code.

### 5.3 writer.py — NO CHANGES IN THIS PR

> **⚠️ DO NOT change `write_order_file` in the Rust acceleration PR.**

The current `write_order_file` implementation uses streaming per-line writes with 1MB buffering:

```python
# Current code (DO NOT CHANGE in this PR):
for rec in order_records:
    f.write(_format_order_row(rec) + "\n")  # Each write() releases GIL during I/O syscall
```

**Why batch join was proposed but rejected for this PR**:
- The current streaming write calls `f.write()` per record. GIL is released only when the C library buffer actually flushes to the OS (~every 1MB / ~12K records). Most individual `f.write()` calls just append to the C buffer under GIL. So write_order_file is **mostly GIL-held** regardless of approach.
- The proposed batch join (`"\n".join(...)`) constructs the entire string under GIL — **increasing** peak GIL-held time compared to incremental writes.
- However, the total write time (~200-400ms) is a small fraction of the ~2s total GIL-held work. Neither streaming nor batch join makes a material difference to the 60s SLA.
- Keeping the streaming write avoids introducing unnecessary risk in this PR.

**Future optimization**: After Rust acceleration is deployed and validated, if `write_order_file` profiling shows it's a bottleneck, consider moving the format+write to Rust (entirely GIL-free). This would be a separate PR with its own benchmarks.

---

## 6. Build System

### 6.1 Recommended: setuptools-rust (integrates with existing setuptools)

The project uses `setuptools` with `pyproject.toml`. Using `setuptools-rust` keeps a single build system and correctly places `_order_accel` inside the `minute_bar` package.

> **⚠️ CRITICAL: Fix build-backend FIRST (Phase 0 prerequisite).**
>
> The current `pyproject.toml` line 3 uses `build-backend = "setuptools.backends._legacy:_Backend"`.
> **This module does NOT exist in setuptools 82+** (verified empirically: `ModuleNotFoundError`).
> `pip install -e .` **fails** with `BackendUnavailable`.
>
> **Required fix**: Change `pyproject.toml` line 3 to:
> ```toml
> build-backend = "setuptools.build_meta:__legacy__"
> ```
> This is the canonical import path since setuptools 69. Apply this fix BEFORE any Rust work.
> After fixing, verify: `pip install -e .` must succeed.

**Root pyproject.toml — only the build-backend line changes.** Do not add `setuptools-rust` to `[build-system] requires` — this would force installation of `setuptools-rust` even on machines without Rust, causing build failures. Instead, developers install it manually (Section 6.3).

**New file: setup.py** (minimal, for Rust extension only):

```python
import os
from setuptools import setup

# Only build Rust extension if setuptools-rust is installed AND
# the order_accel/ directory exists (i.e., Rust source is present).
# Use __file__ for absolute path — works regardless of CWD.
_here = os.path.dirname(os.path.abspath(__file__))
rust_exts = []
if os.path.isdir(os.path.join(_here, "order_accel")):
    try:
        from setuptools_rust import RustExtension
        rust_exts = [
            RustExtension(
                "minute_bar._order_accel",
                path="order_accel/Cargo.toml",
            )
        ]
    except ImportError:
        # Rust source exists but setuptools-rust not installed — warn the developer.
        # This prevents silent "no extension" confusion when a colleague clones the repo.
        import sys
        print(
            "WARNING: order_accel/ directory found but setuptools-rust not installed. "
            "Rust extension will NOT be built. Install with: pip install setuptools-rust",
            file=sys.stderr,
        )

setup(rust_extensions=rust_exts)
```

Key design decisions:
- **`__file__`-based path** — `os.path.join(_here, "order_accel")` works regardless of working directory.
- **No `debug=False`** — let setuptools-rust use environment defaults (debug for dev, release for wheels).
- **`try/except ImportError`** — gracefully skips Rust if setuptools-rust isn't installed.
- **Blocking verification**: After `pip install -e .`, run `python -c "import minute_bar._order_accel; print(minute_bar._order_accel.__file__)"` and assert the path is inside `src/minute_bar/`. If not, add explicit `package_dir={"minute_bar": "src/minute_bar"}` to the `setup()` call.

> **⚠️ PEP 660 editable install note**: With setuptools 82+ and `pip install -e .`, the PEP 660 build backend (from `pyproject.toml`) may bypass `setup.py` entirely, meaning the Rust extension would NOT be built even with `setuptools-rust` installed. **If `pip install -e .` does not build the Rust extension**, use one of these alternatives:
> 1. `python setup.py build_ext --inplace` — explicitly builds the extension in-place
> 2. `cd order_accel && maturin develop --release` — uses maturin directly (Section 6.2)
> 3. Verify with: `python -c "from minute_bar._order_accel import is_available; print(is_available())"` — must return `True`

This ensures:
- `pip install -e .` WITH Rust toolchain builds the extension
- `pip install -e .` WITHOUT Rust toolchain succeeds (no extension, Python fallback)
- `from minute_bar._order_accel import ...` works when extension is built
- Only the build-backend line in `pyproject.toml` changes (fixed in Phase 0 step 1)

### 6.2 Alternative: maturin (if setuptools-rust is unavailable)

If `setuptools-rust` cannot be used, maturin requires additional configuration:

```toml
# order_accel/pyproject.toml (maturin-specific)
[tool.maturin]
module-name = "minute_bar._order_accel"
python-source = "../src"
```

```bash
cd order_accel && maturin develop --release
```

> **Warning**: This creates a dual-build-system situation. `pip install -e .` from root won't build the Rust extension. Developers must run both `pip install -e .` and `maturin develop --release`. setuptools-rust is strongly preferred.

### 6.3 One-time Setup (setuptools-rust)

```bash
# Install build dependencies
pip install setuptools-rust

# Build and install (editable mode)
pip install -e .

# Verify:
python -c "from minute_bar._order_accel import is_available; print(is_available())"
# → True
```

### 6.4 Rust Not Installed? Graceful Skip

The canonical `setup.py` in Section 6.1 handles this with `os.path.isdir("order_accel")` + `try/except ImportError`. If Rust toolchain is not installed or `setuptools-rust` is not available, `pip install -e .` succeeds without the extension. The Python fallback will be used.

### 6.5 Production Deployment

| Target | Build Command | Output |
|--------|--------------|--------|
| Windows dev | `pip install -e .` | `src/minute_bar/_order_accel.pyd` |
| Linux production | `pip wheel . --wheel-dir dist/` | `dist/minute_bar-...-cp312-cp312-manylinux_2_17_x86_64.whl` |
| CI (cibuildwheel) | `cibuildwheel --platform linux` | Wheels for manylinux x86_64 + aarch64 |

Production deployment steps:
1. Build wheel on Linux CI runner with Rust toolchain
2. Publish wheel to artifact repository
3. Production: `pip install minute_bar-<version>-cp312-cp312-manylinux_2_17_x86_64.whl`

---

## 7. Testing Strategy

### 7.1 Rust Unit Tests (in lib.rs)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_line_8col() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,20260528090000100";
        let result = parse_one_line(line, "utf-8").unwrap();
        assert_eq!(result.0, "7203");        // symbol
        assert_eq!(result.1, 20260528090000123); // time
        assert_eq!(result.2, 4580000);       // bidprice
        assert_eq!(result.3, 100);           // bidsize
        assert_eq!(result.4, 4590000);       // askprice
        assert_eq!(result.5, 200);           // asksize
        assert_eq!(result.6, 2);             // decimal
        assert_eq!(result.7, 20260528090000100); // rcvtime
    }

    #[test]
    fn test_parse_valid_line_6col() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200";
        let result = parse_one_line(line, "utf-8").unwrap();
        assert_eq!(result.6, 2);  // default decimal
        assert_eq!(result.7, 0);  // default rcvtime
    }

    #[test]
    fn test_skip_header() {
        // Must match Python: only lowercase "symbol," is checked
        assert!(parse_one_line(b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime", "utf-8").is_none());
        // Uppercase "Symbol," is NOT a header in Python — Rust must NOT skip it.
        // Python parse_order_record would return OrderRecord(symbol="Symbol", ...) because
        // startswith("symbol,") is False for "Symbol,".
        // Rust MUST also accept it as valid data (assuming valid numeric fields).
        // However, "Symbol" as a symbol value + "20260528090000123" as time passes the
        // 17-digit check, so this should parse successfully.
        let uppercase_result = parse_one_line(b"Symbol,20260528090000123,4580000,100,4590000,200,2,0", "utf-8");
        assert!(uppercase_result.is_some(), "Uppercase 'Symbol,' must NOT be treated as header");
        assert_eq!(uppercase_result.unwrap().0, "Symbol");
    }

    #[test]
    fn test_skip_empty_line() {
        assert!(parse_one_line(b"", "utf-8").is_none());
    }

    #[test]
    fn test_short_line() {
        assert!(parse_one_line(b"7203,20260528090000123", "utf-8").is_none()); // < 6 fields
    }

    #[test]
    fn test_extra_cols() {
        assert!(parse_one_line(b"7203,1,2,3,4,5,6,7,8,9", "utf-8").is_none()); // > 8 fields
    }

    #[test]
    fn test_crlf_line() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r\n";
        let result = parse_one_line(line, "utf-8");
        assert!(result.is_some(), "CRLF line should parse correctly");
        assert_eq!(result.unwrap().7, 0); // rcvtime should parse as 0, not fail on \r
    }

    #[test]
    fn test_trailing_cr_only() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r";
        let result = parse_one_line(line, "utf-8");
        assert!(result.is_some(), "trailing CR should be stripped");
    }

    #[test]
    fn test_whitespace_in_fields() {
        let line = b"7203, 20260528090000123 , 4580000 , 100 , 4590000 , 200 ";
        let result = parse_one_line(line, "utf-8");
        assert!(result.is_some(), "whitespace in fields should be trimmed");
    }

    #[test]
    fn test_empty_symbol() {
        let line = b",20260528090000123,4580000,100,4590000,200";
        assert!(parse_one_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_whitespace_only_symbol() {
        // Whitespace-only symbol should be rejected (matches Python .strip() behavior)
        let line = b"   ,20260528090000123,4580000,100,4590000,200";
        assert!(parse_one_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_non_utf8() {
        assert!(parse_one_line(&[0xFF, 0xFE], "utf-8").is_none());
    }

    #[test]
    fn test_negative_value() {
        // Python int() accepts negative; Rust i64 should too
        let line = b"7203,20260528090000123,-100,100,-200,200";
        let result = parse_one_line(line, "utf-8");
        assert!(result.is_some());
        assert_eq!(result.unwrap().2, -100); // bidprice = -100
    }

    #[test]
    fn test_non_17digit_timestamp_rejected() {
        // 8-digit time (20260528): Python str[:8]="20260528" matches today,
        // but //10^9=0 doesn't. Rust rejects to avoid date extraction divergence.
        let line = b"7203,20260528,4580000,100,4590000,200";
        assert!(parse_one_line(line, "utf-8").is_none());

        // 16-digit time (truncated): also rejected
        let line = b"7203,2026052809000012,4580000,100,4590000,200";
        assert!(parse_one_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_non_utf8_encoding_returns_none() {
        // Non-UTF-8 encoding must return None immediately, not attempt decode
        let line = b"7203,20260528090000123,4580000,100,4590000,200";
        assert!(parse_one_line(line, "shift_jis").is_none());
        assert!(parse_one_line(line, "cp932").is_none());
        // But utf-8 works
        assert!(parse_one_line(line, "utf-8").is_some());
    }

    #[test]
    fn test_batch_with_mixed_valid_invalid() {
        // Test parse_one_line directly (no PyO3 context needed for parse logic)
        let lines: Vec<&[u8]> = vec![
            b"symbol,time,...",      // header → skip
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",  // valid
            b"",                      // empty → skip
            b"7203,20260528090000123,4580000,100,4590000,200",       // valid 6-col
            b"bad_data",              // invalid → skip
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",  // valid
        ];
        let mut valid = 0;
        let mut skipped = 0;
        for line in &lines {
            match parse_one_line(line, "utf-8") {
                Some(_) => valid += 1,
                None => skipped += 1,
            }
        }
        assert_eq!(valid, 3);
        assert_eq!(skipped, 3);
    }

    #[test]
    fn test_is_available_self_test() {
        assert!(is_available());
    }
}
```

### 7.2 Python Integration Test (Parity Test)

```python
# tests/test_order_accel.py
"""Validate Rust parse_order_batch produces identical results to Python parse_order_record."""

import pytest
from minute_bar.csv_parser import parse_order_record, use_rust_accel

# Realistic test data — varied symbols, 6-col and 8-col, edge cases
SAMPLE_LINES = [
    b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime",  # header
    b"7203,20260528090000123,4580000,100,4590000,200,2,20260528090000100",
    b"6501,20260528090000456,2345000,500,2350000,300,2,20260528090000400",
    b"9984,20260528090000789,8900000,200,8910000,100,2,20260528090000700",
    b"6758,20260528090001000,12340000,50,12350000,75,2,20260528090000900",
    b"8306,20260528090001234,1500000,1000,1510000,800,2,0",  # rcvtime=0
    b"7203,20260528090001500,4585000,300,4595000,400,2,20260528090001400",
    b"",  # empty line
    b"6501,20260528090001789,2350000,100,2360000,200,2,20260528090001700",
    b"invalid_line",  # should be skipped
    b"7203,20260528090002000,4590000,150,4600000,250,2,20260528090001900",
    b"9984,20260528090002222,8920000,300,8930000,200,2,20260528090002100",
    b",20260528090003000,1000000,500,1010000,400",  # empty symbol → skip
    b"7203,20260529090000123,4580000,100,4590000,200,2,20260529090000100",  # cross-day → different date
]

# today_int for parity test (matches 20260528)
TODAY_INT = 20260528


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_rust_matches_python_field_by_field():
    """Rust parse_order_batch must produce identical field values to Python parse_order_record."""
    from minute_bar._order_accel import parse_order_batch

    rust_batch, rust_skipped = parse_order_batch(SAMPLE_LINES, "utf-8")

    # Python path — simulate engine seqno assignment
    py_records = []
    for line in SAMPLE_LINES:
        r = parse_order_record(line, 0, "utf-8")  # seqno=0 placeholder
        if r is not None:
            py_records.append(r)

    # Count must match
    assert len(rust_batch) == len(py_records), (
        f"Record count mismatch: Rust={len(rust_batch)}, Python={len(py_records)}"
    )

    # Field-by-field comparison (symbol, time, bidprice, bidsize, askprice, asksize, decimal, rcvtime)
    # Rust returns 8-tuples (no seqno); Python OrderRecord has 9 fields (with seqno)
    for i, (rust_fields, py_rec) in enumerate(zip(rust_batch, py_records)):
        # rust_fields = (symbol, time, bidprice, bidsize, askprice, asksize, decimal, rcvtime)
        assert rust_fields[0] == py_rec.symbol, f"Row {i}: symbol mismatch"
        assert rust_fields[1] == py_rec.time, f"Row {i}: time mismatch"
        assert rust_fields[2] == py_rec.bidprice, f"Row {i}: bidprice mismatch"
        assert rust_fields[3] == py_rec.bidsize, f"Row {i}: bidsize mismatch"
        assert rust_fields[4] == py_rec.askprice, f"Row {i}: askprice mismatch"
        assert rust_fields[5] == py_rec.asksize, f"Row {i}: asksize mismatch"
        assert rust_fields[6] == py_rec.decimal, f"Row {i}: decimal mismatch"
        assert rust_fields[7] == py_rec.rcvtime, f"Row {i}: rcvtime mismatch"


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_seqno_assigned_after_date_check():
    """Verify seqno increments only AFTER date check passes, matching engine.py."""
    from minute_bar._order_accel import parse_order_batch
    from minute_bar.models import OrderRecord

    rust_batch, _ = parse_order_batch(SAMPLE_LINES, "utf-8")

    # Simulate engine.py seqno assignment with date filter
    seqno = 0
    valid_records = []
    for fields in rust_batch:
        # Date check BEFORE seqno increment — exactly like engine.py
        record_date = fields[1] // 1_000_000_000  # fields[1] = time
        if record_date != TODAY_INT:
            continue  # skip cross-day record WITHOUT incrementing seqno
        seqno += 1
        record = OrderRecord(
            symbol=fields[0], seqno=seqno, time=fields[1],
            bidprice=fields[2], bidsize=fields[3],
            askprice=fields[4], asksize=fields[5],
            decimal=fields[6], rcvtime=fields[7],
        )
        valid_records.append(record)

    # Seqnos should be 1, 2, 3, ... (dense, monotonically increasing)
    seqnos = [r.seqno for r in valid_records]
    assert seqnos == list(range(1, len(valid_records) + 1))

    # Cross-day record (20260529) should NOT be in valid_records
    for r in valid_records:
        assert r.time // 1_000_000_000 == TODAY_INT


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_crlf_handling():
    """CRLF-terminated lines must parse identically to LF-terminated lines."""
    from minute_bar._order_accel import parse_order_batch

    lf_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    crlf_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r\n"
    cr_only_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r"

    lf_result, _ = parse_order_batch([lf_line], "utf-8")
    crlf_result, _ = parse_order_batch([crlf_line], "utf-8")
    cr_result, _ = parse_order_batch([cr_only_line], "utf-8")

    assert len(lf_result) == 1 and len(crlf_result) == 1 and len(cr_result) == 1
    assert lf_result[0] == crlf_result[0] == cr_result[0]


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_order_csv_byte_identical():
    """Full-scale parity: Rust and Python paths produce byte-identical CSV output."""
    from minute_bar._order_accel import parse_order_batch
    from minute_bar.models import OrderRecord
    from minute_bar.writer import _format_order_row

    # Generate 10,000 varied lines including cross-day boundary
    lines = []
    symbols = [b"7203", b"6501", b"9984", b"6758", b"8306", b"7974"]
    for i in range(10000):
        sym = symbols[i % len(symbols)]
        # Most records use today's date (20260528)
        time_val = 20260528090000000 + (i * 100) % 1000000
        line = f"{sym.decode()},{time_val},{4500000+i},{100+i},{4600000+i},{200+i},2,{time_val-23}".encode()
        lines.append(line)
    # Add cross-day records (20260529) — these affect seqno assignment
    for i in range(5):
        time_val = 20260529090000000 + i * 100
        lines.append(f"7203,{time_val},4600000,100,4610000,200,2,{time_val}".encode())
    # Add empty symbol (both Rust and fixed Python should skip)
    lines.append(b",20260528090099999,1000000,100,1010000,200")
    # Add whitespace-only symbol (both paths should skip after Phase 0 fix)
    lines.append(b"   ,20260528090088888,2000000,200,2010000,300")
    # Add 6-col lines (missing decimal and rcvtime — defaults applied)
    for i in range(20):
        lines.append(f"6758,202605281000{i:05d},5000000,{100+i},5010000,{200+i}".encode())
    # Add 7-col lines (missing rcvtime — default 0)
    for i in range(10):
        lines.append(f"8306,202605281000{i:05d},3000000,{50+i},3010000,{100+i},2".encode())
    # Add CRLF-terminated lines
    lines.append(b"7203,20260528110000001,4600000,100,4610000,200,2,0\r\n")
    lines.append(b"6501,20260528110000002,2400000,300,2410000,400,2,0\r\n")
    # Add whitespace in numeric fields
    lines.append(b"9984, 20260528110000003 , 8900000 , 150 , 8910000 , 250 , 2 , 0")
    lines.insert(0, b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime")

    # Both paths: apply date filter (today=20260528), seqno only after filter passes
    TODAY = 20260528

    # Rust path
    rust_batch, _ = parse_order_batch(lines, "utf-8")
    rust_csv_lines = []
    seqno = 0
    for fields in rust_batch:
        record_date = fields[1] // 1_000_000_000
        if record_date != TODAY:
            continue
        seqno += 1
        rec = OrderRecord(
            symbol=fields[0], seqno=seqno, time=fields[1],
            bidprice=fields[2], bidsize=fields[3],
            askprice=fields[4], asksize=fields[5],
            decimal=fields[6], rcvtime=fields[7],
        )
        rust_csv_lines.append(_format_order_row(rec))

    # Python path — MUST match engine.py exactly: str[:8] for date extraction
    py_csv_lines = []
    seqno = 0
    for line in lines:
        r = parse_order_record(line, 0, "utf-8")
        if r is not None:
            record_date = str(r.time)[:8]  # EXACT engine.py behavior — NOT //10^9
            if record_date != str(TODAY):  # EXACT engine.py behavior — string comparison
                continue
            seqno += 1
            rec = OrderRecord(r.symbol, seqno, r.time, r.bidprice, r.bidsize,
                              r.askprice, r.asksize, r.decimal, r.rcvtime)
            py_csv_lines.append(_format_order_row(rec))

    assert rust_csv_lines == py_csv_lines, (
        f"CSV output mismatch: {len(rust_csv_lines)} vs {len(py_csv_lines)} rows"
    )


def test_fallback_when_rust_unavailable():
    """Verify Python fallback works when Rust extension is not loaded."""
    # This test always runs (even without Rust extension)
    from minute_bar.csv_parser import parse_order_record
    from minute_bar.models import OrderRecord

    line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    record = parse_order_record(line, 1, "utf-8")
    assert record is not None
    assert record.symbol == "7203"
    assert record.time == 20260528090000123
```

### 7.3 E2E Performance Test

```bash
# After pip install -e . (with Rust):
cd D:/FIU && rm -rf test/tickfile_live_output test/checkpoint_tickfile_json
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini

# Verify: 0900 minute gap < 60s
# Verify: order CSV content byte-identical to previous run (optional diff)
```

### 7.4 Automated Performance Gate

```python
# tests/test_order_accel_performance.py
import pytest
import time

@pytest.mark.slow
@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_rust_parse_throughput():
    """Rust parse must achieve >1M rec/s for 100K records."""
    from minute_bar._order_accel import parse_order_batch

    # Generate 100K realistic lines
    lines = []
    for i in range(100_000):
        lines.append(
            f"7203,2026052809{i:06d},4580000,100,4590000,200,2,0".encode()
        )

    start = time.perf_counter()
    results, _ = parse_order_batch(lines, "utf-8")
    elapsed = time.perf_counter() - start

    throughput = len(results) / elapsed
    assert throughput > 1_000_000, f"Rust parse too slow: {throughput:,.0f} rec/s"
    assert elapsed < 0.2, f"100K records took {elapsed:.3f}s (expected <0.2s)"
```

### 7.5 Regression Safety

- All existing 380+ tests must pass **with** Rust extension (CI job 1)
- All existing 380+ tests must pass **without** Rust extension (CI job 2, no Rust toolchain)
- `use_rust_accel()` function enables controlled testing of both paths
- Config toggle `enable_order_accel = false` forces Python path even with Rust installed

---

## 8. Performance Estimates

### 8.1 Per-Batch Math Breakdown

Assumptions:
- Peak minute 0900: 747K records
- `order_chunk_size_bytes` = **524288** (recommended when Rust accel enabled, yields ~6400 lines/batch)
- Each batch: ~6400 lines × ~80 bytes = ~512KB
- **Why 524288 instead of default 65536**: Reduces GIL re-acquire points from 934 to ~117 (8x fewer preemption opportunities for snapshot thread). See Section 9.2 for config.

| Step | GIL | Time/batch (6400 lines) | Batches (747K rec) | Total |
|------|-----|----------------------|-------------------|-------|
| `tailer.read_lines()` | Interleaved | ~0.8ms | 117 | ~94ms |
| Input conversion (`list[bytes]` → `Vec<Vec<u8>>`) | Held | ~0.3-0.4ms | 117 | ~37-50ms |
| Rust parse (GIL released) | **Free** | ~0.56ms | 117 | ~66ms |
| **GIL handoff overhead** | Held | **0-5ms** | 117 | **0-585ms** |
| PyO3 return conversion | Held | ~4.0ms | 117 | ~468ms |
| `OrderRecord` construction + date check | Held | ~3.5-5.0ms | 117 | ~410-585ms |
| `time_to_minute_key` (integer div + str) | Held | **~0.1-0.2ms** | 117 | **~75-150ms** |
| State lock + buffer append | Held | ~2.4ms | 117 | ~281ms |
| **Subtotal (per-batch)** | — | **~10.7-17.6ms** | — | **~1,337-2,207ms** |
| `write_order_file` (unchanged) | Held (~90-95%) | — | 1 | ~200-400ms (~180-380ms GIL-held) |
| GC overhead (amortized with gc.disable) | Held | — | 1 | ~20-50ms (single gc.collect after minute) |
| **Total GIL-held** | — | — | — | **~1,357-2,252ms** |
| **Total wall-clock (single-thread)** | — | — | — | **~1,470-2,345ms** |

> **Contention model (CORRECTED)**: The 18x factor from the performance analysis is a **throughput degradation** (rec/s), NOT a wall-clock multiplier. For 2 CPU-bound threads sharing GIL, wall-clock expansion is ~2-4x. Conservative wall-clock under concurrent load: ~2s × 3 = **~6s**. Target: **<10s**. Well within 60s SLA. **Mandatory concurrent benchmark must validate** (Section 8.3).

> **⚠️ Residual risk**: The PyO3 return conversion (~468ms, creating **5.97M Python objects**) and post-processing are still GIL-held. With the corrected contention model (~2-4x wall-clock expansion for 2 CPU threads), effective processing time should be ~4-8s. The **PyO3 microbenchmark** (Section 8.3 item 2) is a blocking prerequisite — if PyO3 return conversion exceeds 1.0s, switch to the flat binary return alternative (see below).

> **Important**: The "~2s" is single-thread compute time. With corrected contention model (~2-4x for 2 threads), wall-clock should be ~4-8s, well within 60s SLA. The "147s → <10s" improvement comes from: Rust removes ~0.9s of GIL-held parse time, replacing it with ~65ms GIL-free. Snapshot thread can run during Rust parse, eliminating the GIL throughput bottleneck.

> **Memory budget (peak 747K records)**: ~230-410MB estimated peak. Components: (a) PyO3 tuples per-batch: ~2.3MB × 117 batches = released between batches; (b) accumulated NamedTuples: 747K × 112B = ~80MB; (c) accumulated minute_key integers: 747K × 28B = ~21MB; (d) buffer dict: 747K × ~150B = ~107MB; (e) Python GC gen1/gen2 promotions: ~20-100MB. **Must validate with `tracemalloc`** in Phase 3 benchmark.

### 8.3 Required Validation Benchmarks

Before claiming the performance target is met:

1. **Single-thread Rust parse**: 747K records, measure wall-clock. Must be <0.5s.
2. **PyO3 return conversion microbenchmark (BLOCKING)**: Construct 747K 8-tuples of (String, i64×7) in Rust and return via PyO3. Measure Python-side wall-clock time for the return conversion alone. This quantifies the ~5.97M Python object allocation cost (747K tuples × 8 elements). Must be <1.0s (recommended target: <0.8s for margin). If >1.0s, the flat binary return alternative (Section 8.4) becomes the **primary** path — implement it as a parallel track, not just a fallback. Independent review estimates real-world PyO3 conversion at 1.5-2.5s per 747K records (2-5μs per top-level tuple including type dispatch + refcount + GC registration).
3. **Concurrent benchmark (BLOCKING)**: Order thread + snapshot thread running simultaneously, measure order thread wall-clock for 747K records. Must be <30s. **⚠️ IMPORTANT**: This must use **sustained artificial load** — spawn two threads that each parse their respective data types simultaneously for the full duration. Do NOT rely solely on speed=100 E2E test, which produces bursty contention where order/snapshot threads may not overlap significantly. Production (speed=1) creates sustained GIL contention (~12K rec/s for 60s continuously), which is the worst-case scenario the benchmark must validate.
4. **Full E2E test**: `test-tickfile-live.ini`, verify 0900 minute gap <60s.
5. **Batch size comparison**: Run concurrent benchmark with `order_chunk_size_bytes=65536` (934 batches), `524288` (117 batches), and `2097152` (~29 batches). Compare wall-clock times to quantify GIL re-acquire overhead and identify optimal batch size.
6. **Input conversion benchmark**: Measure time to convert `list[bytes]` (6400 elements) to `Vec<Vec<u8>>`. Quantifies hidden GIL-held cost. (PyBackedBytes is NOT viable with `allow_threads()` — borrowed Python refs are `!Send`.)
7. **Memory benchmark**: Use `tracemalloc` to measure peak memory during 747K-record processing. Must be <500MB.

> **speed=100 vs speed=1**: At speed=100, data arrives in a concentrated burst, creating worst-case data volume but **bursty** GIL contention (order/snapshot threads may not overlap). At production speed=1, data arrives over 60s (~12.3K rec/s at peak), creating **sustained** GIL contention that is potentially more severe for wall-clock. **speed=100 validates data volume capacity; sustained concurrent benchmark validates GIL contention.** Both are required.

### 8.4 Flat Binary Return Fallback (if PyO3 tuple return >1.0s)

If the PyO3 microbenchmark shows tuple return >1.0s, switch to flat binary encoding:

```rust
// Alternative: return flat binary buffer instead of Vec<tuple>
// Format: [symbol_len: u16, symbol_bytes, time: i64 LE, bidprice: i64 LE, ...]
// Total per record: ~2 + symbol_len + 7*8 = ~90 bytes
// 747K records = ~66MB returned as single PyBytes (zero-copy from Vec<u8>)

#[pyfunction]
fn parse_order_batch_flat(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<u8>, u64)> {
    // Same guard as primary function
    if lines.len() > MAX_BATCH_SIZE {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Batch too large: {} lines (max {})", lines.len(), MAX_BATCH_SIZE)
        ));
    }
    let encoding_owned = encoding.to_string();
    let (buf, skipped) = py.allow_threads(|| {
        let mut skipped: u64 = 0;
        let mut buf = Vec::with_capacity(lines.len() * 90);
        for line in &lines {
            match parse_one_line(line, &encoding_owned) {
                Some((sym, t, bp, bs, ap, as_, d, rt)) => {
                    let sym_bytes = sym.as_bytes();
                    buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
                    buf.extend_from_slice(sym_bytes);
                    buf.extend_from_slice(&t.to_le_bytes());
                    buf.extend_from_slice(&bp.to_le_bytes());
                    buf.extend_from_slice(&bs.to_le_bytes());
                    buf.extend_from_slice(&ap.to_le_bytes());
                    buf.extend_from_slice(&as_.to_le_bytes());
                    buf.extend_from_slice(&d.to_le_bytes());
                    buf.extend_from_slice(&rt.to_le_bytes());
                }
                None => skipped += 1,
            }
        }
        (buf, skipped)
    });
    Ok((buf, skipped))
}
```

```python
# Python decode: zero-copy via memoryview
import struct

def decode_flat_batch(buf: bytes, count_hint: int) -> list:
    """Decode flat binary buffer from Rust parse_order_batch_flat.
    Format per record: u16 LE symbol_len + symbol_bytes + 7 × i64 LE.
    Returns list of (symbol, time, bidprice, bidsize, askprice, asksize, decimal, rcvtime).
    """
    mv = memoryview(buf)
    offset = 0
    results = []
    skipped = 0
    while offset < len(mv):
        try:
            # IMPORTANT: '<H' (unsigned short) matches Rust u16 — NOT '<h' (signed)
            sym_len = struct.unpack_from('<H', mv, offset)[0]
            offset += 2
            symbol = mv[offset:offset+sym_len].decode('utf-8')  # zero-copy from memoryview
            offset += sym_len
            fields = struct.unpack_from('<7q', mv, offset)  # 7 × signed 64-bit LE
            offset += 56  # 7 * 8 bytes
            results.append((symbol, *fields))
        except (struct.error, UnicodeDecodeError, IndexError) as e:
            # Corrupted record — CANNOT continue (sequential format, no resync).
            # IMPORTANT: Raise exception instead of returning partial results.
            # Engine.py's try/except around _rust_parse_batch will catch this
            # and fall back to Python per-line parsing for the ENTIRE batch.
            # This prevents silent data loss from partial results.
            raise struct.error(
                f"Flat binary decode failed at offset {offset}: {e}. "
                f"Decoded {len(results)} records before failure. "
                f"Batch will be reprocessed via Python fallback."
            ) from e
    return results
```

---

## 9. Startup Validation & Config

### 9.1 Startup Log

```python
# In Engine.__init__() — AFTER self._config is available, BEFORE threads start:
# ALWAYS log Rust status for operational visibility
accel_enabled = getattr(self._config.input, 'enable_order_accel', False)
if _RUST_ACCEL_AVAILABLE and accel_enabled:
    logger.info(
        "Order acceleration: ENABLED (Rust _order_accel loaded, self-test passed)"
    )
elif _RUST_ACCEL_AVAILABLE and not accel_enabled:
    logger.warning(
        "Order acceleration: DISABLED by config (Rust available but enable_order_accel=false). "
        "Set enable_order_accel=true for peak minute performance."
    )
else:
    if accel_enabled:
        # Config says Rust is required but it's not available → fail-fast
        raise RuntimeError(
            "enable_order_accel=true in config but Rust extension _order_accel "
            "is not installed. Peak minute SLA (60s) cannot be met without it. "
            "Install with: pip install setuptools-rust && pip install -e ."
        )
    else:
        logger.warning(
            "Order acceleration: DISABLED (Rust extension not available). "
            "Peak minute performance may not meet 60s SLA."
        )
```

> **Placement requirement**: This MUST be in `Engine.__init__()`, not at module level (config not yet loaded) and not in `_start_threads()` (too late for clean error handling).

### 9.2 Config Toggle

> **⚠️ Section placement**: `enable_order_accel` belongs in the **`[input]`** section, not `[output]`.
> Rust acceleration is a **parse feature** that controls how order CSV lines are decoded.
> Placing it in `[input]` co-locates it with `order_chunk_size_bytes` and `file_encoding`,
> reducing the risk of operators enabling Rust accel without adjusting chunk size.

```ini
# config/production.ini
[input]
enable_order_accel = false   ; Default OFF — must explicitly opt-in after Rust installed
                             ; Set true ONLY after Rust extension is verified working
                             ; If true and Rust not installed → fail-fast
order_chunk_size_bytes = 524288  ; RECOMMENDED when Rust accel enabled (117 batches vs 934)
```

```python
# In config.py InputConfig — add after order_chunk_size_bytes and file_encoding:
# (The exact field name depends on the existing InputConfig structure, but the
# placement must be in InputConfig, not OutputConfig.)
enable_order_accel: bool = False  # ← ADD to InputConfig. Default False.

# INI parsing — add inside the `if parser.has_section("input"):` block:
cfg.input.enable_order_accel = s.getboolean("enable_order_accel", cfg.input.enable_order_accel)

# RECOMMENDED when Rust accel is enabled: increase chunk size to reduce GIL re-acquire points
# cfg.input.order_chunk_size_bytes = 524288  # Uncomment when enable_order_accel=true
```

### 9.3 Rollback Plan

If Rust extension causes issues in production:
1. Set `enable_order_accel = false` in INI config
2. Restart the service
3. System runs in Python fallback mode (no Rust extension used)

If a corrupted `.pyd`/`.so` prevents startup:
1. Delete or rename `_order_accel.pyd`/`_order_accel.so` from `src/minute_bar/` (or wheel package dir)
2. Set `enable_order_accel = false` in INI config
3. Restart — the `try/except ImportError` in `csv_parser.py` handles missing extension gracefully

---

## 10. Implementation Order

### Phase 0: Prerequisites (must complete before Rust work)

1. **Fix pyproject.toml build-backend**: Change `build-backend = "setuptools.backends._legacy:_Backend"` to `build-backend = "setuptools.build_meta:__legacy__"`. The current backend does NOT exist in setuptools 82+ (verified empirically). Verify: `pip install -e .` must succeed.
2. **Audit production data for empty symbols**: Run `grep -c "^," order_*.csv` on existing output to check if empty-symbol records exist. If found, this fix changes production behavior — document and get approval. **Note**: This is a PRE-EXISTING bug — `parse_order_line` (slow path) already rejects empty symbols, but `parse_order_record` (hot path used in engine.py) does NOT. The audit must cover both paths.
3. **Fix Python `parse_order_record` to skip empty/whitespace-only symbols**: add `if not fields[0].strip(): return None` in `csv_parser.py` line ~260. This is a **blocking prerequisite AND a behavior change** — records with empty or whitespace-only symbols were previously accepted and written to output CSVs. After this fix, they will be silently discarded. Both Rust and Python must produce identical results.
4. Run full regression (380+ tests) — verify empty symbol fix doesn't break anything.
5. Add `enable_order_accel` to `InputConfig` (default `False`) + INI parsing in `[input]` section (see Section 9.2 for exact insertion point).
6. Add startup log + fail-fast check (only when `enable_order_accel = true` explicitly).

### Phase 1: Rust Extension

7. Create `order_accel/` with `Cargo.toml` + `Cargo.lock` + `src/lib.rs` (including unit tests from Section 7.1) + `rust-toolchain.toml`. Add `.gitignore` with `order_accel/target/`, `*.pyd`, `*.so` immediately to prevent accidental build artifact commits.
8. Verify `cd order_accel && cargo check` succeeds (PyO3 + Rust version compatibility check)
9. Run `cd order_accel && cargo test` — verify all Rust unit tests pass WITHOUT Python
10. Add `setup.py` with `setuptools_rust.RustExtension` (graceful skip)
11. `pip install -e .` with Rust toolchain + setuptools-rust
12. Verify: `python -c "from minute_bar._order_accel import is_available; print(is_available())"`
13. Add `parse_order_batch` import to `csv_parser.py` with broad fallback

### Phase 2: Python Integration

14. Update `engine.py` order loop:
    - Rust path: date check BEFORE seqno increment, `encoding.lower()` guard, `today_int`, keyword args for OrderRecord
    - **try/except around `_rust_parse_batch`**: on exception, fall back to Python per-line parsing for that batch (NO DATA LOSS)
    - Python fallback: **EXACT original code** — `str(record.time)[:8]` with `today` string
    - today_int guard: raise RuntimeError if `_get_target_date()` returns invalid value
    - Skip-rate WARNING: log WARNING when skipped_count > len(lines) // 2
15. **DO NOT change `write_order_file`** — keep current streaming write (see Section 5.3)
16. Write Python integration tests (Section 7.2) — must include:
    - Cross-day records
    - Empty-symbol records (both `""` and `"   "`)
    - Non-17-digit timestamps
    - Uppercase "Symbol," header
    - 6-col and 7-col lines
    - CRLF lines
    - Whitespace in numeric fields
    - test_order_csv_byte_identical uses `str(r.time)[:8]` for Python path (matching engine.py exactly)
    - All OrderRecord constructions use keyword args (matching engine.py production code)
    - Rust batch exception fallback test (mock Rust to raise, verify Python fallback)
17. Add engine-level integration test (exercise engine.py Rust path, not just parse_order_batch)
18. Add corrupted .pyd rollback test (verify import fallback + system starts)

### Phase 3: Validation

19. Run full regression (380+ tests with Rust + `enable_order_accel=true`)
20. Run full regression (380+ tests without Rust, `enable_order_accel=false`)
21. **PyO3 return conversion microbenchmark** (Section 8.3 item 2) — BLOCKING: must be <1.0s
22. **Batch size comparison benchmark** (Section 8.3 item 5) — compare 65536 vs 524288 vs 2097152
23. Run E2E performance test
24. **Run concurrent benchmark** (order + snapshot threads) — BLOCKING: must show peak minute <60s

### Phase 4: Production Readiness

25. Set `enable_order_accel = true` in production INI after validation
26. Recommend `order_chunk_size_bytes = 524288` when Rust accel enabled
27. Add comprehensive `.gitignore` (order_accel/target/, *.pyd, *.so, __pycache__/, dist/, build/, *.egg-info/)
28. Build Linux wheel on CI with Rust toolchain (`cargo build --locked --release`)
29. **Warmup self-test**: In `Engine.__init__()` **BEFORE** startup log (Section 9.1), BEFORE threads. Call `parse_order_batch` with 1000 hardcoded lines to exercise full PyO3 return path. On failure: log ERROR, set `_RUST_ACCEL_AVAILABLE = False` (via `csv_parser.set_rust_available(False)` — do NOT mutate module variable directly), proceed with Python fallback (do NOT raise). The startup log THEN runs and emits the CORRECT status (WARNING if warmup failed, not "ENABLED").

---

## 11. Risk Mitigation

| Risk | Mitigation | Status |
|------|-----------|--------|
| Rust not available on target machine | Python fallback path + config toggle (default OFF) + fail-fast only when explicitly enabled | ✅ Documented |
| Rust parse differs from Python | Integration test validates field-by-field parity + byte-identical CSV | ✅ Section 7.2 |
| Build complexity | Fix pyproject.toml backend first (Phase 0), then setuptools-rust with graceful skip | ✅ Section 6.1 |
| Maintenance burden | ~150 lines Rust, single function, no external deps beyond pyo3 | Acceptable |
| Seqno drift | Seqno NOT in Rust return; Python assigns it matching current behavior | ✅ Section 5.2 |
| Silent data loss from Rust parse | try/except around Rust batch + Python per-line fallback + skip count logged | ✅ Section 5.2 |
| Rust batch exception | try/except catches exception, falls back to Python per-line for that batch | ✅ Section 5.2 |
| CRLF / encoding issues | Rust strips `\r`, trims whitespace, supports only UTF-8, encoding.lower() guard | ✅ Section 4.3, 5.2 |
| os.replace race | `os.replace` stays inside write lock | ✅ Section 5.3 |
| Production SLA violation | Startup fail-fast when `enable_order_accel=true` but no Rust | ✅ Section 9.1 |
| Windows vs Linux wheel | `setuptools-rust` + `pip wheel` + cibuildwheel | ✅ Section 6.5 |
| No rollback | Config toggle `enable_order_accel=false` + delete .pyd + restart | ✅ Section 9.3 |
| Performance regression | Automated benchmark gate in CI | ✅ Section 7.4 |
| Empty symbol divergence | **PRE-EXISTING BUG**: parse_order_line rejects, parse_order_record accepts. Fix both as Phase 0 blocking prerequisite. Use `.strip()` to catch whitespace-only. | ✅ Section 10 |
| Uppercase header divergence | Only lowercase "symbol," check in Rust; test asserts uppercase parses as data | ✅ Section 4.3, 7.1 |
| Date extraction divergence | 17-digit timestamp validation guard | ✅ Section 4.3 |
| Non-UTF-8 encoding silent decode | Return None immediately for non-UTF-8 | ✅ Section 4.3 |
| PyO3 return GIL risk (5.97M objects) | Mandatory PyO3 microbenchmark + flat binary return fallback (Section 8.4) | ⚠️ Section 8.3 |
| Python fallback code changed | Fallback is EXACT original code (`str[:8]` not `//10^9`) | ✅ Section 5.2 |
| Rust panic crashes process | `panic = "abort"` + MAX_BATCH_SIZE guard prevents OOM. Expected data loss: at most 1M records in buffer. Verify auto-restart. | ✅ Section 4.2 |
| build-backend broken | **Empirically verified broken.** Fix to `setuptools.build_meta:__legacy__` as Phase 0 step 1 | ✅ Section 6.1 |
| setuptools-rust requires conflict | Optional manual install, not in build-system | ✅ Section 6.1 |
| 934-batch GIL re-acquire storm | Recommend `order_chunk_size_bytes=524288` (117 batches); benchmark comparison required | ✅ Section 8.3, 9.2 |
| write_order_file GIL regression | Batch join REMOVED from this PR; keep streaming write | ✅ Section 5.3 |
| Non-reproducible Rust builds | Cargo.lock committed; PyO3 pinned to patch version; `cargo build --locked` | ✅ Section 4.1, 4.2 |
| Skip rate not observable | WARNING log when skipped_count > 50% of lines | ✅ Section 5.2 |
| today_int computation failure | Guard: RuntimeError if `_get_target_date()` returns invalid 8-digit string | ✅ Section 5.2 |
| Engine.py Rust path untested | E2E validation must exercise engine.py Rust path, not just parse_order_batch | ⚠️ Section 8.3 |
| Corrupted .pyd prevents startup | Rollback: delete .pyd + set config false + restart | ✅ Section 9.3 |
| Contention model error | Corrected: 18x is throughput, not wall-clock. Wall-clock ~2-4x for 2 threads. | ✅ Section 8.1 |
| encoding &str not Send | Convert to owned String before allow_threads closure | ✅ Section 4.3 |
| Encoding guard case-sensitive | Use `encoding.lower() in ("utf-8", "utf8")` | ✅ Section 5.2 |

---

## Appendix A: Review History

### Round 1 (2026-06-10): 3-Agent Design Review

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Critical findings (5)**:
1. **Seqno assignment broken** — Rust assigned by line index, Python by valid record count → **Fixed**: Removed seqno from Rust return, Python assigns it.
2. **Integration test passed despite seqno bug** — Test used `i+1` for both paths → **Fixed**: New test uses engine-like seqno simulation.
3. **Module path resolution dead-on-arrival** — maturin installed as top-level module → **Fixed**: Switched to setuptools-rust.
4. **os.replace race condition** — Moved outside write lock → **Fixed**: Kept inside lock.
5. **Silent performance degradation** — No startup check → **Fixed**: Startup log + config toggle + fail-fast.

**Major findings (8)**:
1. Performance estimates unvalidated → **Fixed**: Added per-batch math breakdown + required validation benchmarks.
2. Rust parse stricter than Python int() → **Fixed**: Added .trim() on all numeric fields.
3. Fallback import too narrow → **Fixed**: Broadened to catch RuntimeError, AttributeError, OSError.
4. No config toggle → **Fixed**: Added `enable_order_accel` INI option.
5. PyO3 0.22 outdated → **Fixed**: Upgraded to >=0.23.
6. No CI/CD or cross-platform build → **Fixed**: Added build/deploy section with cibuildwheel.
7. Batch join memory ~120MB → **Fixed**: Documented memory budget.
8. Write improvement overstated → **Fixed**: Adjusted estimates to 200-400ms range.

### Round 2 (2026-06-10): 3-Agent Closure Review

**Agents**: Performance Closure (Agent 1), Correctness Closure (Agent 2), Deploy Closure (Agent 3)

**Performance verdict**: Peak minute <60s confirmed with high confidence. Conservative ~5-10s single-thread compute. GIL bypass architecture is effective — removes ~4.7s GIL-held parse, replaces with ~65ms GIL-free.

**Critical findings (2)**:
1. **Seqno increment before date check** — Rust path incremented seqno before date filter, Python increments after → **Fixed**: Moved `seqno += 1` after date check in both Rust and Python integration paths. Added `test_seqno_assigned_after_date_check` with cross-day records.
2. **Rust type annotation mismatch** — 9 type slots `(String, i64×8)` for 8-element tuple → **Fixed**: Corrected to `(String, i64×7)` = 8 slots matching the actual data fields.

**Major findings (5)**:
1. **Empty symbol mismatch** — Rust skips, Python passes through → **Fixed**: Added to implementation plan as step 7 (fix Python `parse_order_record`). Test data now includes empty-symbol case.
2. **Encoding guard missing** — Non-UTF-8 data could be silently skipped by Rust → **Fixed**: Added `encoding in ("utf-8", "utf8")` check before calling Rust in engine.py integration.
3. **GIL map batch count inconsistency** — Section 3.1 used 117 batches, Section 8.1 used 934 → **Fixed**: Section 3.1 now uses conservative 934 batches consistently.
4. **Integration tests missing cross-day coverage** → **Fixed**: Added cross-day records to test data, seqno test simulates date filter, byte-identical test includes cross-day boundary.
5. **`today_int` undefined** — Need explicit conversion note → **Fixed**: Added comment in engine.py integration code showing `today_int = int(self._get_target_date())`.

**All 3 agents confirmed**: No remaining Critical or Major issues after fixes.

### Round 3 (2026-06-10): Independent Production-Grade Review (Fresh Start)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**6 Critical findings (new, not caught by internal reviews)**:
1. **Uppercase "Symbol," header divergence** — Rust skipped lines Python parses as data → **Fixed**: Removed uppercase check, only lowercase "symbol," matches Python.
2. **Date extraction diverges for non-17-digit timestamps** — `str[:8]` vs `//10^9` produce different results → **Fixed**: Added 17-digit timestamp validation guard in Rust.
3. **Python fallback is NOT original code** — Changed date comparison semantics in fallback path → **Fixed**: Restored exact original `str(record.time)[:8]` comparison in fallback.
4. **PyO3 return conversion dominates GIL-held time** — ~1.5-1.9s GIL-held may still cause SLA violation → **Fixed**: Added explicit residual risk warning, upgraded concurrent benchmark to blocking prerequisite.
5. **build-backend mismatch** — Current pyproject.toml uses `_legacy`, spec proposed changing to `build_meta` → **Fixed**: Do NOT change build-backend; test setuptools-rust with current backend.
6. **Empty symbol must be blocking prerequisite** — Currently "step 7" but causes parity divergence → **Fixed**: Moved to implementation step 1.

**12 Major findings**:
1. Non-UTF-8 encoding silent attempt → **Fixed**: Return None immediately.
2. Numeric underscore divergence → **Rejected**: Production data never has underscores.
3. Memory ~222MB not 120MB → **Fixed**: Updated budget.
4. enable_order_accel defaults True → **Fixed**: Changed to False.
5. setup.py editable install placement → **Fixed**: Added os.path.isdir check + no debug=False.
6. setuptools-rust requires conflict → **Fixed**: Not in build-system requires.
7. Concurrent benchmark unvalidated → **Fixed**: Blocking prerequisite.
8. time_to_minute_key GIL-held → **Deferred**: Follow-up optimization.
9. state.lock contention → **Deferred**: Pre-existing issue.
10. Rust panic not handled → **Fixed**: `panic = "abort"` in Cargo.toml.
11. Missing rust-toolchain.toml → **Fixed**: Added to file structure.
12. Decouple write_order_file → **Rejected**: Batch join is additive, doesn't change semantics.

### Round 3 (2026-06-10): Independent Production-Grade Review (Fresh Start)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Critical findings (5)**:
1. **Empty symbol is a BEHAVIOR CHANGE** — Records previously accepted will be silently discarded. Added production data audit step (Phase 0 step 1). → **Fixed**: Added audit step + skip-category breakdown recommendation.
2. **PyO3 return creates 5.97M Python objects** — 747K × 8 = 5,976,000 allocations under GIL. Previous reviews flagged as "residual risk" but did not quantify. → **Fixed**: Added PyO3 microbenchmark as blocking prerequisite. Documented alternatives (flat binary, Rust-side date filter, NumPy array).
3. **write_order_file batch join is net-negative for GIL** — Current streaming write already releases GIL during I/O; batch join construction is entirely GIL-held. Previous review Rejected decoupling; new evidence reverses decision. → **Fixed**: Batch join REMOVED from this PR. Section 5.3 now says "DO NOT change write_order_file".
4. **test_order_csv_byte_identical uses //10^9 instead of str[:8]** — Test validates different algorithm than engine.py Python fallback. False confidence. → **Fixed**: Test now uses `str(r.time)[:8]` matching engine.py exactly.
5. **934 batch GIL re-acquire storm** — Each batch boundary is a GIL re-acquire point for snapshot thread preemption. → **Fixed**: Recommend `order_chunk_size_bytes=524288` (117 batches, 8x reduction). Updated Section 8.1 math to use 117 batches.

**Major findings (10)**:
1. time_to_minute_key per-record str() allocation → **Deferred**: Evaluate after concurrent benchmark.
2. Engine.py Rust path untested → **Fixed**: Added to E2E validation requirements.
3. test_skip_header missing assertion for uppercase "Symbol" → **Fixed**: Added explicit assertion.
4. Skip-rate WARNING threshold → **Fixed**: WARNING when skipped_count > 50%.
5. today_int computation robustness → **Fixed**: Added guard with assertion.
6. No Cargo.lock → **Fixed**: Added to file structure + pinned PyO3 to patch version.
7. panic=abort + OOM → **Fixed**: Added MAX_BATCH_SIZE guard.
8. Batch join memory guard → **Moot**: Batch join removed from PR.
9. Startup validation placement ambiguous → **Fixed**: Specified Engine.__init__() explicitly.
10. No cargo test in build steps → **Fixed**: Added to Phase 1 steps.
11. OrderRecord varargs unpacking fragile → **Fixed**: Changed to keyword args.
12. GIL contention margin uses 10x not 18x → **Fixed**: Recalculated with 18x worst-case (~36s).
13. Corrupted .pyd rollback missing → **Fixed**: Added delete .pyd step to rollback plan.
14. setuptools.backends._legacy verification → **Added**: Phase 1 verification step.

### Round 4 (2026-06-10): Closure Review (3-Agent)

**Agents**: Performance Closure (Agent 1), Correctness Closure (Agent 2), Deploy Closure (Agent 3)

**All 3 agents confirmed: No remaining Critical or Major issues.**

**Post-review fixes applied (5 minor items)**:
1. today_int `assert` → `raise RuntimeError` (cannot be disabled with -O)
2. Test code OrderRecord positional args → keyword args (matches engine.py production code)
3. MAX_BATCH_SIZE doc comment corrected ("raises PyValueError" not "returns empty")
4. Phase 1 steps merged (test writing included in step 6, cargo test at step 7)
5. Implementation steps renumbered sequentially (Phase 0: 1-5, Phase 1: 6-12, Phase 2: 13-16, Phase 3: 17-22, Phase 4: 23-26)

**Conclusion**: Design approved for implementation. All 4 rounds of review complete.

### Round 5 (2026-06-10): Fresh Independent Production-Grade Review (3-Agent)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Critical findings (2 new)**:
1. **Fallback path `today` undefined** — engine.py integration computed `today_str` but Python fallback referenced `today` → **Fixed**: Unified to `today = self._get_target_date()`, guard before branch, `today_int = int(today)`.
2. **setuptools.backends._legacy may not exist** — Agent verified module missing in setuptools 82 → **Fixed**: Added explicit fallback instruction and blocking Phase 1 gate.

**Major findings (5 new + 3 re-escalated)**:
1. time_to_minute_key quantified ~150-300ms → **Deferred**: ~8-15% of GIL work, acceptable for initial deployment.
2. write_order_file GIL description misleading → **Fixed**: Corrected to "GIL only released during C buffer flush".
3. today_int guard should protect both paths → **Fixed**: Guard moved before branch.
4. Import fallback logs WARNING not INFO → **Fixed**: Changed to WARNING with exception type.
5. setup.py absolute path → **Fixed**: Uses `__file__`-based path.
6. Engine-level integration test missing → **Documented**: Step 16 must include concrete test design.
7. 17-digit timestamp skip categorization → **Deferred**: Current skipped_count + 50% WARNING sufficient.
8. Empty symbol test needs Phase 0 annotation → **Accepted**: Minor documentation addition.

### Round 6 (2026-06-10): Closure Review (3-Agent)

**All 3 agents confirmed: No remaining Critical or Major issues.**

**Post-review fixes applied (from Round 5)**:
- Variable names unified: `today` (str) → guard → `today_int` (int)
- setuptools.backends._legacy explicit fallback added
- write_order_file GIL description corrected
- Import fallback WARNING level with exception type name
- setup.py `__file__`-based absolute path
- Startup log covers all 4 states (enabled, disabled-by-config, disabled-not-available, fail-fast)

**Conclusion**: Design approved for implementation. All 6 rounds (across 2 independent review sessions) complete. Total findings resolved: 15+ Critical, 25+ Major across all rounds.

### Round 7 (2026-06-10): Fresh Independent Production-Grade Review (3-Agent, with source code verification)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3 — empirically verified setuptools, pip, Rust, config.py)

**5 Critical findings (new, not caught by previous 6 rounds)**:
1. **`encoding: &str` in `allow_threads` closure won't compile** — borrows from Python str (!Send), violates Send requirement → **Fixed**: Convert to `encoding.to_string()` before closure.
2. **pyproject.toml build-backend ALREADY BROKEN** — empirically verified: `setuptools.backends._legacy:_Backend` does not exist in setuptools 82. pip install -e . fails → **Fixed**: Override "Do NOT change" instruction. Change to `setuptools.build_meta:__legacy__` as Phase 0 step 1.
3. **No try/except around `_rust_parse_batch`** — exception loses entire batch + kills engine → **Fixed**: Added try/except with Python per-line fallback for that batch.
4. **Encoding guard case-sensitive** — "UTF-8" silently disables Rust → **Fixed**: Changed to `encoding.lower()`.
5. **Empty symbol is PRE-EXISTING bug** — parse_order_line rejects, parse_order_record accepts → **Fixed**: Strengthened Phase 0 description. Use `.strip()` to catch whitespace-only.

**10 Major findings**:
1. PyO3 return needs concrete fallback → **Fixed**: Added flat binary return API sketch (Section 8.4).
2. 18x contention factor is category error → **Fixed**: Corrected to wall-clock ~2-4x for 2 threads (Section 8.1).
3. test_order_csv_byte_identical lacks edge cases → **Fixed**: Extended to include 6-col, 7-col, CRLF, whitespace.
4. Vec<Vec<u8>> input copy not in GIL map → **Fixed**: Added as separate line item (~37-50ms).
5. time_to_minute_key not separate GIL item → **Fixed**: Added as separate line item (~150-300ms).
6. OutputConfig insertion point unspecified → **Fixed**: Specified exact location in config.py.
7. cibuildwheel insufficient → **Deferred**: Phase 4. Added minimal config sketch.
8. No test for corrupted .pyd rollback → **Fixed**: Added test description to Section 7.
9. panic=abort data loss not documented → **Fixed**: Added to risk table.
10. Whitespace-only symbols accepted → **Fixed**: Changed to `.strip()` / `.trim()` check.

### Round 11 (2026-06-10): Fresh Independent Production-Grade Review (3-Agent, Round 11)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Key insight**: The spec has been through 10 rounds of review. This round found **0 new Critical** and **8 new Major** issues — mostly quantitative refinements and operational improvements rather than fundamental design flaws.

**Major findings (8 new)**:
1. **time_to_minute_key integer arithmetic** — `time // 100_000` eliminates ~225-375ms GIL-held string work → **Fixed**: Added as recommended optimization in §5.2.
2. **gc.disable() during batch loop** — Amortizes ~10K gen0 scans → **Fixed**: Added gc.disable/enable pattern in §5.2.
3. **GIL handoff overhead not modeled** — 117 batches × 0-5ms per handoff = 0-585ms → **Fixed**: Added to GIL coverage map (§3.1) and per-batch math (§8.1).
4. **write_order_file GIL-held ~90-95%** — C buffer only flushes every ~12K records, not per write → **Fixed**: Updated §3.1 and §8.1 estimates from ~187ms to ~180-380ms GIL-held.
5. **speed=100 bursty vs speed=1 sustained contention** → **Fixed**: Added explicit note to §8.3 concurrent benchmark requiring sustained artificial load.
6. **enable_order_accel should be in [input] not [output]** → **Fixed**: Moved config to [input] section in §9.2, updated all references.
7. **setup.py silent skip when Rust source exists but build tools missing** → **Fixed**: Added stderr warning in §6.1.
8. **Memory budget ~230-410MB needs validation** → **Fixed**: Added memory budget to §8.1, tracemalloc benchmark to §8.3.

**Re-confirmed (already addressed)**:
- pyproject.toml broken → Phase 0 step 1
- Empty symbol pre-existing bug → Phase 0 step 3
- No CI/CD → Phase 4 gap
- PyO3 5.97M objects → blocking microbenchmark + flat binary fallback
- All 15+ previously resolved Critical/Major items still correctly addressed

**Conclusion**: All 8 Major issues fixed. Design approved for implementation with no remaining Critical or Major issues.

### Round 13 (2026-06-10): Fresh Independent Production-Grade Review (3-Agent, Round 13)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Critical findings (1 new)**:
1. **Flat binary decoder silently returns partial results** — `decode_flat_batch` breaks on corruption, returning partial records → silent data loss. → **Fixed**: Now raises `struct.error`, engine.py try/except falls back to Python per-line parsing.

**Major findings (5 new)**:
1. **setup.py may be bypassed by PEP 660 editable installs** — setuptools 82+ may not invoke setup.py. → **Fixed**: Documented alternative build commands (`python setup.py build_ext --inplace`, `maturin develop --release`).
2. **PyO3 return cost potentially 1.5-2.5s** — Real-world estimate 3× higher than spec's ~468ms. → **Fixed**: Microbenchmark threshold guidance lowered to 0.8s; flat binary may become primary path.
3. **State lock contention not in GIL model** — `self._state.lock` adds unpredictable 10-50ms per batch × 117 batches. → **Already present** in GIL map ("Buffer append + state lock ~200-300ms"). Confirmed adequate.
4. **`_process_parsed_record` signature not enumerated** — `...state_params...` pseudocode only. → **Fixed**: Explicit parameter list with 14 parameters and return tuple.
5. **Warmup self-test runs AFTER startup log** — Misleading telemetry on failure. → **Fixed**: Warmup now runs BEFORE startup log.

**Minor fixes**: Test count "379+" → "380+"; GIL handoff expected ~293ms note; psutil RSS measurement added; PyO3 transient memory in budget.

**Conclusion**: All 1 Critical + 5 Major issues fixed. No regressions from prior 12 rounds. Design approved for implementation.

### Round 15 (2026-06-11): Fresh Independent Production-Grade Review (3-Agent, Round 15)

**Agents**: Performance/GIL (Agent 1), Correctness (Agent 2), Build/Deploy (Agent 3)

**Critical findings (0 new)**: All previously identified Critical items confirmed correctly addressed. No new Critical issues found by any agent.

**Major findings (3 genuinely new)**:
1. **`pending_shared_orders` state-lock is batch-scoped, not record-scoped** — Engine.py lines 748-763 process ALL accumulated orders per batch under `self._state.lock`. Including this inside `_process_parsed_record` would cause 747K lock acquisitions instead of ~117. → **Fixed**: Refactored `_process_parsed_record` to only APPEND to `pending_shared_orders`; state-lock processing remains in batch-level caller code. Added explicit batch-scoped vs record-scoped documentation.
2. **`drain_count` is batch-scoped, not record-scoped** — Engine.py line 666 increments per batch, not per record. Including it in `_process_parsed_record` is misleading. → **Fixed**: Removed `drain_count` from `_process_parsed_record` parameter list and return tuple. It remains as a local variable in the batch loop.
3. **`PyBackedBytes` not viable with `allow_threads()`** — Borrowed Python references are `!Send`, incompatible with GIL-released code. Spec listed it as "future optimization" but it's actually impossible. → **Fixed**: Removed "future optimization" mention; explicitly ruled out with explanation in GIL map and benchmark plan.

**Re-confirmed (already addressed, not re-escalated)**:
- PyO3 5.97M objects cost — blocking microbenchmark + flat binary fallback (confirmed by all 3 agents)
- pyproject.toml broken — Phase 0 step 1 (empirically verified by Agent 3)
- PEP 660 bypasses setup.py — documented in Section 6.1 (Agent 3 re-confirmed)
- gc.disable() process-wide — documented in Section 4.4 (Agents 1 and 3 re-confirmed)
- 17-digit timestamp guard — by design, documented (Agent 2 re-confirmed parity)
- _process_parsed_record pseudocode — highest-risk refactoring point (all agents agree)

**Conclusion**: 0 Critical, 3 Major (all documentation/specification precision fixes for `_process_parsed_record`). All 3 Major issues fixed. Design approved for implementation.

---

## Appendix B: Implementation Plan 关键建议

> 以下建议来自 15 轮 45+ agent 设计评审的综合结论，供 `writing-plans` skill 直接引用。

### 最高风险项

1. **`_process_parsed_record` 重构** — 最高风险步骤
   - 必须从 engine.py lines 681-765 提取，13 参数 + 4 返回值
   - **batch-scoped vs record-scoped 必须严格区分**：
     - Record-scoped（放入 `_process_parsed_record`）：cross-day flush, late-order detection, record-driven flush, buffer append, watermark update, `pending_shared_orders.append`
     - **Batch-scoped（必须留在 batch loop）**：`pending_shared_orders` state-lock 处理 (lines 748-763), `drain_count` increment (line 666), periodic flush check (lines 767-782)
   - 重构完成后、添加 Rust 代码前，**必须通过 380+ 测试验证**
   - 如果 `_process_parsed_record` 错误地将 batch-scoped 逻辑放入 per-record 函数，会导致 747K lock acquisitions（而非 ~117）

### 类型与语义约束

2. **`minute_key = str(record.time // 100_000)`** — **必须返回 `str`**，不能返回 `int`。下游代码 (writer.py, flusher.py, tickfile.py) 全部做字符串切片 (`minute_key[:8]`, `minute_key[8:12]`)
3. **`enable_order_accel` 放在 `[input]` section (InputConfig)**，NOT `[output]` (OutputConfig)。Rust acceleration 是 parse feature，与 `order_chunk_size_bytes` 和 `file_encoding` 同属 input 配置
4. **Rust `time` 字段使用 `i64`，NOT `u64`** — Python `int()` 接受负数，`u64` 不接受
5. **Flat binary `'<H>'` unsigned short** — NOT `'<h>'` signed。匹配 Rust `u16`
6. **Seqno 在 date check 之后递增** — 所有 3 条代码路径 (Rust success, Rust fallback, Python fallback) 都必须 `date_check → seqno += 1`，不能反过来

### 构建与部署

7. **Phase 0 step 1: 修复 pyproject.toml** — `build-backend` 改为 `setuptools.build_meta:__legacy__`。当前 `setuptools.backends._legacy:_Backend` 在 setuptools 82+ 不存在，`pip install -e .` 会失败
8. **PEP 660 会绕过 setup.py** — 如果 `pip install -e .` 未构建 Rust extension，使用 `python setup.py build_ext --inplace` 或 `maturin develop --release` 作为替代
9. **不要实现 batch join for writer.py** — 遵循 spec Section 5.3 "DO NOT change write_order_file"。当前 streaming write 已经在 I/O syscall 时释放 GIL；batch join 反而增加 GIL-held 时间
10. **Phase 4: 在 csv_parser.py 添加 `set_rust_available(False)` 函数** — warmup self-test 失败时通过此函数更新模块状态，不要直接修改模块变量

### 性能验证

11. **PyO3 微基准测试是 blocking gate** — 747K 8-tuple 返回必须 <1.0s（推荐 <0.8s）。如果超过，flat binary path (Section 8.4) 成为主路径
12. **并发基准测试必须使用持续负载** — 不能只依赖 speed=100 E2E（bursty contention）。必须用人工持续负载（order + snapshot threads 同时 parse 60s）
13. **`PyBackedBytes` 不可行** — 借用的 Python 引用是 `!Send`，无法在 `allow_threads()` 内使用。输入转换的 `Vec<Vec<u8>>` 拷贝不可避免
14. **`gc.disable()` 是进程级** — 影响所有线程。`try/finally` 确保及时恢复。concurrent benchmark 必须测量 snapshot thread 在 disable 窗口内的 RSS 影响

### Rust/Python 一致性

15. **17-digit timestamp guard 是 by design** — Rust 拒绝非 17-digit 时间戳，Python 接受。这是有意为之的安全措施，防止 `str[:8]` vs `//10^9` 对非标准时间戳产生不同日期
16. **Python fallback 是 EXACT original code** — 使用 `str(record.time)[:8]`（不是 `//10^9`）。Rust path 才使用 `//10^9`（有 17-digit guard 保护）
17. **`encoding.lower()` guard** — ConfigParser 不会 lowercase 值。"UTF-8" 必须也能匹配

### 上线前 Checklist

18. **Empty symbol fix (Phase 0 step 3)** — `csv_parser.py` 添加 `if not fields[0].strip(): return None`。这是 pre-existing bug（slow path 拒绝空 symbol，hot path 接受）。必须先 audit 生产数据
19. **启动日志 4 个状态** — ENABLED, DISABLED-by-config, fail-fast RuntimeError, DISABLED-not-available
20. **Rollback** — `enable_order_accel = false` + 删除 .pyd + 重启
21. **Order CSV 内容一致性校验** — Rust path 和 Python path 生成相同分钟必须 byte-identical
22. **Benchmark pass/fail 阈值** — peak minute <60s (hard), <30s (target), Rust parse >1M rec/s
