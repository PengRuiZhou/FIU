# Phase 21 (Sub-phases a+b+c) Design: Rust Order Full Batch + Rust Snapshot Parse+OHLCV + Rust Tickfile

> **Date**: 2026-06-11
> **Status**: In Review
> **Note**: This document covers all three sub-phases. Naming cross-reference: Phase 4 GIL Analysis maps: Phase 21 (A) = Phase 21a+b+c; Phase 22 (E) = Phase 21b; Phase 23 = Phase 21c. Phase 4 Table 8.4 "Phase 21 → 3-5s" = Phase 21a+b+c combined outcome. Each ships independently: Phase 21a (Order) → Phase 21b (Snapshot) → Phase 21c (Tickfile). See Section 12 for rollout details.
> **Parent**: `docs/superpowers/specs/2026-06-11-phase4-e2e-gil-analysis.md`
> **Related**: `docs/superpowers/specs/2026-06-10-rust-order-accel-design.md`

---

## 1. Summary

Phase 4 E2E GIL Analysis identified that **three Python per-record paths hold the GIL simultaneously**:
- Order thread: `_process_parsed_record` + `OrderRecord` creation (~1.2s GIL/0900s)
- Snapshot thread: `parse_snapshot_line` + OHLCV aggregation (~135s GIL全天)
- Tickfile Writer: reads `raw_order_buffers` + `latest_order_by_symbol` under GIL

Phase 21 A+B moves all three onto Rust, eliminating GIL contention entirely.

**Scope:**
- **A**: Rust Order full batch — parse + date check + seqno + minute_key group-by + late order detection + buffer management
- **B**: Rust Snapshot parse + OHLCV aggregation (parse and aggregate both in Rust)
- **C**: Rust Tickfile generation (Tickfile Writer also moved to Rust)

**Goal**: 0900 peak minute: 180s → 3–5s wall-clock.

---

## 2. Root Cause Recap

From Phase 4 analysis:
- Order thread Rust parsing releases GIL (503K rec/s)
- But `_process_parsed_record` + `OrderRecord(seqno=...)` requires GIL
- Snapshot data thread holds GIL for `parse_snapshot_line` + `process_snapshot` (pure Python)
- Result: order thread spends 99% of time waiting for GIL

Three-thread GIL competition:
```
Order Thread (GIL held: ~1.2s/0900s)
  └─ OrderRecord() + _process_parsed_record per record

Snapshot Thread (GIL held: ~135s全天，其中约 80% 集中在 0900 前后峰值 10 分钟窗口)
  └─ parse_snapshot_line() + OHLCV aggregation per record

Tickfile Writer (GIL held: ~422ms/0900s，峰值可膨胀至 ~1,281ms)
  └─ reads raw_order_buffers + latest_order_by_symbol under GIL
```

---

## 3. Design Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Late order detection location | Rust internal | Rust receives `_flushed_order_minutes` set from Python before each batch; Python passes complete accumulated set (all historical flushed minutes); Rust does late detection in HashSet lookup |
| Snapshot batch state (base_vol/amt) | Pass through call chain | Rust `aggregate_snapshot_batch` receives base values, returns updated values; Python passes back on next call |
| Tickfile state sync | Atomic dict swap for `latest_order`; Rust-held buffer for `raw_order_buffers` | Rust returns flat binary `latest_order`; Python decodes and atomically replaces `SharedState.latest_order_by_symbol` reference. `raw_order_buffers` remain in Rust; Python tickfile thread calls `tickfile_get_raw_buffer(minute_key)` to retrieve (Rust returns and clears) |
| Tickfile Writer location | CSV generation in Rust; disk write in Python | Phase C moves CSV generation to Rust; Python thread remains responsible for `file.write()` (~50ms GIL per minute) |
| Regression strategy | Golden snapshot tests + field-level tolerance | 3 new golden output tests (order batch, snapshot OHLCV, tickfile CSV) + field-level float tolerance comparison for OHLCV + existing 389 tests |

---

## 4. Part A — Rust Order Full Batch

### 4.1 New Rust Functions

```rust
/// Main entry point: process a batch of raw order CSV lines entirely in Rust.
///
/// Panic safety: all code paths wrapped in std::panic::catch_unwind.
/// On panic, returns Err with boxed error string (Python catches as exception).
///
/// **Seqno assignment**: Starts at 0, increments by 1 per valid (non-skipped,
/// date-matching) record. Skipped/invalid records do NOT consume seqno values.
/// Seqno is global across the trading session (not per-batch).
///
/// Parsing (GIL released):
///   - UTF-8 decode + split + 8-field parse
///   - Line ending normalization: CRLF→LF, CR→LF
///   - UTF-8 BOM detection and skip
///   - Date check: str(time)[:8] == today_str (must use STRING SLICE, NOT integer division)
///     IMPORTANT: Python uses str(record.time)[:8]. Rust must match exactly:
///     time = 20260528090000123 → str[:8] = "20260528" (12 chars for minute_key[:12])
///     Do NOT use time // 100_000 (produces wrong string for minute_key!)
///   - Seqno assignment (after date check passes, one seqno per valid record)
///   - minute_key: str(time)[:12] (12-character string slice, NOT integer division)
///
/// Grouping (GIL held only for HashMap ops):
///   - minute_key → Vec<RecordFlat> grouping (preserve original file order via IndexMap)
///   - Late order detection: if minute_key in flushed_minutes HashSet
///   - latest_order per symbol: track max (time, rcvtime)
///
/// Cross-day handling:
///   - prev_date: i64 (previous batch's date, 0 if first batch)
///   - If record_date != prev_date and prev_date != 0: Rust clears internal flushed_minutes AND raw_order_buffers HashMap
///   - This handles cross-day reset identically to Python _process_parsed_record
///   - ⚠️ Cross-day raw_order_buffers cleanup: stale entries from a previous trading day must not be retrieved by tickfile_get_raw_buffer
///
/// Late order cap (Python-side):
///   - Rust routes ALL late orders to late_order_buf (no per-minute cap in Rust)
///   - Python side enforces _max_late_order_records cap (unchanged)
///
/// Returns (GIL held for trivial tuple construction):
///   - per_minute_buf: flat binary per minute (magic: 0xAA 0xBB 0xCC 0x01)
///   - late_order_buf: flat binary for late orders (magic: 0xAA 0xBB 0xCC 0x02)
///   - latest_order_buf: flat binary latest_order per symbol (magic: 0xAA 0xBB 0xCC 0x03)
///   - late_minute_keys: Vec<String> for tickfile routing
///   - skipped: u64
#[pyfunction]
fn process_order_batch(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
    today_str: &str,
    today_int: i64,
    prev_date: i64,
    flushed_minutes: Vec<String>,
) -> PyResult<(
    Vec<u8>,       // per_minute_buf (magic 0xCC01)
    Vec<u8>,       // late_order_buf (magic 0xCC02)
    Vec<u8>,       // latest_order_buf (magic 0xCC03)
    Vec<String>,   // late_minute_keys
    u64,           // skipped count
)>
```

### 4.2 Flat Binary Formats

**Magic header (all buffers):**
```
[4 bytes: magic = 0xAA 0xBB 0xCC {version_byte}]
[2 bytes: version u16 LE = 1]
[4 bytes: schema_hash u32 LE]
```
If magic or version mismatches, Python raises `ValueError` immediately.

**Schema hash definition**: `schema_hash = CRC32(field_name_1 + field_type_1 + field_name_2 + ...)` over all fields in the record in order. This detects field layout changes (added/removed/reordered fields). Algorithm: ASCII concatenation of `"{name}:{type},"` for each field in order, **including the trailing comma after the last field**, then CRC32 of the resulting string. Example for order record: `"symbol:str,time:i64,bidprice:i64,bidsize:i64,askprice:i64,asksize:i64,decimal:i64,rcvtime:i64,"` → CRC32. The trailing comma is part of the hash input — Python `zlib.crc32()` and Rust `crc32fast` must both include it.

> **⚠️ P0 PREREQUISITE — DO NOT DEPLOY WITHOUT THIS**: CRC32 test vectors below are PLACEHOLDERS (not real computed values). Before Phase 21a deployment, compute actual CRC32 of the field layout string using the algorithm above, store verified values in `tests/test_schema_hash_parity.py`, and run in CI. See Section 9 P0 Prerequisites for full implementation steps. If deployed with placeholders, every batch will raise `ValueError("schema_hash mismatch")` and silently fall back to Python path — losing all Phase 21 performance gains with no error visible to operators.

**Test vectors** (must match in both Rust and Python, verified in CI):
  Order record schema: CRC32 = 0x8A1B3C4D
  Snapshot record schema: CRC32 = 0x5D6E7F8A
  OHLCV entry schema: CRC32 = 0x1A2B3C4D
CI must verify these values match on both Windows and Linux before deployment. Schema_hash values (0x8A1B3C4D etc.) are placeholders. Implementation must compute actual CRC32 values using the exact algorithm (Python `zlib.crc32` over ASCII field-layout string) and verify both Windows and Linux produce identical output. Add `test_schema_hash_parity.py` to CI.
  latest_snapshot_buf schema: CRC32 = 0xE5F6A7B8

**Version upgrade strategy**: Increment version byte on any format change. Python validates version before decoding. If version > Python's max supported version, raise `ValueError` with version mismatch message. Rust and Python must be deployed together (no version drift tolerated in production).

**Per-minute buffer** (grouped by minute_key, original file order preserved):
```
[4 bytes: magic 0xCC 0x01][2 bytes: version = 1][4 bytes: schema_hash]
[2 bytes: minute_key_len u16 LE][minute_key_len bytes: minute_key UTF-8]
[4 bytes: record_count u32 LE]
for each record:
  [2 bytes: symbol_len u16 LE][symbol_len bytes: symbol]
  [8 bytes: time i64 LE]
  [8 bytes: bidprice i64 LE]
  [8 bytes: bidsize i64 LE]
  [8 bytes: askprice i64 LE]
  [8 bytes: asksize i64 LE]
  [8 bytes: decimal i64 LE]
  [8 bytes: rcvtime i64 LE]
```
~90 bytes/record. 747K records ≈ 67MB.

**Latest-order buffer** (one entry per symbol, max ~4505):
**IMPORTANT**: Must contain all fields needed by `build_tickfile_row` and `recover_tickfile_seqno`:
```
[4 bytes: magic 0xCC 0x03][2 bytes: version = 1][4 bytes: schema_hash]
for each entry:
  [2 bytes: symbol_len u16 LE][symbol_len bytes: symbol]
  [8 bytes: time i64 LE]
  [8 bytes: bidprice i64 LE]    // tickfile build_tickfile_row needs these
  [8 bytes: bidsize i64 LE]
  [8 bytes: askprice i64 LE]
  [8 bytes: asksize i64 LE]
  [8 bytes: decimal i64 LE]
  [8 bytes: rcvtime i64 LE]
  [8 bytes: seqno i64 LE]        // for tickfile recovery (fields[59])
```
~66 bytes/entry. ~297KB total for 4505 symbols.


### 4.3 Late Order Detection + Panic State Management + Cross-Day Reset (Rust Internal)

**Late Order Detection**:
- `flushed_minutes: Vec<String>` passed from Python `_flushed_order_minutes` set before each batch
- Python passes the **complete accumulated set** — all minutes ever flushed in this run (not just newly flushed ones)
- Rust builds `HashSet<String>` once per batch call (microseconds)
- For each record: if `minute_key in flushed_minutes` → route to `late_order_buf`
- Rust returns `late_minute_keys: Vec<String>` so Python knows which minute keys had late orders (for tickfile trigger routing)
- Late order cap (`_max_late_order_records`) is enforced by Python (unchanged), not by Rust
- `prev_date` parameter handles cross-day reset: if `record_date != prev_date && prev_date != 0`, Rust clears its internal flushed_minutes HashSet (equivalent to Python `_flushed_order_minutes.clear()`)

**Panic Recovery — raw_order_buffers State Reset**:
If `process_order_batch` panics, Rust's internal `raw_order_buffers` HashMap may be in a partially-updated state. On panic return, Python must call `rust_reset_state()` (or equivalent panic recovery function) to clear Rust's internal `raw_order_buffers`, `flushed_minutes`, and any other per-batch state before resuming. Without this, subsequent Part A calls may operate on stale/corrupted buffers. The panic recovery function must be implemented as a Rust `#[pyfunction]` callable from Python.

- `flushed_minutes: Vec<String>` passed from Python `_flushed_order_minutes` set before each batch
- Python passes the **complete accumulated set** — all minutes ever flushed in this run (not just newly flushed ones)
- Rust builds `HashSet<String>` once per batch call (microseconds)
- For each record: if `minute_key in flushed_minutes` → route to `late_order_buf`
- Rust returns `late_minute_keys: Vec<String>` so Python knows which minute keys had late orders (for tickfile trigger routing)
- Late order cap (`_max_late_order_records`) is enforced by Python (unchanged), not by Rust
- `prev_date` parameter handles cross-day reset: if `record_date != prev_date && prev_date != 0`, Rust clears its internal flushed_minutes HashSet (equivalent to Python `_flushed_order_minutes.clear()`)

### 4.4 Python-Side Processing After Rust Call

**Panic safety**: Python must call Rust FFI inside `try/except`. On Rust panic, log error and fall back to Python path.

**Important — peak minute decode cost**: `per_minute_buf` decode is NOT negligible at 0900 peak.
- 0900 peak: 747,434 records across ~90 minute_keys → ~8,300 records/minute_key
- At 0.5μs per OrderRecord decode: ~8,300 × 0.5μs = ~4.2ms per minute_key
- Total peak order decode GIL: 747,434 × 0.5μs = **~374ms** (not "~25μs" which is the daily average)
- This is significant but still vastly better than Phase 4's 180s wall-clock

For each minute_key in per_minute_buf:
1. Python decodes flat binary → creates `List[OrderRecord]` (NamedTuple, ~50 entries average per minute_key, ~8,300 at peak)
   - **Peak decode cost**: ~4.2ms per minute_key at 0900, ~374ms total for all minute_keys
   - Decode is GIL-held but parallelizable across threads
2. Python calls `_flush_order_minute` (existing file-writing logic, per-minute boundary)
3. Python enqueues tickfile trigger (if tickfile enabled)

**Tickfile raw_order_buf**: The `raw_order_buf` bytes are passed directly to Rust `tickfile_generate`. Python does NOT decode `raw_order_buf` for tickfile purposes. Python decodes `per_minute_buf` only for `_flush_order_minute` file writing.

For late orders:
1. Python decodes late_order_buf (typically < 100 records)
2. Writes to per-minute order file via `append_order_records` (existing late-order path)
3. Python enforces `_max_late_order_records` cap (unchanged logic)

For latest_order:
1. Python decodes latest_order_buf → new dict
2. Atomically replaces `SharedState.latest_order_by_symbol` reference
3. Tickfile writer sees either old or new dict (no partial updates)

**Python `prev_date` State Management**:
Python must track `prev_date` across Part A calls. After each successful `process_order_batch` call, Python stores the date of the last processed record as the new `prev_date`. If Part A panics and Python falls back to Python path, Python retains the last successful `prev_date` value and passes it on the next Part A call. This prevents cross-day boundary detection from failing after a panic fallback.


### 4.5 GIL Time Comparison

**Phase 21a (Order only, Part A)**

| Step | Current (per chunk) | Phase 21a (per chunk) |
|------|---------------------|----------------------|
| Rust parse | 12ms (GIL released) | 12ms (GIL released) |
| Date check + seqno + minute_key | ~3ms (GIL held) | **0ms (in Rust)** |
| OrderRecord creation | ~6ms (GIL held) | **~0.05ms (decode flat binary)** |
| buf.records.append | ~0.7ms (GIL held) | **0ms (Rust-side buffer)** |
| raw_order_buffers append | ~2ms (GIL held) | **0ms (Rust-side flat buffer)** |
| latest_order_by_symbol decode | ~0.5ms (GIL held) | **~0.1ms (batch decode)** |
| **Total order thread GIL** | **~12ms/chunk** | **~0.2ms/chunk** |
| **747K records (100 chunks)** | **~1.2s GIL time** | **~0.02s GIL time** |

> **Peak order decode (0900)**: ~374ms GIL total (747K × 0.5μs) — see Section 4.4. This is significant but vastly better than Phase 4's 180s wall-clock.

**Phase 21b (Order + Snapshot, Part A + Part B) — snapshot GIL breakdown**

| Step | Current (per chunk) | Phase 21a only | Phase 21a+b (A+B) |
|------|---------------------|----------------|---------------------|
| Rust parse_snapshot_batch | ~15ms (GIL held) | ~15ms (GIL held) | **~15ms (GIL released)** |
| Rust aggregate_snapshot_batch | N/A | N/A | **~5ms (GIL released)** |
| Python decode ohlcv_buf | N/A | **~1ms (GIL held)** | **~1ms (GIL held)** |
| Python _flush_ohlcv_minute | ~10ms (GIL held) | **~10ms (GIL held)** | **~10ms (GIL held, unchanged)** |
| **Total snapshot GIL per chunk** | **~25ms** | **~26ms** | **~11ms** |

Phase 21a **alone does NOT reduce snapshot GIL** (snapshot stays at ~26ms/chunk because Python still parses+aggregates).
Phase 21a+b reduces snapshot GIL from ~25ms to ~11ms per chunk (56% reduction).
At 0900 peak (~1,400 chunks): Phase 21a snapshot GIL ≈ ~36s, Phase 21a+b snapshot GIL ≈ ~15s.

**Phase 21a+b+c (Order + Snapshot + Tickfile, Part A + B + C) — tickfile GIL**

| Step | Phase 21a+b | Phase 21a+b+c |
|------|-------------|----------------|
| tickfile_generate (4505 symbols) | ~0ms (Python CSV gen ~120ms GIL) | **~50ms (GIL held, PyO3 FFI)** |
| Python disk write | ~50ms | **~50ms (unchanged)** |
| **Total tickfile GIL per minute** | **~170ms** | **~100ms** |

Tickfile GIL in Phase 21a+b+c (~100ms) is WORSE than Phase 21a+b (~50ms) because Rust CSV gen holds GIL via PyO3 FFI. This is ACCEPTABLE because: (a) tickfile is background thread, not on 0900 critical path; (b) Phase 23 async I/O eliminates Python disk write GIL. Still vastly better than Phase 4's ~1,281ms peak.

**Wall-clock model for 0900 peak** (Phase 21a+b+c):
- Order Rust parse: ~1.5s (GIL released, parallel)
- Order Python decode+flush: ~0.4s (374ms decode + ~30ms flush, parallel with snapshot)
- Snapshot Rust parse+agg: ~21s (GIL released, parallel with order) ⚠️ **Sequential constraint in snapshot thread**
- Snapshot Python flush: ~14s (parallel with order, ~10ms/chunk × 1,400 chunks) ⚠️ **Sequential constraint — hard floor**
- Tickfile Rust CSV gen: ~0.05s (GIL held, ~50ms PyO3 FFI)
- Tickfile Python disk write: ~0.05s (sequential after order+minute boundary)
- **Estimated wall-clock: 22-35s assuming dedicated CPU cores.**
- **⚠️ 3-5s target requires A+B+C combined + snapshot Rust work faster than modeled OR snapshot Python flush also moved to Rust.** Phase 21a alone (~21s wall-clock) passes 60s SLA but misses 3-5s target by 4-7×.
- **Sensitivity**: under 75% CPU availability wall-clock ~28-45s; under 50% CPU availability wall-clock may reach 50-60s (approaching SLA boundary). **Optimization pipeline**: regardless of 实测 result (whether 3-5s, 6-60s, or > 60s), Phase 22 snapshot optimization proceeds — the ultimate goal is to achieve < 60s wall-clock. Phase 23 tickfile async I/O begins only after Phase 22 achieves wall-clock < 60s. **Phase 21a alone**: If实测 > 60s: ROLLBACK per Section 9 gate; if实测 = 6-60s: investigate but proceed to Phase 21b (snapshot Rust).
- **Per-component wall-clock budget**: Order thread (~2s) + Snapshot thread (~35s sequential within snapshot thread) = ~35s minimum wall-clock floor.
- **Sequential constraint**: snapshot Rust parse+agg (~21s) + Python flush (~14s) are sequential within the snapshot thread — this is the dominant constraint, NOT GIL contention.
- **CPU contention risk**: wall-clock may approach 60s SLA boundary at lower CPU availability.


---

## 5. Part B — Rust Snapshot Parse + OHLCV Aggregation

### 5.1 New Rust Functions

Two-phase approach for snapshot (matches the Python structure):

**Phase 1: Parse (GIL released)**

**Panic safety**: all `#[pyfunction]` entries wrapped in `std::panic::catch_unwind`. On panic, returns `Err` with boxed error string.

```rust
struct SnapshotParsed {  // mirrors Python SnapshotRecord fields exactly
    symbol: String,
    time: i64,
    seqno: i64,  // assigned by Python aggregator
    rcvtime: i64,
    preclose: i64,  // raw integer from parse_snapshot_line
    lastprice: i64,   // lasttradeprice from CSV; OHLCV uses this for price
    lasttradeqty: i64, totalvol: i64, totalamount: i64,
    sessionid: i64, tradetype: String, status: String, direction: i64,
    pflag: String, decimal: i64, vwap: i64, shortsellflag: i64,
}

#[pyfunction]
fn parse_snapshot_batch(
    py: Python,
/// Empty line handling: skip empty/whitespace-only lines after decoding. Must match Python parse_snapshot_line behavior.
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<SnapshotParsed>, u64)>
```


**Phase 2: Aggregate (GIL released)**

**Panic safety**: wrapped in `std::panic::catch_unwind`.

```rust
#[pyfunction]
fn aggregate_snapshot_batch(
    py: Python,
    records: Vec<SnapshotParsed>,
    current_minute: &str,
    base_vol_by_symbol: Vec<(String, i64)>,
    base_amt_by_symbol: Vec<(String, f64)>,
    flushed_minutes: Vec<String>,
) -> PyResult<(
    Vec<u8>,              // ohlcv_buf flat binary (magic 0xCC 0x04)
    String,                // new_current_minute (last seen)
    Vec<(String, i64)>,   // updated base_vol_by_symbol
    Vec<(String, f64)>,   // updated base_amt_by_symbol
    Vec<String>,           // late_minute_keys
)>
```

**Float precision**: `amount = ((totalamount - base_amt) / 10^decimal)` uses IEEE 754 `f64`. Golden tests must use `abs(diff) <= 1e-6` tolerance for open/high/low/close/volume/amount fields. Cumulative floating-point error in `base_amt_by_symbol` is acknowledged; tolerance-based comparison absorbs it.

**Benchmark target**: `aggregate_snapshot_batch` must process ~1,400 snapshot chunks at 0900 peak in < ~5ms/chunk GIL released (total ~7s for all chunks). If measured > 10ms/chunk, Phase 21a+b wall-clock exceeds 60s SLA — trigger Phase 22 snapshot optimization before Phase 23.

### 5.2 OHLCV Flat Binary Format

**Magic header** (same versioning scheme as order buffers):
```
[4 bytes: magic = 0xAA 0xBB 0xCC 0x04]
[2 bytes: version u16 LE = 1]
[4 bytes: schema_hash u32 LE]
```
Schema hash definition: same CRC32(field_name:field_type,) algorithm as order buffers.

**OHLCV per-symbol entry**:
```
[2 bytes: minute_key_len u16 LE][minute_key_len bytes: minute_key UTF-8]
[2 bytes: symbol_len u16 LE][symbol_len bytes: symbol]
[8 bytes: open f64 LE]
[8 bytes: high f64 LE]
[8 bytes: low f64 LE]
[8 bytes: close f64 LE]
[8 bytes: volume i64 LE]
[4 bytes: count u32 LE]
[4 bytes: decimal u32 LE]
```
~63 bytes/symbol. 4505 symbols ≈ 284KB/minute.


### 5.2b `latest_snapshot_buf` Flat Binary Format

For tickfile generation in Part C, the snapshot's latest state is needed. This buffer holds the most recent SnapshotRecord per symbol.

> ⚠️ **Type consistency note**: `SnapshotParsed.preclose` is `i64` (raw integer from CSV). `latest_snapshot_buf.preclose` is stored as `f64` (pre-scaled, already divided by `10^decimal`). When `tickfile_generate` reads `preclose` from `latest_snapshot_buf`, it must NOT re-apply decimal scaling — the value is already scaled. If `preclose` requires decimal scaling in Python `build_tickfile_row`, confirm the scaled-vs-raw convention before implementation. Same applies to `lastprice` / `open` / `high` / `low` / `close` — if stored as `f64` they are pre-scaled; if stored as `i64` they need decimal scaling at use time.

**Format** (same magic/version/schema_hash scheme):
```
[4 bytes: magic = 0xAA 0xBB 0xCC 0x05]
[2 bytes: version u16 LE = 1]
[4 bytes: schema_hash u32 LE]
[4 bytes: entry_count u32 LE]
for each entry:
  [2 bytes: symbol_len u16 LE][symbol_len bytes: symbol]
  [8 bytes: time i64 LE]
  [8 bytes: preclose f64 LE]    ← pre-scaled (already divided by 10^decimal); NOT raw integer
  [8 bytes: lastprice f64 LE]   ← pre-scaled
  [8 bytes: open f64 LE]         ← pre-scaled (from OHLCV aggregation)
  [8 bytes: high f64 LE]         ← pre-scaled (from OHLCV aggregation)
  [8 bytes: low f64 LE]          ← pre-scaled (from OHLCV aggregation)
  [8 bytes: close f64 LE]        ← pre-scaled (from OHLCV aggregation)
  [8 bytes: totalvol i64 LE]
  [8 bytes: totalamount f64 LE]
  [8 bytes: decimal u32 LE]
```

Python decoder: `decode_snapshot_buf(buf: bytes) -> Dict[str, SnapshotData]`. Must validate magic/version/schema_hash before decoding.


### 5.3 Batch State Propagation

```
Python                    Rust
======                    ====
base_vol/amt_by_symbol ──► aggregate_snapshot_batch()
                              │
                              ├── OHLCV aggregation in Rust (HashMap<symbol, OHLCVAggregate>)
                              │
                          returns (ohlcv_buf, new_current_minute,
                                   updated_base_vol, updated_base_amt, late_minute_keys)
                              │
◄──── next call passes back ──┘
```

- `current_minute` advances monotonically; passed to Rust so it can detect minute boundaries
- `flushed_minutes`: Python passes **complete accumulated set** of all flushed minute keys (same as Order — not just newly flushed ones). Rust uses it to route late snapshots to `late_minute_keys`.


> **Panic rollback for Part B**: If aggregate_snapshot_batch panics, Python must:
> 1. NOT store the returned `updated_base_vol/amt` — restore from rollback copy of previous batch's values
> 2. NOT add the returned `late_minute_keys` to Python's `flushed_minutes` set
> 3. Clear Python's `flushed_minutes` set before resuming Python path (same as Part A)
> **State retention**: Python retains the pre-call rollback copy of base_vol/amt_by_symbol dicts — do NOT discard them. The rollback copy serves as the working state for the next call. On the next successful Rust call, the returned updated values replace the rollback copy.
> A panic before return leaves return values partially constructed. Python caller must keep a rollback copy of the previous batch's base values AND flushed_minutes state before each Rust call.

- `base_vol_by_symbol` / `base_amt_by_symbol`: Python dict converted to `Vec<(String, i64/f64)>` each call. Cost: 4505 iterations × ~1400 batches ≈ 6.3M dict ops per 0900 minute. ⚠️ The ~5-10ms estimate assumes ~100ns/op; at ~100ns/op actual cost ≈ 630ms. This is Python GIL work and is on the snapshot thread critical path. Measure before deployment; if >50ms total, consider caching the vector between calls.

### 5.4 OHLCV Aggregation Logic (Rust)

Matches existing `OHLCVAggregate.update()`:
```rust
fn update_aggregate(agg: &mut OHLCVAggregate, record: &SnapshotParsed, base_vol: i64, base_amt: f64) {
    let d = 10_f64.powi(record.decimal);
    let price = record.lastprice as f64 / d;
    if agg.count == 0 {
        agg.open = price;
        agg.start_totalvol = record.totalvol;
        agg.start_totalamount = record.totalamount as f64;
    }
    agg.high = agg.high.max(price);
    agg.low = agg.low.min(price);
    agg.close = price;
    agg.end_totalvol = record.totalvol;
    agg.end_totalamount = record.totalamount as f64;
    agg.count += 1;
    agg.seqno = record.seqno;
    agg.decimal = record.decimal;
    if record.lasttradeqty > 0 {  // reserved for future use: set to true if any trade qty > 0 in this aggregation window; OHLCVEntry output format does not yet include this field — implement its consumer before enabling
        agg.any_lasttradeqty_positive = true;
    }
    agg.volume = (record.totalvol - base_vol).max(0);
    agg.amount = ((record.totalamount as f64 - base_amt) / d).max(0.0);
}
```

### 5.5 Python-Side Processing After Rust Call

**Panic safety**: Python must call Rust FFI inside `try/except`. On Rust panic, fall back to Python path.

1. Decode `ohlcv_buf` flat binary → `OHLCVAggregate` objects
   - Validate magic bytes and schema_hash before decoding
2. Call existing `_flush_ohlcv_minute` with decoded aggregates (per-minute file writing)
3. Handle late snapshots via `late_minute_keys` (existing late snapshot queue)
4. Store updated `base_vol/amt_by_symbol` for next batch call

---

## 6. Part C — Rust Tickfile Generation

### 6.1 New Rust Functions

```rust
/// Generate tickfile CSV for one minute in Rust.
///
/// **NOT trivial computation.** Performs per-symbol CSV formatting:
///   - 4505 symbols × 28 columns
///   - float division for decimal scaling
///   - string concatenation and formatting
/// GIL is held for the full duration (~50ms for 4505 symbols).
///
/// Panic safety: wrapped in std::panic::catch_unwind. On panic, returns Err.
///
/// Inputs:
///   - raw_order_buf: per-minute flat binary from process_order_batch
///   - latest_order_buf: latest_order flat binary from process_order_batch
///     (contains bidprice/bidsize/askprice/asksize/decimal fields needed by build_tickfile_row)
///   - latest_snapshot_buf: latest snapshot flat binary from Rust snapshot state
///     (needed for snapshot-mode tickfile: preclose/open/high/low/totalvol/totalamount/decimal)
///   - minute_key: the minute being generated
///
/// Output:
///   - CSV string (one line per symbol with tickfile fields)
///
/// Algorithm (matches Python build_tickfile_row):
///   - For each symbol: look up latest_order from latest_order_buf
///   - If snapshot mode: also look up latest_snapshot from latest_snapshot_buf
///   - Format all fields (including NA fallbacks, decimal scaling)
///   - Sort by symbol
///   - Return CSV string
#[pyfunction]
fn tickfile_generate(
    py: Python,
    raw_order_buf: Vec<u8>,
    latest_order_buf: Vec<u8>,
    latest_snapshot_buf: Vec<u8>,
    minute_key: &str,
) -> PyResult<String>
```

**Error Handling**: `tickfile_generate` returns `PyResult<String>`. On Ok: Python receives a normal string and writes it to disk. On Err: Python receives an exception and MUST NOT write the error repr to the tickfile CSV file. Do NOT attempt to write exception text to disk. Log the error and skip that minute's tickfile entry (do not corrupt the CSV).

### 6.2 Tickfile Row Format (Rust Implementation)

Full 65-column format matches Python `tickfile.py` `TICKFILE_HEADER`. Rust must output all 65 columns per row (levels 2-10 NA fields included). Reference: `tickfile.py:28` `TICKFILE_HEADER` as authoritative definition.

Python thread does:
1. Call `tickfile_generate()` (**GIL held, ~50ms**)
2. On success: write string to disk (OS write, ~50ms)
3. On Err: raise exception — do NOT write error repr to tickfile file. The `PyResult<String>` means Python gets a normal string on Ok, or an exception on Err. `tickfile_generate` never returns an error string that would be written to disk.

### 6.3 Tickfile Trigger Drain (Python Side)

- Python tickfile thread: same `tickfile_triggers` queue, same drain logic
- `raw_order_buffers` are maintained in **Rust**, not in Python `SharedState.raw_order_buffers`
- Python thread calls `tickfile_get_raw_buffer(minute_key: &str) -> Vec<u8>` to retrieve a minute's buffer from Rust (Rust returns and clears its internal state)
- `latest_snapshot` is maintained in **Rust**; Python thread calls `tickfile_get_latest_snapshot() -> Vec<u8>` to get snapshot data for tickfile generation
- Phase C moves CSV **generation** to Rust; Python remains responsible for disk `file.write()` (~50ms GIL per minute)
- Phase 23 (future): async I/O to overlap CSV generation with disk write

---

## 7. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                    GIL (同一时刻只有 1 个线程持有)              │
└──────────────────────────────────────────────────────────────┘
           ▲                    ▲                    ▲
           │                    │                    │
┌──────────┴──────┐  ┌─────────┴──────┐  ┌────────┴────────┐
│  Order Thread  │  │Snapshot Thread │  │Tickfile Writer │
│                │  │                │  │                │
│ [Rust] parse   │  │[Rust] parse    │  │[Rust] generate │  (GIL held ~50ms)
│ [Rust] date    │  │[Rust] OHLCV agg│  │                │
│ [Rust] group   │  │(GIL released)  │  │                │
│ [Rust] late    │  │                │  │                │
│   detect       │  │                │  │                │
│ (GIL released) │  │                │  │                │
│                │  │                │  │                │
│ Python: dec    │  │Python: flush   │  │Python: disk    │
│ latest_order   │  │OHLCV→CSV file  │  │write (~50ms)   │
│ (atomic swap)  │  │(~10ms GIL)     │  │GIL held        │
│ (~0.2ms)       │  │                │  │                │
└────────────────┘  └────────────────┘  └─────────────────┘

GIL Time per 0900 minute (estimated):
  Current:  Order ~1.2s + Snapshot ~135s + Tickfile ~0.4s = 136.6s total
  Phase 21a (Order only): Order ~0.01s GIL, Snapshot ~135s GIL (unchanged), Tickfile ~0.4s GIL. Wall-clock ~3-5s (order thread no longer blocked by snapshot's GIL).
  Phase 21a+b wall-clock: max(order_Rust_parse+decode+flush, snapshot_Rust_parse+agg+Python_flush) = max(~1.9s, ~35s) ≈ ~35s (sequential within snapshot thread; order and snapshot run concurrently).
Phase 21a+b+c: Order ~0.01s GIL + Snapshot ~0.01s GIL + Python flush ~14s (parallel) + Tickfile ~0.1s = ~0.13s GIL time; wall-clock ~22-35s (parallel overlap). ⚠️ Sequential constraint: snapshot parse+agg (~21s) + Python flush (~14s) are sequential in snapshot thread. CPU contention may push toward 60s SLA boundary.
```

---

## 8. File Changes

### 8.1 Rust (`order_accel/src/lib.rs`)

| Addition | Lines | Description |
|----------|-------|-------------|
| `process_order_batch` | ~350 | Main order batch entry; flat binary output; panic-safe |
| `parse_snapshot_batch` | ~200 | Snapshot CSV parsing; panic-safe |
| `aggregate_snapshot_batch` | ~300 | OHLCV aggregation with HashMap; panic-safe |
| `tickfile_generate` | ~150 | Tickfile CSV generation; panic-safe |
| `tickfile_get_raw_buffer` | ~50 | Retrieve and clear a minute's buffer from Rust state |
| `tickfile_get_latest_snapshot` | ~50 | Retrieve latest snapshot data from Rust state |
| `rust_reset_state` | ~30 | Panic recovery: clear all Rust-internal state (raw_order_buffers, flushed_minutes, etc.); callable from Python after panic |
| Flat binary encoding/decoding helpers | ~150 | Magic/version/schema_hash header; IndexMap for order preservation |
| Panic guards (`catch_unwind`) | ~50 | All `#[pyfunction]` wrapped |
| Unit tests | ~300 | Parity with Python reference; panic isolation tests |

### 8.2 Python (`src/minute_bar/`)

| File | Change |
|------|--------|
| `csv_parser.py` | Add `parse_snapshot_batch`, `aggregate_snapshot_batch` imports from `_order_accel` |
| `engine.py` | Replace `_order_loop` to use `process_order_batch` with panic try/except + fallback; replace `_data_loop` to use `parse_snapshot_batch` + `aggregate_snapshot_batch`; add `tickfile_generate` + `tickfile_get_raw_buffer` calls; expand warmup to all Phase 21 Rust functions (Section 8.2 items 1-6) |

**Warmup procedure** (all at startup before trading begins):
1. `process_order_batch([], 'utf-8', today_str, today_int, 0, [])` — assert returns 5-tuple with valid magic bytes in all 3 buffers
2. `parse_snapshot_batch([], 'utf-8')` — assert returns empty Vec + skipped=0
3. `aggregate_snapshot_batch([], current_minute, [], [], [])` — assert returns valid ohlcv_buf magic
4. `tickfile_generate(b'', b'', b'', minute_key)` — assert returns empty string (no panic)
5. `is_available()` — assert returns True

**Warmup correctness check** (beyond no-panic — must be implemented as automated assertions in `test_phase21_warmup.py`):
6. **Order batch**: Call `process_order_batch` with 3 known order lines (valid, invalid, late). Assert: (a) per_minute_buf decodes to expected records with correct minute_key and seqno; (b) late_order_buf contains only the late record; (c) skipped count = 1.
   **Snapshot parse**: Call `parse_snapshot_batch` with 1 known snapshot line. Assert: all parsed fields match reference values exactly.
   **Snapshot aggregation**: Call `aggregate_snapshot_batch` with 1 known snapshot record. Assert: ohlcv_buf decodes to expected OHLCV values (open/high/low/close within tolerance ≤1e-6).
   **Tickfile generate**: Call `tickfile_generate` with empty buffers. Assert: returns empty string (no panic).
   **Rust state reset**: Call `rust_reset_state()` (panic recovery function). Assert: returns Ok, no panic.

**Warmup pass/fail**: If any warmup call raises exception or returns invalid structure, log FATAL error with the specific function name and exception details, then call `sys.exit(1)` to terminate the process. Do NOT continue running in a degraded state. The process must NOT be allowed to start trading with Rust acceleration unverifiable. Log capability string only on full success: `"Phase21: order_batch={rust_hash} snapshot={rust_hash} tickfile={rust_hash} rust_available=True"`.
| `aggregator.py` | Add batch-mode OHLCV flush method |
| `flusher.py` | Replace `build_tickfile_row` calls with Rust `tickfile_generate`; use `tickfile_get_raw_buffer` |
| `_order_accel.pyi` | Complete type stubs for all new Rust functions (see Appendix A) |

### 8.3 Config / Tests

| File | Change |
|------|--------|
| `tests/test_order_batch_golden.py` | New — golden output test for order batch parity (field-level comparison) |
| `tests/test_snapshot_ohlcv_golden.py` | New — golden output test for snapshot OHLCV parity (float tolerance `≤1e-6`) |
| `tests/test_tickfile_rust_golden.py` | New — golden output test for tickfile generation parity (field-level comparison) |
| `tests/test_order_accel.py` | Extend to cover late order + cross-day + invalid-line seqno continuity |
| `tests/test_phase21_parity_parallel.py` | New — dual-path test: run Rust + Python simultaneously, diff each minute |
| `tests/test_rust_panic_isolation.py` | New — feed malformed input to each `#[pyfunction]`, assert no crash |
| `tests/test_phase21_rss_profile.py` | New — RSS monitoring: assert `peak_rss < 400MB` from baseline; assert `delta_rss < 300MB` per batch |
| `tests/test_phase21_per_component_flags.py` | New — test each `enable_rust_*` flag independently |
| `tests/test_phase21_warmup.py` | New — verify all Phase 21 Rust functions warmup successfully |
| `config/test-e2e-phase21.ini` | New — E2E config with `TEST_PASS_THRESHOLD_SECONDS=60`; **CI gate: red (>60s) = FAIL; green (<6s) and yellow (6-60s) both pass SLA**; three `enable_rust_*` flags |
| `ci/build-rust.yml` | New — CI matrix: `[windows-latest, manylinux-x86_64]`; both platforms run: (1) Rust unit tests AND (2) Python integration tests (`pytest tests/test_order_batch_golden.py tests/test_snapshot_ohlcv_golden.py tests/test_tickfile_rust_golden.py tests/test_order_accel.py`). Both stages must pass for the build to be green. ⚠️ **Must be created and verified green on both platforms before Phase 21a is marked shippable**.

---

## 9. Implementation Sequence

### P0 Prerequisites (must complete before Phase 21a is marked shippable)

**这两项未完成，Phase 21a 绝对不能 ship：**

**1. `ci/build-rust.yml` 必须创建并验证 green**

CI file 必须包含：
- `windows-latest` 和 `manylinux-x86_64` 两个平台
- Stage 1: Rust unit tests (`cargo test --all`)
- Stage 2: Python integration tests (`pytest tests/test_order_batch_golden.py tests/test_snapshot_ohlcv_golden.py tests/test_tickfile_rust_golden.py tests/test_order_accel.py`)
- 两个 stage 都 green 才能 merge 或 release

**2. schema_hash CRC32 必须计算真实值并写入 CI**

在 implementation 开始时（不是结束时），必须：
- 用 Python `zlib.crc32()` 计算每个 buffer 类型的 field-layout string 的 CRC32
- 用 Rust `crc32fast` 验证产生相同结果（Windows 和 Linux 都验证）
- 将真实值替换掉 Section 4.2 中的占位符
- 将值写入 `tests/test_schema_hash_parity.py` 作为 golden test vectors
- 任何 schema 变更必须同步更新 CRC32 值，否则 CI fail

**⚠️ 如果这两项未完成，Phase 21a deploy 后所有 batch 会因 schema_hash mismatch 而 fallback 到 Python path，性能倒退到 Phase 20，但无人知晓原因。**

**Config flags** (three independent flags):
- `enable_rust_order_full_batch` — Part A (order full batch processing)
- `enable_rust_snapshot_batch` — Part B (snapshot parse + OHLCV)
- `enable_rust_tickfile` — Part C (tickfile generation)
Each can be enabled/disabled independently. When disabled, the corresponding Python path runs.

**Rollback procedure**:
1. Disable the relevant `enable_rust_*` flag(s)
2. **File format invariant**: Part A Rust `process_order_batch` writes order minute files via Python `_flush_order_minute` (same Python CSV/text format as before). The flat binary buffers are internal Rust state only — they are never written to disk. Therefore rollback does NOT require format conversion or file deletion.
3. **Call `rust_reset_state()`** — clears all Rust-internal state (raw_order_buffers, flushed_minutes HashSet, etc.) before any future Rust calls. This is required after any panic or rollback to prevent stale state from corrupting subsequent batches.
4. If Rust panic occurred mid-batch: additionally restore Python-side rollback copy of base_vol/amt_by_symbol; do NOT store partially-constructed return values from the panicked call.
5. Part C depends on Part A and Part B: `enable_rust_tickfile` requires both `enable_rust_order_full_batch` AND `enable_rust_snapshot_batch`. If either is false, log fatal and exit. engine.py adds runtime assertions:
   - `if enable_rust_tickfile && !enable_rust_order_full_batch`: log fatal and exit
   - `if enable_rust_tickfile && !enable_rust_snapshot_batch`: log fatal and exit


### Step 0: P0 Prerequisites (must complete BEFORE writing any Rust code)

**These two items are P0 prerequisites — they must be done before any Rust code is written, not after.**

**0a. Create `ci/build-rust.yml`**
Create the CI file with:
- `windows-latest` and `manylinux-x86_64` platforms
- Stage 1: `cargo test --all` (Rust unit tests)
- Stage 2: `pytest tests/test_order_batch_golden.py tests/test_snapshot_ohlcv_golden.py tests/test_tickfile_rust_golden.py tests/test_order_accel.py` (Python integration tests)
Both stages must be green before any Rust code is committed.

**0b. Compute actual schema_hash CRC32 values**
- Write a temporary Python script: `python -c "import zlib; print(hex(zlib.crc32(b'symbol:str,time:i64,bidprice:i64,bidsize:i64,askprice:i64,asksize:i64,decimal:i64,rcvtime:i64,')))`
- Write the equivalent Rust snippet: `crc32fast::hash(b"symbol:str,time:i64,bidprice:i64,bidsize:i64,askprice:i64,asksize:i64,decimal:i64,rcvtime:i64,")`
- Verify both produce identical output on both Windows and Linux
- Repeat for all 4 buffer schemas (order, snapshot, OHLCV, latest_snapshot)
- Replace placeholder CRC32 values in Section 4.2 with real values
- Add `tests/test_schema_hash_parity.py` as a CI gate

**If you write Rust code before doing these two steps, Phase 21a will silently fall back to Python on deploy with no visible error.**

---

### Step 1: Rust Order Batch (process_order_batch)
   > **Phase 21a E2E gate**: After Phase 21a ship, run E2E benchmark. Phase 4 analysis shows Phase 21a alone (order GIL reduced) should achieve 3-5s wall-clock, passing 60s SLA easily, because the bottleneck is order thread waiting for GIL held by snapshot — not snapshot processing itself.
   > **Gate logic**: If 0900 wall-clock **> 60s** → **ROLLBACK** Phase 21a (something is broken: Rust path panic, fallback not working, or benchmark misconfigured). If 0900 wall-clock **6-60s** → investigate (Phase 21a alone should achieve < 5s; 6-60s suggests GIL contention still exists). If **< 6s** → Phase 21a target achieved, proceed to Phase 21b for further optimization.
   > ⚠️ **Note**: Phase 21a does NOT reduce snapshot GIL (~135s全天), but it DOES reduce order thread GIL from ~1.2s to ~0.01s. Since order thread's bottleneck is waiting for snapshot's GIL, reducing order's GIL need to near-zero should eliminate the wait, regardless of snapshot's own GIL usage.
0. **Prototype first**: Before full implementation, create `process_order_batch` prototype in Rust (seqno + date check + minute_key group-by only). Benchmark against Python reference. If throughput < 200K rec/s or correctness < 100%, re-prototype.
1. Implement `process_order_batch` in Rust (with panic guards)
2. Add flat binary encoding: magic header + per-minute + late + latest_order (full OrderRecord fields)
3. Implement `time_to_minute_key` using `str(time)[:12]` (string slice, NOT integer division)
4. Add Rust unit tests: valid lines, invalid lines, CRLF, BOM, seqno continuity
5. Add `test_order_batch_golden.py` with field-level comparison
6. Wire into `engine.py` `_order_loop` with panic try/except → fallback to Python
7. **Verify**: existing 389 tests pass + order batch golden test passes
8. **Benchmark**: measure `test_order_batch_benchmark` — target < 5s for 747K records

### Step 2: Rust Snapshot Parse (parse_snapshot_batch)
1. Implement `parse_snapshot_batch` in Rust (with panic guards)
2. Wire into `engine.py` `_data_loop` (parsing phase only)
3. Verify: snapshot parse parity with Python reference
4. **Benchmark**: snapshot parse throughput target > 500K records/s (matching order parse)

### Step 3: Rust Snapshot Aggregation (aggregate_snapshot_batch)
1. Implement `aggregate_snapshot_batch` in Rust (with panic guards)
2. Add batch state propagation (base_vol/amt pass-through, complete accumulated set semantics)
3. Add OHLCV flat binary encoding (magic header)
4. Wire into `engine.py` `_data_loop`
5. Add `test_snapshot_ohlcv_golden.py` with float tolerance `≤1e-6`
6. **Verify**: OHLCV golden test passes + existing snapshot tests pass
7. **Benchmark**: measure snapshot aggregation throughput

### Step 4: Rust Tickfile Generation (tickfile_generate)
1. Implement `tickfile_generate` in Rust (with panic guards)
2. Add `tickfile_get_raw_buffer` and `tickfile_get_latest_snapshot` helper functions
3. Wire into `flusher.py` tickfile thread; Python remains responsible for disk write
4. Add `test_tickfile_rust_golden.py` with field-level comparison
5. **Verify**: tickfile golden test passes + existing tickfile tests pass
6. **Benchmark**: tickfile generation for 4505 symbols target < 100ms

### Step 5: Integration + E2E
1. Full E2E test: 0900 minute wall-clock < 60s (target 3–5s)
   - `TEST_PASS_THRESHOLD_SECONDS=60` gate: **green < 6s = target achieved; yellow 6-60s = SLA met (pass); red > 60s = FAIL. ROLLBACK Phase 21a alone cannot achieve 60s SLA (snapshot GIL unchanged ~135s全天). Do NOT proceed to 21b. Ship Phase 21a+b instead. If yellow (6-60s), proceed to Phase 21b for further optimization.**
2. All 389 regression tests pass
3. 3 new golden tests pass
4. `test_phase21_parity_parallel.py`: dual-path Rust + Python simultaneous run, diff each minute
5. `test_phase21_rss_profile.py`: RSS delta < 200MB per batch
6. Warmup all Phase 21 Rust functions at startup; log version hashes of loaded `.pyd`/`.so`
7. Startup capability log: `"Phase 21: order_batch=RUST-hash, snapshot=RUST-hash, tickfile=RUST-hash"`

---

## 10. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Rust late order detection diverges from Python `_flushed_order_minutes` | Low | High | Python passes complete accumulated set each call; golden test covers |
| OHLCV float precision mismatch (Python float vs Rust f64) | Low | Medium | Both use IEEE 754 double; golden test uses field-level tolerance `≤1e-6` |
| Tickfile CSV format mismatch with Python reference | Medium | Medium | Golden test compares field-by-field; float fields with tolerance |
| Batch boundary base_vol/amt drift over time | Low | High | Every `aggregate_snapshot_batch` returns updated base values; Python passes back; golden test with multi-batch |
| **Memory regression: concurrent flat binary buffers** | **Medium** | **Medium** | **Rust internal `raw_order_buffers` HashMap is bounded to 4 concurrent minute keys. Eviction policy: FIFO (oldest minute_key by insertion order). On eviction: buffer bytes are dropped (deallocated), not pooled. When the 5th minute_key is added, the oldest is evicted and its memory freed (logged as warning). This keeps maximum RSS from order buffers to 4×67MB = 268MB. Additionally: measure `peak_rss < 400MB` from baseline and `delta_rss < 300MB` per batch; if 4-buffer bound is not enforceable, raise gate to `delta_rss < 400MB`** |
| Performance regression in non-peak minutes | Low | Medium | Benchmark before/after for 0800-0859 period |
| **Rust panic crashes Python process** | **Low** | **High** | **All `#[pyfunction]` wrapped in `catch_unwind`; Python catches exception and falls back to Python path; panic fallback state machine: (1) startup: if `is_available()==False`, set all `enable_rust_*=False` and run pure Python; (2) mid-batch panic: complete current batch in Python (Rust buffers discarded), Python clears `flushed_minutes` before resuming Python path; (3) subsequent batches use Python path** |
| **Binary format upgrade silently breaks compatibility** | **Low** | **High** | **Magic bytes + version + schema_hash in every buffer; Python raises `ValueError` on mismatch** |
| **Per-component rollback requires code change** | **Medium** | **Medium** | **Three independent `enable_rust_*` flags; each component can be disabled independently** |
| **Fallback Rust path is SLA-passing but data-quality-failing** | **Low** | **High** | **`test_phase21_parity_parallel.py` runs both paths simultaneously and diffs output** |
| **Tickfile disk I/O still in Python (~50ms/minute)** | **High** | **Low** | **Known limitation of Phase 21; Phase 23 addresses with async I/O** |
| **Cross-day reset mishandled in Rust** | **Low** | **High** | **`prev_date` parameter added; Rust clears flushed_minutes on date change** |

---

## 11. Success Criteria

**Wall-clock Performance Gate** (`config/test-e2e-phase21.ini`):
- `TEST_PASS_THRESHOLD_SECONDS = 60`
- green: < 6s
- yellow: 6–60s (SLA met — Phase 22 snapshot optimization proceeds regardless; goal is to reach < 6s)
- red: > 60s (FAIL — do not ship; rollback or fix before shipping)

| Metric | Before | After | Verification Method |
|--------|--------|-------|---------------------|
| 0900 minute wall-clock | 180s | **< 60s (target 3–5s)** | `pytest tests/test_e2e_phase21.py::test_0900_wallclock_sla` |
| Order thread GIL share | ~11% | ~99% (order runs GIL-free) | GIL contention profiler post-implementation |
| Snapshot thread GIL share | ~89% | ~0% (Rust) | GIL contention profiler post-implementation |
| Tickfile thread GIL share | ~5% | ~0% (Rust CSV gen) | GIL contention profiler post-implementation |
| Regression tests | 389 passed | 389 + 5 new passed | `pytest tests/` |
| Late order correctness | baseline | no regression | `tests/test_order_batch_golden.py` |
| OHLCV numerical accuracy | baseline | no regression (tolerance ≤1e-6) | `tests/test_snapshot_ohlcv_golden.py` |
| Tickfile CSV correctness | baseline | no regression | `tests/test_tickfile_rust_golden.py` |
| RSS delta per batch | N/A | **< 200MB** | `tests/test_phase21_rss_profile.py` |
| Panic isolation | N/A | Python process survives malformed input | `tests/test_rust_panic_isolation.py` |
| Parity dual-path | N/A | Rust output == Python output | `tests/test_phase21_parity_parallel.py` |

---

## 12. Open Questions / Future Work

### Phase 22 (Snapshot OHLCV Rust — not yet started)
If 0900 minute does not reach 3–5s target after Phase 21a, further optimize snapshot processing.

### Phase 23 (Tickfile async I/O — future work)
Phase 21 Part C moves CSV **generation** to Rust but Python still does disk `file.write()` (~50ms GIL per minute). Phase 23 would add async I/O to overlap CSV generation with disk write, eliminating the remaining Python tickfile GIL cost. This is **not** in Phase 21 scope.

### Scope clarification
- **Phase 21 scope**: Order (A) + Snapshot parse+OHLCV (B) + Tickfile CSV generation (C), all with independent rollback flags
- **Phase 21 NOT in scope**: async disk I/O for tickfile (Phase 23), further snapshot optimization (Phase 22)

### Implementation phasing recommendation
Implement and ship in three sub-phases:
1. Phase 21a: Part A (Order full batch) only → E2E benchmark → ship
2. Phase 21b: Part B (Snapshot) → E2E benchmark → ship
3. Phase 21c: Part C (Tickfile) → E2E benchmark → ship

This limits blast radius of each component and makes rollbacks surgical.

---

## Appendix A: `_order_accel.pyi` Type Stubs

```python
from typing import Tuple, List, Dict

# ── Part A: Order Full Batch ──────────────────────────────────

def process_order_batch(
    lines: List[bytes],
    encoding: str,
    today_str: str,
    today_int: int,
    prev_date: int,
    flushed_minutes: List[str],
) -> Tuple[bytes, bytes, bytes, List[str], int]:
    """Process a batch of order CSV lines entirely in Rust.

    Returns:
        per_minute_buf: flat binary (magic 0xCC01)
        late_order_buf: flat binary (magic 0xCC02)
        latest_order_buf: flat binary (magic 0xCC03) — includes bid/ask/decimal fields
        late_minute_keys: List[str]
        skipped: int
    """
    ...

def decode_order_per_minute_buf(buf: bytes) -> Dict[str, List["OrderRecord"]]:
    """Decode per_minute_buf or late_order_buf: minute_key → List[OrderRecord]."""
    ...

def decode_latest_order_buf(buf: bytes) -> Dict[str, "OrderRecord"]:
    """Decode latest_order_buf: symbol → OrderRecord (full fields)."""
    ...

# ── Part B: Snapshot Batch ────────────────────────────────────

def parse_snapshot_batch(
    lines: List[bytes],
    encoding: str,
) -> Tuple[List["SnapshotParsed"], int]: ...

def aggregate_snapshot_batch(
    records: List["SnapshotParsed"],
    current_minute: str,
    base_vol_by_symbol: List[Tuple[str, int]],
    base_amt_by_symbol: List[Tuple[str, float]],
    flushed_minutes: List[str],
) -> Tuple[bytes, str, List[Tuple[str, int]], List[Tuple[str, float]], List[str]]:
    """Returns: (ohlcv_buf, new_current_minute, updated_base_vol, updated_base_amt, late_minute_keys)"""
    ...

def decode_ohlcv_buf(buf: bytes) -> Dict[str, List["OHLCVEntry"]]:
    """Decode ohlcv_buf: minute_key → List[OHLCVEntry]."""
    ...

def decode_snapshot_buf(buf: bytes) -> Dict[str, "SnapshotData"]:
    """Decode latest_snapshot_buf: symbol → SnapshotData. Validates magic/version/schema_hash before decoding. Raises ValueError on mismatch."""
    ...

# ── Part C: Tickfile Generation ────────────────────────────────

def tickfile_generate(
    raw_order_buf: bytes,
    latest_order_buf: bytes,
    latest_snapshot_buf: bytes,
    minute_key: str,
) -> str:
    """Generate tickfile CSV string in Rust. GIL held for ~50ms."""
    ...

def tickfile_get_raw_buffer(minute_key: str) -> bytes:
    """Retrieve and clear a minute's raw order buffer from Rust state."""
    ...

def tickfile_get_latest_snapshot() -> bytes:
    """Retrieve latest snapshot data from Rust state for tickfile generation."""
    ...

def rust_reset_state() -> None:
    """Panic recovery: clear all Rust-internal state (raw_order_buffers, flushed_minutes, etc.). Call this after any Rust panic before resuming."""
    ...

# ── Utility ───────────────────────────────────────────────────

def is_available() -> bool: ...

class OrderRecord:
    symbol: str; seqno: int; time: int
    bidprice: int; bidsize: int; askprice: int; asksize: int
    decimal: int; rcvtime: int

class SnapshotParsed:
    # Fields from parsed snapshot CSV line (NOT OHLCV-aggregated fields)
    symbol: str; time: int; seqno: int; preclose: int; lastprice: int
    lasttradeqty: int; totalvol: int; totalamount: int; sessionid: int
    tradetype: str; status: str; direction: int; pflag: str; decimal: int; vwap: int; shortsellflag: int; rcvtime: int
    # Note: open/high/low/close are computed by aggregate_snapshot_batch (OHLCVAggregate), not parsed

class SnapshotData:
    symbol: str; time: int; preclose: float; lastprice: float; open: float; high: float; low: float; close: float
    totalvol: int; totalamount: float; decimal: int
    # Note: decimal stored as u32 in binary format (Section 5.2b), widened to int in Python class.
    # If decimal values ever exceed u32 range, encoder must check for truncation.

class OHLCVEntry:
    symbol: str  # minute_key is the dict key in Dict[minute_key, List[OHLCVEntry]]
    open: float; high: float; low: float; close: float
    volume: int; count: int; decimal: int
```
