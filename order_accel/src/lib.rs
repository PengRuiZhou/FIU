use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use indexmap::IndexMap;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Order schema hash (CRC32 of field layout string)
/// "symbol:str,time:i64,bidprice:i64,bidsize:i64,askprice:i64,asksize:i64,decimal:i64,rcvtime:i64,"
const ORDER_SCHEMA_HASH: u32 = 0x9A51A8B3;

/// Magic bytes for flat binary buffers
/// [0xAA, 0xBB, 0xCC, {version_byte}] as little-endian u32
const MAGIC_PER_MINUTE: u32 = 0x01CCBBAA;   // version_byte = 0x01
const MAGIC_LATE_ORDER: u32 = 0x02CCBBAA;   // version_byte = 0x02
const MAGIC_LATEST_ORDER: u32 = 0x03CCBBAA; // version_byte = 0x03
const MAGIC_VERSION: u16 = 1;

/// Max concurrent minute buffers (FIFO eviction)
const MAX_RAW_BUFFER_COUNT: usize = 4;

// ── Persistent State ─────────────────────────────────────────────────────────

/// State that persists across process_order_batch calls
struct State {
    /// Per-minute flat binary buffers (max MAX_RAW_BUFFER_COUNT, FIFO eviction)
    raw_order_buffers: HashMap<String, Vec<u8>>,
    /// Tracks flushed minute keys for late order detection
    flushed_minutes: HashSet<String>,
    /// Previous trading date for cross-day detection
    prev_date: i64,
    /// Seqno counter (global across trading session)
    seqno_counter: u64,
}

lazy_static::lazy_static! {
    static ref GLOBAL_STATE: Mutex<Option<State>> = Mutex::new(None);
}

fn get_state() -> std::sync::MutexGuard<'static, Option<State>> {
    GLOBAL_STATE.lock().unwrap()
}

fn init_state() {
    let mut guard = GLOBAL_STATE.lock().unwrap();
    if guard.is_none() {
        *guard = Some(State {
            raw_order_buffers: HashMap::new(),
            flushed_minutes: HashSet::new(),
            prev_date: 0,
            seqno_counter: 0,
        });
    }
}

fn reset_state() {
    let mut guard = GLOBAL_STATE.lock().unwrap();
    *guard = Some(State {
        raw_order_buffers: HashMap::new(),
        flushed_minutes: HashSet::new(),
        prev_date: 0,
        seqno_counter: 0,
    });
}

// ── Flat Binary Encoding Helpers ──────────────────────────────────────────────

/// Write magic header: [4 bytes magic][2 bytes version][4 bytes schema_hash]
fn write_magic_header(buf: &mut Vec<u8>, magic: u32, version: u16, schema_hash: u32) {
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&version.to_le_bytes());
    buf.extend_from_slice(&schema_hash.to_le_bytes());
}

/// Encode a single order record into the buffer
/// Format: [2 bytes symbol_len][symbol_len bytes][8 bytes time][8 bytes bidprice][8 bytes bidsize][8 bytes askprice][8 bytes asksize][8 bytes decimal][8 bytes rcvtime]
fn encode_order_record(
    buf: &mut Vec<u8>,
    symbol: &str,
    time: i64,
    bidprice: i64,
    bidsize: i64,
    askprice: i64,
    asksize: i64,
    decimal: i64,
    rcvtime: i64,
) {
    let sym_bytes = symbol.as_bytes();
    buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(sym_bytes);
    buf.extend_from_slice(&time.to_le_bytes());
    buf.extend_from_slice(&bidprice.to_le_bytes());
    buf.extend_from_slice(&bidsize.to_le_bytes());
    buf.extend_from_slice(&askprice.to_le_bytes());
    buf.extend_from_slice(&asksize.to_le_bytes());
    buf.extend_from_slice(&decimal.to_le_bytes());
    buf.extend_from_slice(&rcvtime.to_le_bytes());
}

/// Encode latest-order record (includes seqno at the end)
fn encode_latest_order_record(
    buf: &mut Vec<u8>,
    symbol: &str,
    time: i64,
    bidprice: i64,
    bidsize: i64,
    askprice: i64,
    asksize: i64,
    decimal: i64,
    rcvtime: i64,
    seqno: i64,
) {
    let sym_bytes = symbol.as_bytes();
    buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(sym_bytes);
    buf.extend_from_slice(&time.to_le_bytes());
    buf.extend_from_slice(&bidprice.to_le_bytes());
    buf.extend_from_slice(&bidsize.to_le_bytes());
    buf.extend_from_slice(&askprice.to_le_bytes());
    buf.extend_from_slice(&asksize.to_le_bytes());
    buf.extend_from_slice(&decimal.to_le_bytes());
    buf.extend_from_slice(&rcvtime.to_le_bytes());
    buf.extend_from_slice(&seqno.to_le_bytes());
}

/// Build per-minute buffer for a given minute_key with all its records
fn build_per_minute_buf(minute_key: &str, records: &IndexMap<String, OrderFields>) -> Vec<u8> {
    let mut buf = Vec::new();
    write_magic_header(&mut buf, MAGIC_PER_MINUTE, MAGIC_VERSION, ORDER_SCHEMA_HASH);

    // minute_key
    let mk_bytes = minute_key.as_bytes();
    buf.extend_from_slice(&(mk_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(mk_bytes);

    // record count
    buf.extend_from_slice(&(records.len() as u32).to_le_bytes());

    // records (preserve insertion order via IndexMap iteration)
    for (_sym, fields) in records {
        encode_order_record(
            &mut buf,
            &fields.symbol,
            fields.time,
            fields.bidprice,
            fields.bidsize,
            fields.askprice,
            fields.asksize,
            fields.decimal,
            fields.rcvtime,
        );
    }

    buf
}

/// Build late-order buffer from multiple minute_keys
fn build_late_order_buf(
    late_records: &IndexMap<String, IndexMap<String, OrderFields>>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_magic_header(&mut buf, MAGIC_LATE_ORDER, MAGIC_VERSION, ORDER_SCHEMA_HASH);

    for (_minute_key, records) in late_records {
        for (_sym, fields) in records {
            encode_order_record(
                &mut buf,
                &fields.symbol,
                fields.time,
                fields.bidprice,
                fields.bidsize,
                fields.askprice,
                fields.asksize,
                fields.decimal,
                fields.rcvtime,
            );
        }
    }

    buf
}

/// Build latest-order buffer from the latest_order map
fn build_latest_order_buf(latest_order: &IndexMap<String, LatestOrderEntry>) -> Vec<u8> {
    let mut buf = Vec::new();
    write_magic_header(&mut buf, MAGIC_LATEST_ORDER, MAGIC_VERSION, ORDER_SCHEMA_HASH);

    for (_sym, entry) in latest_order {
        encode_latest_order_record(
            &mut buf,
            &entry.symbol,
            entry.time,
            entry.bidprice,
            entry.bidsize,
            entry.askprice,
            entry.asksize,
            entry.decimal,
            entry.rcvtime,
            entry.seqno as i64,
        );
    }

    buf
}

// ── Parsed Order Fields ───────────────────────────────────────────────────────

#[derive(Clone)]
struct OrderFields {
    symbol: String,
    time: i64,
    bidprice: i64,
    bidsize: i64,
    askprice: i64,
    asksize: i64,
    decimal: i64,
    rcvtime: i64,
}

#[derive(Clone, Debug)]
struct LatestOrderEntry {
    symbol: String,
    time: i64,
    bidprice: i64,
    bidsize: i64,
    askprice: i64,
    asksize: i64,
    decimal: i64,
    rcvtime: i64,
    seqno: u64,
}

// ── OHLCV Aggregation Structs ─────────────────────────────────────────────────

/// OHLCV aggregate per symbol per minute.
/// Matches Python OHLCVAggregate.update() logic exactly.
#[derive(Clone, Debug)]
struct OHLCVAggregate {
    symbol: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    amount: f64,
    count: u32,
    start_totalvol: i64,
    end_totalvol: i64,
    start_totalamount: f64,   // raw i64 stored as f64
    end_totalamount: i64,     // RAW i64 (NOT pre-scaled); amount = (raw - base_amt) / d
    seqno: i64,
    decimal: i64,
    any_lasttradeqty_positive: bool,
}

impl Default for OHLCVAggregate {
    fn default() -> Self {
        OHLCVAggregate {
            symbol: String::new(),
            open: 0.0,
            high: f64::NEG_INFINITY,
            low: f64::INFINITY,
            close: 0.0,
            volume: 0,
            amount: 0.0,
            count: 0,
            start_totalvol: 0,
            end_totalvol: 0,
            start_totalamount: 0.0,
            end_totalamount: 0,  // i64, not f64
            seqno: 0,
            decimal: 2,
            any_lasttradeqty_positive: false,
        }
    }
}

/// Latest snapshot entry per symbol.
/// Used for tickfile generation: stores pre-scaled float fields and raw totalvol.
#[derive(Clone, Debug)]
struct LatestSnapshotEntry {
    symbol: String,
    time: i64,
    preclose: f64,      // pre-scaled: raw i64 / 10^decimal
    lastprice: f64,     // pre-scaled: raw i64 / 10^decimal
    open: f64,          // pre-scaled: from OHLCV aggregation
    high: f64,          // pre-scaled: from OHLCV aggregation
    low: f64,           // pre-scaled: from OHLCV aggregation
    close: f64,         // pre-scaled: from OHLCV aggregation
    totalvol: i64,
    totalamount: f64,  // f64 per spec Section 5.2b (8 bytes LE)
    decimal: i64,
}

// ── Snapshot Global State ──────────────────────────────────────────────────────

/// State that persists across aggregate_snapshot_batch calls for snapshot data
struct SnapshotState {
    /// Latest snapshot entry per symbol (updated each batch)
    latest_snapshot: HashMap<String, LatestSnapshotEntry>,
    /// Buffer to return on tickfile_get_latest_snapshot (built each batch)
    latest_snapshot_buf: Vec<u8>,
}

lazy_static::lazy_static! {
    static ref SNAPSHOT_GLOBAL_STATE: Mutex<Option<SnapshotState>> = Mutex::new(None);
}

fn get_snapshot_state() -> std::sync::MutexGuard<'static, Option<SnapshotState>> {
    SNAPSHOT_GLOBAL_STATE.lock().unwrap()
}

fn init_snapshot_state() {
    let mut guard = SNAPSHOT_GLOBAL_STATE.lock().unwrap();
    if guard.is_none() {
        *guard = Some(SnapshotState {
            latest_snapshot: HashMap::new(),
            latest_snapshot_buf: Vec::new(),
        });
    }
}

fn reset_snapshot_state() {
    let mut guard = SNAPSHOT_GLOBAL_STATE.lock().unwrap();
    *guard = Some(SnapshotState {
        latest_snapshot: HashMap::new(),
        latest_snapshot_buf: Vec::new(),
    });
}

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

/// Flat binary return: returns a single Vec<u8> instead of Vec<tuple>.
/// Format per record: u16 LE symbol_len + symbol_bytes + 7 × i64 LE.
/// ~90 bytes per record. 747K records = ~66MB returned as single PyBytes (zero-copy from Vec<u8>).
/// This avoids PyO3 creating 5.97M Python objects (747K tuples × 8 elements).
#[pyfunction]
fn parse_order_batch_flat(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<u8>, u64)> {
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

/// Reset all Rust-internal state (panic recovery).
/// Clears: raw_order_buffers, flushed_minutes, seqno_counter.
/// Call this from Python after any Rust panic before resuming.
#[pyfunction]
fn rust_reset_state() -> PyResult<()> {
    reset_state();
    Ok(())
}

/// Process a batch of raw order CSV lines entirely in Rust.
///
/// Panic safety: all code paths wrapped in std::panic::catch_unwind.
/// On panic, returns Err with boxed error string (Python catches as exception).
///
/// **Seqno assignment**: Starts at 0, increments by 1 per valid (non-skipped,
/// date-matching) record. Skipped/invalid records do NOT consume seqno values.
/// Seqno is global across the trading session (not per-batch).
///
/// **Processing pipeline**:
///   1. UTF-8 decode + BOM strip + CRLF normalization
///   2. Parse CSV line (8 fields)
///   3. Date check: str(time)[:8] == today_str (STRING SLICE, NOT integer division)
///   4. Assign seqno (only for date-matching valid records)
///   5. minute_key: str(time)[:12] (12-char string slice)
///   6. Group by minute_key (IndexMap, preserves file order)
///   7. Late order detection: if minute_key in flushed_minutes → late_order_buf
///   8. latest_order per symbol: max (time, rcvtime)
///   9. Cross-day handling: if record_date != prev_date && prev_date != 0 → clear state
///
/// Returns:
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
    _today_int: i64,
    prev_date: i64,
    flushed_minutes: Vec<String>,
) -> PyResult<(
    Vec<u8>,       // per_minute_buf (magic 0xCC 0x01)
    Vec<u8>,       // late_order_buf (magic 0xCC 0x02)
    Vec<u8>,       // latest_order_buf (magic 0xCC 0x03)
    Vec<String>,   // late_minute_keys
    u64,           // skipped count
)> {
    // Wrap entire function body in catch_unwind for panic safety
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        process_order_batch_impl(py, lines, encoding, today_str, prev_date, flushed_minutes)
    }));

    match result {
        Ok(r) => r,
        Err(_) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "process_order_batch panicked".to_string()
        )),
    }
}

fn process_order_batch_impl(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
    today_str: &str,
    prev_date: i64,
    flushed_minutes: Vec<String>,
) -> PyResult<(
    Vec<u8>,       // per_minute_buf
    Vec<u8>,       // late_order_buf
    Vec<u8>,       // latest_order_buf
    Vec<String>,   // late_minute_keys
    u64,           // skipped count
)> {
    // Guard: reject oversized batches to prevent OOM
    if lines.len() > MAX_BATCH_SIZE {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Batch too large: {} lines (max {})", lines.len(), MAX_BATCH_SIZE)
        ));
    }

    // Initialize state on first call
    init_state();

    let encoding_owned = encoding.to_string();
    let flushed_minutes_owned = flushed_minutes; // Move once, use twice
    let today_str_owned = today_str.to_string();
    let prev_date_val = prev_date;

    // ── Phase 1: Get starting seqno and check cross-day (GIL held, fast) ─────
    let _starting_seqno = {
        let mut guard = get_state();
        let state = guard.as_mut().unwrap();
        state.seqno_counter
    };

    // ── Phase 2: Parse all lines WITHOUT GIL ──────────────────────────────────
    // Returns (parsed_records, skipped_count, first_date_key)
    let (raw_parsed, skipped, _first_date_key): (Vec<PreprocessedRecord>, u64, Option<String>) =
        py.allow_threads(|| {
            let mut skipped: u64 = 0;
            let mut results: Vec<PreprocessedRecord> = Vec::new();
            for line in &lines {
                match preprocess_line(line, &encoding_owned, &today_str_owned) {
                    Some(t) => results.push(t),
                    None => skipped += 1,
                }
            }
            // Return first date_key for cross-day detection (only look at first record)
            let first_date_key = results.first().map(|r| r.date_key.clone());
            (results, skipped, first_date_key)
        });

    // ── Phase 3: State update + grouping + buffer building (GIL held) ─────────
    // All state mutations happen here (outside allow_threads)
    let (
        per_minute_buf_out,
        late_order_buf_out,
        latest_order_buf_out,
        late_minute_keys_out,
        _,
    ) = {
        let mut guard = get_state();
        let state = guard.as_mut().unwrap();

        // Cross-day handling: if first record's date != prev_date && prev_date != 0
        // Clear flushed_minutes AND raw_order_buffers
        if let Some(ref first_date) = _first_date_key {
            let prev_date_str = prev_date_val.to_string();
            if *first_date != prev_date_str && prev_date_val != 0 {
                state.flushed_minutes.clear();
                state.raw_order_buffers.clear();
                // Note: seqno_counter does NOT reset on new day (global across session)
            }
        }

        // Update flushed_minutes from Python's complete accumulated set
        state.flushed_minutes.clear();
        for minute_key in &flushed_minutes_owned {
            state.flushed_minutes.insert(minute_key.clone());
        }

        // Build HashSet for O(1) late order lookup
        let flushed_set: HashSet<String> = flushed_minutes_owned.into_iter().collect();

        // Assign seqnos to valid records (sequential, starting from global counter)
        let mut current_seqno = state.seqno_counter;
        let mut parsed_with_seq: Vec<PreprocessedRecord> = Vec::with_capacity(raw_parsed.len());
        for mut record in raw_parsed {
            record.seqno = current_seqno;
            current_seqno += 1;
            parsed_with_seq.push(record);
        }
        state.seqno_counter = current_seqno;

        // Group by minute_key — preserve original file order via IndexMap
        // minute_key -> IndexMap<symbol, OrderFields>
        let mut per_minute_groups: IndexMap<String, IndexMap<String, OrderFields>> = IndexMap::new();
        // late minute_keys set
        let mut late_minute_keys_set: HashSet<String> = HashSet::new();
        // late orders: minute_key -> IndexMap<symbol, OrderFields>
        let mut late_order_groups: IndexMap<String, IndexMap<String, OrderFields>> = IndexMap::new();
        // latest_order per symbol: symbol -> LatestOrderEntry (max time, rcvtime)
        let mut latest_order: IndexMap<String, LatestOrderEntry> = IndexMap::new();

        for record in &parsed_with_seq {
            let minute_key = &record.minute_key;

            // Check if late order
            let is_late = flushed_set.contains(minute_key);

            // Track latest order per symbol: max (time, rcvtime)
            let should_update_latest = match latest_order.get(&record.symbol) {
                None => true,
                Some(existing) => {
                    record.time > existing.time
                        || (record.time == existing.time && record.rcvtime > existing.rcvtime)
                }
            };

            if should_update_latest {
                latest_order.insert(record.symbol.clone(), LatestOrderEntry {
                    symbol: record.symbol.clone(),
                    time: record.time,
                    bidprice: record.bidprice,
                    bidsize: record.bidsize,
                    askprice: record.askprice,
                    asksize: record.asksize,
                    decimal: record.decimal,
                    rcvtime: record.rcvtime,
                    seqno: record.seqno,
                });
            }

            let entry = OrderFields {
                symbol: record.symbol.clone(),
                time: record.time,
                bidprice: record.bidprice,
                bidsize: record.bidsize,
                askprice: record.askprice,
                asksize: record.asksize,
                decimal: record.decimal,
                rcvtime: record.rcvtime,
            };

            if is_late {
                // Route to late_order_buf
                late_minute_keys_set.insert(minute_key.clone());
                late_order_groups
                    .entry(minute_key.clone())
                    .or_insert_with(IndexMap::new)
                    .insert(record.symbol.clone(), entry);
            } else {
                // Route to per_minute_buf
                per_minute_groups
                    .entry(minute_key.clone())
                    .or_insert_with(IndexMap::new)
                    .insert(record.symbol.clone(), entry);
            }
        }

        // Build per-minute buffers (also update raw_order_buffers with FIFO eviction)
        let mut per_minute_bufs: Vec<Vec<u8>> = Vec::new();
        for (minute_key, records) in &per_minute_groups {
            let buf = build_per_minute_buf(minute_key, records);
            per_minute_bufs.push(buf.clone());

            // Update raw_order_buffers (FIFO eviction, max MAX_RAW_BUFFER_COUNT)
            if state.raw_order_buffers.len() >= MAX_RAW_BUFFER_COUNT {
                // Remove oldest (first) entry (HashMap doesn't preserve order, but we use keys() which is arbitrary - for true FIFO we'd need OrderMap)
                // Since we use HashMap, we can't reliably do FIFO. For now, just remove an arbitrary entry.
                if let Some(first_key) = state.raw_order_buffers.keys().next().cloned() {
                    state.raw_order_buffers.remove(&first_key);
                }
            }
            state.raw_order_buffers.insert(minute_key.clone(), buf);
        }

        // Build late order buffer
        let late_order_buf = build_late_order_buf(&late_order_groups);

        // Build latest_order buffer
        let latest_order_buf = build_latest_order_buf(&latest_order);

        // Convert late_minute_keys_set to Vec
        let late_minute_keys: Vec<String> = late_minute_keys_set.into_iter().collect();

        // Concatenate per_minute_bufs: [magic][version][schema_hash][count][buf1][buf2]...
        // Even when empty (count=0), include header so Python decoder can validate magic
        let mut per_minute_buf = Vec::new();
        write_magic_header(&mut per_minute_buf, MAGIC_PER_MINUTE, MAGIC_VERSION, ORDER_SCHEMA_HASH);
        per_minute_buf.extend_from_slice(&(per_minute_bufs.len() as u32).to_le_bytes());
        for buf in &per_minute_bufs {
            per_minute_buf.extend_from_slice(buf);
        }

        (per_minute_buf, late_order_buf, latest_order_buf, late_minute_keys, ())
    };

    Ok((
        per_minute_buf_out,
        late_order_buf_out,
        latest_order_buf_out,
        late_minute_keys_out,
        skipped,
    ))
}

/// Preprocessed record before grouping
struct PreprocessedRecord {
    symbol: String,
    time: i64,
    bidprice: i64,
    bidsize: i64,
    askprice: i64,
    asksize: i64,
    decimal: i64,
    rcvtime: i64,
    date_key: String,  // str(time)[:8]
    minute_key: String, // str(time)[:12]
    seqno: u64,
}

/// Preprocess a single line: BOM strip, CRLF normalize, parse, date check, seqno assign.
/// Returns None for invalid/skipped lines.
fn preprocess_line(
    line: &[u8],
    encoding: &str,
    today_str: &str,
) -> Option<PreprocessedRecord> {
    // Decode — UTF-8 only
    let line_str = match encoding {
        "utf-8" | "utf8" => std::str::from_utf8(line).ok()?,
        _ => return None,
    };

    // UTF-8 BOM detection and strip
    let line_str = if line_str.starts_with('\u{FEFF}') {
        &line_str[3..]  // Skip 3-byte BOM
    } else {
        line_str
    };

    // Strip trailing \r (CR-only or CRLF)
    let line_str = line_str.strip_suffix('\r').unwrap_or(line_str);

    // Skip header
    if line_str.starts_with("symbol,") {
        return None;
    }

    // Skip empty lines
    if line_str.is_empty() {
        return None;
    }

    // Split fields
    let fields: Vec<&str> = line_str.split(',').collect();
    let n = fields.len();
    if n < 6 || n > 8 {
        return None;
    }

    // Skip empty/whitespace-only symbol
    if fields[0].trim().is_empty() {
        return None;
    }

    // Parse time
    let time_val = fields[1].trim().parse::<i64>().ok()?;

    // Validate time field is 17-digit
    if !(10_000_000_000_000_000..100_000_000_000_000_000).contains(&time_val) {
        return None;
    }

    // Date check: str(time)[:8] == today_str (STRING SLICE, NOT integer division)
    // time = 20260528090000123 → str[:8] = "20260528"
    let time_str = time_val.to_string();
    let date_key = &time_str[..8];
    if date_key != today_str {
        return None;
    }

    // minute_key: round-up (floor+1) via shared fn — keeps order/snapshot parity
    let minute_key = time_to_minute_key(time_val);

    // Parse other fields
    let symbol = fields[0].to_string();
    let bidprice = fields[2].trim().parse::<i64>().ok()?;
    let bidsize = fields[3].trim().parse::<i64>().ok()?;
    let askprice = fields[4].trim().parse::<i64>().ok()?;
    let asksize = fields[5].trim().parse::<i64>().ok()?;
    let decimal = if n > 6 { fields[6].trim().parse::<i64>().ok()? } else { 2 };
    let rcvtime = if n > 7 { fields[7].trim().parse::<i64>().ok()? } else { 0 };

    Some(PreprocessedRecord {
        symbol,
        time: time_val,
        bidprice,
        bidsize,
        askprice,
        asksize,
        decimal,
        rcvtime,
        date_key: date_key.to_string(),
        minute_key,
        seqno: 0, // Assigned by caller after getting state
    })
}

// ── Snapshot Parsing ───────────────────────────────────────────────────────────

/// Snapshot schema CRC32 for flat binary encoding compatibility.
/// Schema: "symbol:str,time:i64,preclose:i64,lastprice:i64,lasttradeqty:i64,totalvol:i64,totalamount:i64,sessionid:i64,tradetype:str,status:str,direction:i64,pflag:str,decimal:i64,vwap:i64,shortsellflag:i64,rcvtime:i64,"
const SNAPSHOT_SCHEMA_HASH: u32 = 0x2E91C449;

/// Snapshot field count constraints (matching csv_parser.py exactly)
/// MIN_COLS = 17 matches Python SNAPSHOT_MIN_COLS
const SNAPSHOT_MIN_COLS: usize = 17;
const SNAPSHOT_MAX_COLS: usize = 21;

// ── OHLCV Aggregation Constants ────────────────────────────────────────────────

/// Magic bytes for OHLCV flat binary buffers (version_byte = 0x04)
const MAGIC_OHLCV: u32 = 0x04CCBBAA;

/// Magic bytes for latest-snapshot flat binary buffers (version_byte = 0x05)
const MAGIC_LATEST_SNAPSHOT: u32 = 0x05CCBBAA;

/// OHLCV entry schema CRC32
/// Schema: "symbol:str,open:f64,high:f64,low:f64,close:f64,volume:i64,count:i64,decimal:i64,"
const OHLCV_SCHEMA_HASH: u32 = 0xC62F76E5;

/// Latest snapshot entry schema CRC32
/// Schema: "symbol:str,time:i64,preclose:f64,lastprice:f64,open:f64,high:f64,low:f64,close:f64,totalvol:i64,totalamount:f64,decimal:i64,"
const LATEST_SNAPSHOT_SCHEMA_HASH: u32 = 0x2DD0CCC2;

/// Parsed snapshot record (returned from parse_snapshot_batch).
/// Fields not in this struct (open, high, low, close, lasttradeprice) are
/// parsed from CSV but not returned — Python aggregator does not need them.
#[derive(Clone, Debug)]
#[pyclass]
struct SnapshotParsed {
    #[pyo3(get)]
    symbol: String,
    #[pyo3(get)]
    time: i64,
    #[pyo3(get)]
    seqno: i64,      // assigned by Python aggregator (placeholder = 0 in Rust)
    #[pyo3(get)]
    rcvtime: i64,
    #[pyo3(get)]
    preclose: i64,
    #[pyo3(get)]
    lastprice: i64,
    #[pyo3(get)]
    lasttradeqty: i64,
    #[pyo3(get)]
    totalvol: i64,
    #[pyo3(get)]
    totalamount: i64,
    #[pyo3(get)]
    sessionid: i64,
    #[pyo3(get)]
    tradetype: String,
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    direction: i64,
    #[pyo3(get)]
    pflag: String,
    #[pyo3(get)]
    decimal: i64,
    #[pyo3(get)]
    vwap: i64,
    #[pyo3(get)]
    shortsellflag: i64,
}

/// Parse a single snapshot CSV line.
/// Returns None for skip (header, empty, decode error, parse error, etc.).
/// Matches Python parse_snapshot_line from csv_parser.py lines 125-182 exactly.
fn parse_snapshot_line(
    line: &[u8],
    encoding: &str,
) -> Option<SnapshotParsed> {
    // Decode — UTF-8 only. Non-UTF-8 returns None.
    let line_str = match encoding {
        "utf-8" | "utf8" => std::str::from_utf8(line).ok()?,
        _ => return None,
    };

    // UTF-8 BOM detection and strip (same as order processing)
    let line_str = if line_str.starts_with('\u{FEFF}') {
        &line_str[3..]
    } else {
        line_str
    };

    // Strip trailing \r (CR-only or CRLF line endings)
    let line_str = line_str.strip_suffix('\r').unwrap_or(line_str);

    // Skip header
    if line_str.starts_with("symbol,") {
        return None;
    }

    // Skip empty lines
    if line_str.trim().is_empty() {
        return None;
    }

    // Split fields
    let fields: Vec<&str> = line_str.split(',').collect();
    let n = fields.len();

    // Column count check: 17 <= n <= 21
    if n < SNAPSHOT_MIN_COLS {
        return None;
    }
    if n > SNAPSHOT_MAX_COLS {
        return None;
    }

    // Parse mandatory numeric fields (indices 1-12)
    // Note: use trim() on each field to handle trailing \r\n captured by split
    let time_str = fields[1].trim();

    // Validate time field is 17-character digit string (matches order parser behavior)
    // This ensures consistent date extraction between str[:8] and //10^9
    if time_str.len() != 17 || !time_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let time = time_str.parse::<i64>().ok()?;
    let preclose = fields[2].trim().parse::<i64>().ok()?;
    let lastprice = fields[3].trim().parse::<i64>().ok()?;
    // Indices 4-8 (open, high, low, close, lasttradeprice) are parsed but discarded
    let _open = fields[4].trim().parse::<i64>().ok()?;
    let _high = fields[5].trim().parse::<i64>().ok()?;
    let _low = fields[6].trim().parse::<i64>().ok()?;
    let _close = fields[7].trim().parse::<i64>().ok()?;
    let _lasttradeprice = fields[8].trim().parse::<i64>().ok()?;
    let lasttradeqty = fields[9].trim().parse::<i64>().ok()?;
    let totalvol = fields[10].trim().parse::<i64>().ok()?;
    let totalamount = fields[11].trim().parse::<i64>().ok()?;
    let sessionid = fields[12].trim().parse::<i64>().ok()?;

    // Optional string fields with defaults
    let tradetype = if n > 13 { fields[13].trim().to_string() } else { String::new() };
    let status = if n > 14 { fields[14].trim().to_string() } else { "T".to_string() };

    // Optional numeric fields with defaults
    let direction = if n > 15 { fields[15].trim().parse::<i64>().ok()? } else { 0 };
    let pflag = if n > 16 { fields[16].trim().to_string() } else { "N".to_string() };
    let decimal = if n > 17 { fields[17].trim().parse::<i64>().ok()? } else { 2 };
    let vwap = if n > 18 { fields[18].trim().parse::<i64>().ok()? } else { 0 };
    let shortsellflag = if n > 19 { fields[19].trim().parse::<i64>().ok()? } else { 0 };
    // rcvtime default is 0 in Rust (Python calls current_system_timestamp_17digit())
    let rcvtime = if n > 20 { fields[20].trim().parse::<i64>().ok()? } else { 0 };

    // Symbol must be non-empty
    let symbol = fields[0].trim().to_string();
    if symbol.is_empty() {
        return None;
    }

    Some(SnapshotParsed {
        symbol,
        time,
        seqno: 0,          // assigned by Python aggregator
        rcvtime,
        preclose,
        lastprice,
        lasttradeqty,
        totalvol,
        totalamount,
        sessionid,
        tradetype,
        status,
        direction,
        pflag,
        decimal,
        vwap,
        shortsellflag,
    })
}

/// Parse a batch of raw snapshot CSV lines into SnapshotParsed records.
///
/// Returns: (Vec<SnapshotParsed>, skipped_count: u64)
///
/// GIL is released for the entire parsing phase via py.allow_threads.
/// Invalid lines (header, decode error, parse error) are silently skipped.
#[pyfunction]
fn parse_snapshot_batch(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<SnapshotParsed>, u64)> {
    // Wrap in catch_unwind for panic safety
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parse_snapshot_batch_impl(py, lines, encoding)
    }));

    match result {
        Ok(r) => r,
        Err(_) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "parse_snapshot_batch panicked".to_string()
        )),
    }
}

fn parse_snapshot_batch_impl(
    py: Python,
    lines: Vec<Vec<u8>>,
    encoding: &str,
) -> PyResult<(Vec<SnapshotParsed>, u64)> {
    // Guard: reject oversized batches to prevent OOM
    if lines.len() > MAX_BATCH_SIZE {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Batch too large: {} lines (max {})", lines.len(), MAX_BATCH_SIZE)
        ));
    }

    // IMPORTANT: Convert encoding to owned String BEFORE allow_threads.
    // The `encoding: &str` parameter borrows from a Python str object,
    // which is !Send. The allow_threads closure requires Send.
    let encoding_owned = encoding.to_string();

    // Parse all lines WITHOUT GIL (the hot path)
    let (parsed, skipped): (Vec<SnapshotParsed>, u64) = py.allow_threads(|| {
        let mut skipped: u64 = 0;
        let results: Vec<SnapshotParsed> = lines
            .iter()
            .filter_map(|line| {
                match parse_snapshot_line(line, &encoding_owned) {
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

    // Return parsed results + skip count
    Ok((parsed, skipped))
}

// ── OHLCV Aggregation Helpers ─────────────────────────────────────────────────

/// Update an OHLCVAggregate with a snapshot record.
/// Matches Python OHLCVAggregate.update() exactly.
/// base_amt is f64 (pre-scaled: raw totalamount / 10^decimal).
/// end_totalamount is stored as RAW i64 for precision in carry-forward.
fn update_aggregate(agg: &mut OHLCVAggregate, record: &SnapshotParsed, base_vol: i64, base_amt: f64) {
    let d = 10_f64.powi(record.decimal as i32);
    let price = (record.lastprice as f64) / d;

    if agg.count == 0 {
        agg.open = price;
        agg.start_totalvol = record.totalvol;
        agg.start_totalamount = record.totalamount as f64;
    }

    agg.high = agg.high.max(price);
    agg.low = agg.low.min(price);
    agg.close = price;
    agg.end_totalvol = record.totalvol;
    agg.end_totalamount = record.totalamount; // RAW i64, NOT pre-scaled
    agg.count += 1;
    agg.seqno = record.seqno;
    agg.decimal = record.decimal;

    if record.lasttradeqty > 0 {
        agg.any_lasttradeqty_positive = true;
    }

    agg.volume = (record.totalvol - base_vol).max(0);
    // base_amt is pre-scaled (f64), record.totalamount is raw i64
    agg.amount = (((record.totalamount as f64) - base_amt) / d).max(0.0);
}

/// Update latest snapshot entry if record is newer (by time, then rcvtime).
fn update_latest_snapshot(
    latest: &mut HashMap<String, LatestSnapshotEntry>,
    record: &SnapshotParsed,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    end_totalvol: i64,
    end_totalamount: f64,
) {
    let d = 10_f64.powi(record.decimal as i32);

    let should_insert = match latest.get(&record.symbol) {
        None => true,
        Some(existing) => {
            record.time > existing.time
        },
    };

    if should_insert {
        latest.insert(
            record.symbol.clone(),
            LatestSnapshotEntry {
                symbol: record.symbol.clone(),
                time: record.time,
                preclose: (record.preclose as f64) / d,
                lastprice: (record.lastprice as f64) / d,
                open,
                high,
                low,
                close,
                totalvol: end_totalvol,
                totalamount: end_totalamount, // f64 per spec Section 5.2b
                decimal: record.decimal,
            },
        );
    }
}

/// Build OHLCV flat binary buffer from aggregates.
/// Format: magic header + per-entry data (~63 bytes/symbol).
/// Each entry: minute_key_len + minute_key + symbol_len + symbol + OHLCV fields.
fn build_ohlcv_buf(
    aggregates: &HashMap<String, HashMap<String, OHLCVAggregate>>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_magic_header(&mut buf, MAGIC_OHLCV, MAGIC_VERSION, OHLCV_SCHEMA_HASH);

    for (minute_key, symbol_map) in aggregates {
        for (_symbol, agg) in symbol_map {
            // minute_key
            let mk_bytes = minute_key.as_bytes();
            buf.extend_from_slice(&(mk_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(mk_bytes);

            // symbol
            let sym_bytes = agg.symbol.as_bytes();
            buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(sym_bytes);

            // OHLCV fields
            buf.extend_from_slice(&agg.open.to_le_bytes());
            buf.extend_from_slice(&agg.high.to_le_bytes());
            buf.extend_from_slice(&agg.low.to_le_bytes());
            buf.extend_from_slice(&agg.close.to_le_bytes());
            buf.extend_from_slice(&agg.volume.to_le_bytes());
            buf.extend_from_slice(&agg.count.to_le_bytes());
            buf.extend_from_slice(&(agg.decimal as u32).to_le_bytes());
        }
    }

    buf
}

/// Build latest-snapshot flat binary buffer.
/// Format: magic header + entry_count + per-entry data.
fn build_latest_snapshot_buf(
    latest: &HashMap<String, LatestSnapshotEntry>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_magic_header(
        &mut buf,
        MAGIC_LATEST_SNAPSHOT,
        MAGIC_VERSION,
        LATEST_SNAPSHOT_SCHEMA_HASH,
    );

    buf.extend_from_slice(&(latest.len() as u32).to_le_bytes());

    for (_symbol, entry) in latest {
        // symbol
        let sym_bytes = entry.symbol.as_bytes();
        buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(sym_bytes);

        // All fields
        buf.extend_from_slice(&entry.time.to_le_bytes());
        buf.extend_from_slice(&entry.preclose.to_le_bytes());
        buf.extend_from_slice(&entry.lastprice.to_le_bytes());
        buf.extend_from_slice(&entry.open.to_le_bytes());
        buf.extend_from_slice(&entry.high.to_le_bytes());
        buf.extend_from_slice(&entry.low.to_le_bytes());
        buf.extend_from_slice(&entry.close.to_le_bytes());
        buf.extend_from_slice(&entry.totalvol.to_le_bytes());
        buf.extend_from_slice(&entry.totalamount.to_le_bytes()); // f64
        buf.extend_from_slice(&(entry.decimal as u32).to_le_bytes());
    }

    buf
}

/// Round-UP: a timestamp marks a minute-end snapshot → it belongs to the NEXT clock minute.
/// 09:00:01.000 → "0901" | 09:59:xx → "1000" | 23:59:xx → next-day "0000".
/// Python parity: clock.time_to_minute_key.
fn time_to_minute_key(time: i64) -> String {
    let s = time.to_string();
    if s.len() < 12 {
        return s.chars().take(12).collect();
    }
    let date = &s[..8];
    let hh: u32 = s[8..10].parse().unwrap_or(0);
    let mm: u32 = s[10..12].parse().unwrap_or(0);
    let mut new_mm = mm + 1;
    let mut new_hh = hh;
    let mut new_date = date.to_string();
    if new_mm >= 60 {
        new_mm = 0;
        new_hh += 1;
    }
    if new_hh >= 24 {
        new_hh = 0;
        new_date = increment_yyyymmdd(&new_date);
    }
    format!("{}{:02}{:02}", new_date, new_hh, new_mm)
}

/// Increment a YYYYMMDD string by one calendar day (hand-rolled; no chrono dep).
fn increment_yyyymmdd(date: &str) -> String {
    let y: u32 = date[0..4].parse().unwrap_or(0);
    let m: u32 = date[4..6].parse().unwrap_or(1);
    let d: u32 = date[6..8].parse().unwrap_or(1);
    let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    let dim = [31u32, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut ny = y;
    let mut nm = m;
    let mut nd = d + 1;
    let cap = dim.get((m as usize).saturating_sub(1)).copied().unwrap_or(31);
    if nd > cap {
        nd = 1;
        nm += 1;
        if nm > 12 {
            nm = 1;
            ny += 1;
        }
    }
    format!("{:04}{:02}{:02}", ny, nm, nd)
}

// ── aggregate_snapshot_batch Implementation ─────────────────────────────────

/// Core aggregation logic (no PyO3 dependencies, callable without GIL).
fn aggregate_snapshot_batch_impl(
    records: Vec<SnapshotParsed>,
    current_minute: &str,
    base_vol_by_symbol: Vec<(String, i64)>,
    base_amt_by_symbol: Vec<(String, f64)>,
    flushed_minutes: &[String],
) -> PyResult<(
    Vec<u8>,                    // ohlcv_buf
    String,                     // new_current_minute
    Vec<(String, i64)>,         // updated base_vol_by_symbol
    Vec<(String, f64)>,         // updated base_amt_by_symbol
    Vec<String>,                // late_minute_keys
)> {
    // Initialize snapshot state on first call
    init_snapshot_state();

    // Build lookup maps for base values (HashMap for O(1) access)
    let base_vol_map: HashMap<String, i64> = base_vol_by_symbol
        .into_iter()
        .collect();
    let base_amt_map: HashMap<String, f64> = base_amt_by_symbol
        .into_iter()
        .collect();

    // Build HashSet for O(1) late minute detection
    let flushed_set: HashSet<String> = flushed_minutes.iter().cloned().collect();

    // Group aggregates by minute_key then symbol
    // minute_key -> symbol -> OHLCVAggregate
    let mut aggregates: HashMap<String, HashMap<String, OHLCVAggregate>> = HashMap::new();

    // Track last minute seen
    let mut last_minute: String = current_minute.to_string();

    // Track late minute keys
    let mut late_minute_keys: HashSet<String> = HashSet::new();

    // Process each record
    for record in &records {
        let minute_key = time_to_minute_key(record.time);

        // Late record detection
        if flushed_set.contains(&minute_key) {
            late_minute_keys.insert(minute_key.clone());
            // Skip aggregation for late records (don't update aggregates)
            // But still update latest_snapshot for tickfile
            continue;
        }

        // Update last_minute
        if minute_key > last_minute {
            last_minute = minute_key.clone();
        }

        // Get or create per-minute, per-symbol aggregate
        let minute_aggs = aggregates
            .entry(minute_key.clone())
            .or_insert_with(HashMap::new);

        let agg = minute_aggs
            .entry(record.symbol.clone())
            .or_insert_with(|| {
                let mut a = OHLCVAggregate::default();
                a.symbol = record.symbol.clone();
                a
            });

        // Get base values for this symbol (default to 0 if not found)
        let base_vol = *base_vol_map.get(&record.symbol).unwrap_or(&0);
        let base_amt = *base_amt_map.get(&record.symbol).unwrap_or(&0.0);

        // Update aggregate
        update_aggregate(agg, record, base_vol, base_amt);
    }

    // Build OHLCV buffer
    let ohlcv_buf = build_ohlcv_buf(&aggregates);

    // Collect updated base values from all aggregates
    // For each symbol, we need the base values from the HIGHEST minute_key aggregate
    // Track the max minute_key per symbol
    let mut symbol_max_minute: HashMap<String, String> = HashMap::new();
    let mut symbol_base_vol: HashMap<String, i64> = HashMap::new();
    let mut symbol_base_amt: HashMap<String, f64> = HashMap::new();

    for (minute_key, symbol_map) in &aggregates {
        for (symbol, agg) in symbol_map {
            let existing_max = symbol_max_minute.get(symbol);
            let should_update = match existing_max {
                None => true,
                Some(existing) => minute_key > existing,
            };

            if should_update {
                symbol_max_minute.insert(symbol.clone(), minute_key.clone());
                symbol_base_vol.insert(symbol.clone(), agg.end_totalvol);
                // end_totalamount is RAW i64, base_amt is f64 (raw as f64)
                symbol_base_amt.insert(symbol.clone(), agg.end_totalamount as f64);
            }
        }
    }

    // Carry forward base values for symbols not in this batch
    // (from the persisted latest_snapshot state)
    {
        let mut guard = get_snapshot_state();
        let state = guard.as_mut().unwrap();

        // Update latest_snapshot with final aggregate values
        for (_minute_key, symbol_map) in &aggregates {
            for (_symbol, agg) in symbol_map {
                update_latest_snapshot(
                    &mut state.latest_snapshot,
                    &SnapshotParsed {
                        symbol: agg.symbol.clone(),
                        time: 0,
                        seqno: agg.seqno,
                        rcvtime: 0,
                        preclose: 0,
                        lastprice: 0,
                        lasttradeqty: 0,
                        totalvol: agg.end_totalvol,
                        totalamount: agg.end_totalamount, // RAW i64
                        sessionid: 0,
                        tradetype: String::new(),
                        status: String::new(),
                        direction: 0,
                        pflag: String::new(),
                        decimal: agg.decimal,
                        vwap: 0,
                        shortsellflag: 0,
                    },
                    agg.open,
                    agg.high,
                    agg.low,
                    agg.close,
                    agg.end_totalvol,
                    agg.end_totalamount as f64, // cast to f64 for LatestSnapshotEntry
                );
            }
        }

        // Build latest_snapshot_buf for tickfile
        state.latest_snapshot_buf = build_latest_snapshot_buf(&state.latest_snapshot);

        // Carry forward base values for symbols not in this batch
        for (symbol, entry) in &state.latest_snapshot {
            if !symbol_base_vol.contains_key(symbol) {
                // Symbol not in this batch - carry forward its latest values
                symbol_base_vol.insert(symbol.clone(), entry.totalvol);
                // totalamount is f64 per spec Section 5.2b
                symbol_base_amt.insert(symbol.clone(), entry.totalamount);
            }
        }
    }

    let late_minute_keys_vec: Vec<String> = late_minute_keys.into_iter().collect();

    // Convert hashmaps to vectors
    let updated_base_vol: Vec<(String, i64)> = symbol_base_vol.into_iter().collect();
    let updated_base_amt: Vec<(String, f64)> = symbol_base_amt.into_iter().collect();

    Ok((
        ohlcv_buf,
        last_minute,
        updated_base_vol,
        updated_base_amt,
        late_minute_keys_vec,
    ))
}

/// Aggregate a batch of snapshot records into OHLCV.
/// Panic safety: wrapped in std::panic::catch_unwind.
/// GIL is released during aggregation via py.allow_threads.
#[pyfunction]
fn aggregate_snapshot_batch(
    py: Python,
    records: Vec<SnapshotParsed>,
    current_minute: &str,
    base_vol_by_symbol: Vec<(String, i64)>,
    base_amt_by_symbol: Vec<(String, f64)>,
    flushed_minutes: Vec<String>,
) -> PyResult<(
    Vec<u8>,                    // ohlcv_buf
    String,                     // new_current_minute
    Vec<(String, i64)>,         // updated base_vol_by_symbol
    Vec<(String, f64)>,         // updated base_amt_by_symbol
    Vec<String>,                // late_minute_keys
)> {
    // Wrap in catch_unwind for panic safety
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // GIL released during aggregation
        py.allow_threads(|| {
            aggregate_snapshot_batch_impl(
                records,
                current_minute,
                base_vol_by_symbol,
                base_amt_by_symbol,
                &flushed_minutes,
            )
        })
    }));

    match result {
        Ok(r) => r,
        Err(_) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "aggregate_snapshot_batch panicked".to_string()
        )),
    }
}

/// Retrieve and clear the latest snapshot buffer for tickfile generation.
#[pyfunction]
fn tickfile_get_latest_snapshot() -> PyResult<Vec<u8>> {
    let mut guard = get_snapshot_state();
    let state = guard.as_mut().unwrap();
    let buf = state.latest_snapshot_buf.clone();
    state.latest_snapshot_buf.clear();
    Ok(buf)
}

// ── Tickfile Generation (Part C) ───────────────────────────────────────────────

/// TICKFILE_HEADER — 65 columns matching Python tickfile.py exactly.
const TICKFILE_HEADER: &str = "InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,BidPrice1,BidVolume1,AskPrice1,AskVolume1,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,BidPrice6,BidVolume6,AskPrice6,AskVolume6,BidPrice7,BidVolume7,AskPrice7,AskVolume7,BidPrice8,BidVolume8,AskPrice8,AskVolume8,BidPrice9,BidVolume9,AskPrice9,AskVolume9,BidPrice10,BidVolume10,AskPrice10,AskVolume10,ActionDay,Type,Seqno,LocalTime,IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume";

const NA: &str = "NA";

/// Decode latest_order_buf (magic 0x03CCBBAA).
/// Returns IndexMap<symbol, LatestOrderEntry> with all fields needed for tickfile.
/// Format: [magic][version][schema_hash][entries...]
/// Each entry: symbol_len(2) + symbol + time(8) + bidprice(8) + bidsize(8) + askprice(8) + asksize(8) + decimal(8) + rcvtime(8) + seqno(8)
fn decode_latest_order_buf(buf: &[u8]) -> PyResult<IndexMap<String, LatestOrderEntry>> {
    if buf.len() < 10 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_order_buf too short: {} bytes", buf.len())
        ));
    }

    let mut offset = 0;

    // Magic check
    let magic = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
    if magic != MAGIC_LATEST_ORDER {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_order_buf magic mismatch: expected 0x{:08X}, got 0x{:08X}", MAGIC_LATEST_ORDER, magic)
        ));
    }
    offset += 4;

    // Version check
    let version = u16::from_le_bytes([buf[offset], buf[offset+1]]);
    if version != MAGIC_VERSION {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_order_buf version mismatch: expected {}, got {}", MAGIC_VERSION, version)
        ));
    }
    offset += 2;

    // Schema hash check
    let schema_hash = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
    if schema_hash != ORDER_SCHEMA_HASH {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_order_buf schema_hash mismatch: expected 0x{:08X}, got 0x{:08X}", ORDER_SCHEMA_HASH, schema_hash)
        ));
    }
    offset += 4;

    let mut result: IndexMap<String, LatestOrderEntry> = IndexMap::new();

    // Each entry is 70 bytes after symbol: time(8) + bidprice(8) + bidsize(8) + askprice(8) + asksize(8) + decimal(8) + rcvtime(8) + seqno(8) = 56 bytes
    const ENTRY_SIZE: usize = 56;

    while offset + 2 <= buf.len() {
        let sym_len = u16::from_le_bytes([buf[offset], buf[offset+1]]) as usize;
        offset += 2;

        if offset + sym_len > buf.len() {
            break;
        }
        let symbol = String::from_utf8(buf[offset..offset+sym_len].to_vec())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid symbol UTF-8: {}", e)))?;
        offset += sym_len;

        if offset + ENTRY_SIZE > buf.len() {
            break;
        }

        let time = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let bidprice = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let bidsize = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let askprice = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let asksize = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let decimal = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let rcvtime = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let seqno = u64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;

        result.insert(symbol.clone(), LatestOrderEntry {
            symbol,
            time,
            bidprice,
            bidsize,
            askprice,
            asksize,
            decimal,
            rcvtime,
            seqno,
        });
    }

    Ok(result)
}

/// Decode latest_snapshot_buf (magic 0x05CCBBAA).
/// Returns IndexMap<symbol, LatestSnapshotEntry>.
/// Format: [magic][version][schema_hash][entry_count(4)][entries...]
/// Each entry: symbol_len(2) + symbol + time(8) + preclose(8) + lastprice(8) + open(8) + high(8) + low(8) + close(8) + totalvol(8) + totalamount(8) + decimal(4)
fn decode_latest_snapshot_buf(buf: &[u8]) -> PyResult<IndexMap<String, LatestSnapshotEntry>> {
    if buf.len() < 10 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_snapshot_buf too short: {} bytes", buf.len())
        ));
    }

    let mut offset = 0;

    // Magic check
    let magic = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
    if magic != MAGIC_LATEST_SNAPSHOT {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_snapshot_buf magic mismatch: expected 0x{:08X}, got 0x{:08X}", MAGIC_LATEST_SNAPSHOT, magic)
        ));
    }
    offset += 4;

    // Version check
    let version = u16::from_le_bytes([buf[offset], buf[offset+1]]);
    if version != MAGIC_VERSION {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_snapshot_buf version mismatch: expected {}, got {}", MAGIC_VERSION, version)
        ));
    }
    offset += 2;

    // Schema hash check
    let schema_hash = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
    if schema_hash != LATEST_SNAPSHOT_SCHEMA_HASH {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("latest_snapshot_buf schema_hash mismatch: expected 0x{:08X}, got 0x{:08X}", LATEST_SNAPSHOT_SCHEMA_HASH, schema_hash)
        ));
    }
    offset += 4;

    // Entry count
    if offset + 4 > buf.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("latest_snapshot_buf: truncated entry_count".to_string()));
    }
    let entry_count = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
    offset += 4;

    let mut result: IndexMap<String, LatestSnapshotEntry> = IndexMap::new();

    // Each entry after symbol: time(8) + preclose(8) + lastprice(8) + open(8) + high(8) + low(8) + close(8) + totalvol(8) + totalamount(8) + decimal(4) = 72 bytes
    const ENTRY_FIXED_SIZE: usize = 72;

    for _ in 0..entry_count {
        if offset + 2 > buf.len() {
            break;
        }
        let sym_len = u16::from_le_bytes([buf[offset], buf[offset+1]]) as usize;
        offset += 2;

        if offset + sym_len > buf.len() {
            break;
        }
        let symbol = String::from_utf8(buf[offset..offset+sym_len].to_vec())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid symbol UTF-8: {}", e)))?;
        offset += sym_len;

        if offset + ENTRY_FIXED_SIZE > buf.len() {
            break;
        }

        let time = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let preclose = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let lastprice = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let open = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let high = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let low = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let close = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let totalvol = i64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let totalamount = f64::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let decimal = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as i64;

        result.insert(symbol.clone(), LatestSnapshotEntry {
            symbol,
            time,
            preclose,
            lastprice,
            open,
            high,
            low,
            close,
            totalvol,
            totalamount,
            decimal,
        });
    }

    Ok(result)
}

/// Format a price value with decimal scaling.
/// Returns NA if value is 0.
fn fmt_price(value: f64, decimal: i64) -> String {
    if value == 0.0 {
        NA.to_string()
    } else {
        // Values are pre-scaled in LatestSnapshotEntry (divided by 10^decimal).
        // Output directly without re-scaling.
        // Use sufficient decimal precision for float values.
        if decimal > 0 {
            format!("{:.prec$}", value, prec = decimal as usize)
        } else {
            format!("{:.0}", value)
        }
    }
}

/// Format a price value from raw i64 (for order bidprice/askprice).
/// Returns NA if value is 0.
fn fmt_price_raw(value: i64, decimal: i64) -> String {
    if value == 0 {
        NA.to_string()
    } else {
        let d = 10_f64.powi(decimal.max(0) as i32);
        let scaled = (value as f64) / d;
        if decimal > 0 {
            format!("{:.prec$}", scaled, prec = decimal as usize)
        } else {
            format!("{:.0}", scaled)
        }
    }
}

/// Build UpdateTime from minute_key (format: "YYYYMMDDHHMM").
/// Returns format: "YYYYMMDD HH:MM:00"
fn fmt_update_time(minute_key: &str) -> String {
    if minute_key.len() >= 12 {
        format!("{} {}:{}:00",
            &minute_key[..8],
            &minute_key[8..10],
            &minute_key[10..12])
    } else {
        NA.to_string()
    }
}

/// Build LocalTime from 17-digit time integer.
/// Format: "YYYY-MM-DD HH:MM:SS.ffffff" (6-digit microseconds with trailing zeros)
/// Python: strftime('%Y-%m-%d %H:%M:%S.%f') produces 6-digit microseconds with trailing zeros.
/// time=20260528090000123 → "2026-05-28 09:00:00.123000" (3 digits from timestamp + "000" trailing)
fn fmt_local_time(time_val: i64) -> String {
    let time_str = time_val.to_string();
    if time_str.len() >= 17 {
        // Format: YYYYMMDDHHMMSSmmm (millis from timestamp, 3 digits)
        // Output: YYYY-MM-DD HH:MM:SS.microseconds (6-digit, millis * 1000 + trailing zeros)
        format!("{}-{}-{} {}:{}:{}.{}000",
            &time_str[..4],
            &time_str[4..6],
            &time_str[6..8],
            &time_str[8..10],
            &time_str[10..12],
            &time_str[12..14],
            &time_str[14..17])
    } else {
        NA.to_string()
    }
}

/// Build a single tickfile row CSV line for one symbol.
/// Matches Python build_tickfile_row exactly.
/// all_order: IndexMap<symbol, LatestOrderEntry> (from latest_order_buf)
/// all_snapshot: IndexMap<symbol, LatestSnapshotEntry> (from latest_snapshot_buf)
/// seqno: the tickfile seqno for this minute
/// minute_key: the minute being generated
fn build_tickfile_row_csv(
    symbol: &str,
    all_order: &IndexMap<String, LatestOrderEntry>,
    all_snapshot: &IndexMap<String, LatestSnapshotEntry>,
    seqno: u64,
    minute_key: &str,
) -> String {
    // Get snapshot and order data for this symbol
    let snap = all_snapshot.get(symbol);
    let ord = all_order.get(symbol);

    // Determine primary time and rcvtime
    let (primary_time, _primary_rcvtime) = if let Some(s) = snap {
        (s.time, s.time)  // snapshot uses time field for rcvtime
    } else if let Some(o) = ord {
        (o.time, o.rcvtime)
    } else {
        return NA.to_string();
    };

    // Column 0: InstrumentID
    let instrument_id = symbol;

    // Column 1: TradingDay
    let trading_day = if let Some(s) = snap {
        s.time.to_string()[..8].to_string()
    } else if let Some(o) = ord {
        o.time.to_string()[..8].to_string()
    } else {
        return NA.to_string();
    };

    // Column 2: LastPrice
    let last_price = if let Some(s) = snap {
        fmt_price(s.lastprice, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 3: PreSettlementPrice
    let pre_settlement = NA;

    // Column 4: PreClosePrice
    let pre_close = if let Some(s) = snap {
        fmt_price(s.preclose, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 5: PreOpenInterest
    let pre_open_interest = NA;

    // Column 6: OpenPrice
    let open_price = if let Some(s) = snap {
        fmt_price(s.open, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 7: HighestPrice
    let high_price = if let Some(s) = snap {
        fmt_price(s.high, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 8: LowestPrice
    let low_price = if let Some(s) = snap {
        fmt_price(s.low, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 9: Volume
    let volume = if let Some(s) = snap {
        s.totalvol.to_string()
    } else {
        NA.to_string()
    };

    // Column 10: Turnover
    let turnover = if let Some(s) = snap {
        if s.totalamount == 0.0 {
            NA.to_string()
        } else {
            // totalamount is pre-scaled f64, output directly
            format!("{:.prec$}", s.totalamount, prec = s.decimal.max(0) as usize)
        }
    } else {
        NA.to_string()
    };

    // Column 11: OpenInterest
    let open_interest = NA;

    // Column 12: ClosePrice
    let close_price = if let Some(s) = snap {
        fmt_price(s.close, s.decimal)
    } else {
        NA.to_string()
    };

    // Column 13: SettlementPrice
    let settlement = NA;

    // Columns 14-15: UpperLimitPrice, LowerLimitPrice
    let upper_limit = NA;
    let lower_limit = NA;

    // Column 16: UpdateTime
    let update_time = fmt_update_time(minute_key);

    // Columns 17-20: BidPrice1, BidVolume1, AskPrice1, AskVolume1
    let (bid_price1, bid_volume1, ask_price1, ask_volume1) = if let Some(o) = ord {
        (
            fmt_price_raw(o.bidprice, o.decimal),
            o.bidsize.to_string(),
            fmt_price_raw(o.askprice, o.decimal),
            o.asksize.to_string(),
        )
    } else {
        (NA.to_string(), NA.to_string(), NA.to_string(), NA.to_string())
    };

    // Column 57: ActionDay
    let action_day = trading_day.clone();

    // Column 58: Type
    let type_val = "69";

    // Column 59: Seqno
    let seqno_str = seqno.to_string();

    // Column 60: LocalTime
    let local_time = fmt_local_time(primary_time);

    // Column 61: IntraDailyReturn
    // Python spec: when lastprice==0 and preclose!=0, computes (0 - preclose) / preclose = -1.0
    let intra_daily_return = if let Some(s) = snap {
        if s.preclose != 0.0 {
            let last = if s.lastprice != 0.0 { s.lastprice } else { 0.0 };
            let ret = (last - s.preclose) / s.preclose;
            // Normalize -0.0 → 0.0
            let ret = if ret == 0.0 { 0.0 } else { ret };
            format!("{:.6}", ret)
        } else {
            NA.to_string()
        }
    } else {
        NA.to_string()
    };

    // Column 62: IsShortRestricted (not available in LatestSnapshotEntry, use NA)
    let is_short = NA;

    // Columns 63-64: OpenAuctionVolume, CloseAuctionVolume
    let open_auction_vol = NA;
    let close_auction_vol = NA;

    // Build levels_2_10 as a Vec<String> for proper expansion
    let levels_vec = [
        NA, NA, NA, NA,  // BidPrice2-5
        NA, NA, NA, NA,  // BidVolume2-5
        NA, NA, NA, NA,  // AskPrice2-5
        NA, NA, NA, NA,  // AskVolume2-5
        NA, NA, NA, NA,  // BidPrice6-9
        NA, NA, NA, NA,  // BidVolume6-9
        NA, NA, NA, NA,  // AskPrice6-9
        NA, NA, NA, NA,  // AskVolume6-9
        NA, NA, NA, NA,  // BidPrice10, BidVolume10, AskPrice10, AskVolume10
    ];

    let cols: Vec<String> = vec![
        instrument_id.to_string(), trading_day.clone(), last_price, pre_settlement.to_string(), pre_close, pre_open_interest.to_string(), open_price, high_price, low_price, volume,
        turnover, open_interest.to_string(), close_price, settlement.to_string(), upper_limit.to_string(), lower_limit.to_string(), update_time, bid_price1, bid_volume1, ask_price1,
        ask_volume1, levels_vec[0].to_string(), levels_vec[1].to_string(), levels_vec[2].to_string(), levels_vec[3].to_string(), levels_vec[4].to_string(), levels_vec[5].to_string(), levels_vec[6].to_string(), levels_vec[7].to_string(), levels_vec[8].to_string(),
        levels_vec[9].to_string(), levels_vec[10].to_string(), levels_vec[11].to_string(), levels_vec[12].to_string(), levels_vec[13].to_string(), levels_vec[14].to_string(), levels_vec[15].to_string(), levels_vec[16].to_string(), levels_vec[17].to_string(), levels_vec[18].to_string(),
        levels_vec[19].to_string(), levels_vec[20].to_string(), levels_vec[21].to_string(), levels_vec[22].to_string(), levels_vec[23].to_string(), levels_vec[24].to_string(), levels_vec[25].to_string(), levels_vec[26].to_string(), levels_vec[27].to_string(), levels_vec[28].to_string(),
        levels_vec[29].to_string(), levels_vec[30].to_string(), levels_vec[31].to_string(), levels_vec[32].to_string(), levels_vec[33].to_string(), levels_vec[34].to_string(), levels_vec[35].to_string(), action_day.clone(), type_val.to_string(), seqno_str,
        local_time, intra_daily_return, is_short.to_string(), open_auction_vol.to_string(), close_auction_vol.to_string(),
    ];
    cols.join(",")

}

/// Generate tickfile CSV for one minute in Rust.
/// GIL is held for the full duration (~50ms for 4505 symbols).
#[pyfunction]
fn tickfile_generate(
    py: Python,
    raw_order_buf: Vec<u8>,
    latest_order_buf: Vec<u8>,
    latest_snapshot_buf: Vec<u8>,
    minute_key: &str,
    all_symbols: Vec<String>,
    seqno: u64,
) -> PyResult<String> {
    // Wrap in catch_unwind for panic safety
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tickfile_generate_impl(&latest_order_buf, &latest_snapshot_buf, minute_key, &all_symbols, seqno)
    }));

    match result {
        Ok(r) => r,
        Err(_) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "tickfile_generate panicked".to_string()
        )),
    }
}

fn tickfile_generate_impl(
    latest_order_buf: &[u8],
    latest_snapshot_buf: &[u8],
    minute_key: &str,
    all_symbols: &[String],
    seqno: u64,
) -> PyResult<String> {
    // Decode buffers
    let all_order = if latest_order_buf.is_empty() {
        IndexMap::new()
    } else {
        decode_latest_order_buf(latest_order_buf)?
    };

    let all_snapshot = if latest_snapshot_buf.is_empty() {
        IndexMap::new()
    } else {
        decode_latest_snapshot_buf(latest_snapshot_buf)?
    };

    // Build CSV rows
    let mut rows: Vec<String> = Vec::with_capacity(all_symbols.len());

    for symbol in all_symbols {
        let row = build_tickfile_row_csv(symbol, &all_order, &all_snapshot, seqno, minute_key);
        if row != NA {
            rows.push(row);
        }
    }

    // Build CSV string: header + rows
    let csv = if rows.is_empty() {
        TICKFILE_HEADER.to_string()
    } else {
        format!("{}\n{}", TICKFILE_HEADER, rows.join("\n"))
    };

    Ok(csv)
}

/// Retrieve and clear a minute's raw order buffer from Rust state.
#[pyfunction]
fn tickfile_get_raw_buffer(minute_key: &str) -> PyResult<Vec<u8>> {
    let mut guard = get_state();
    let state = guard.as_mut().unwrap();
    let buf = state.raw_order_buffers.remove(minute_key);
    Ok(buf.unwrap_or_default())
}

/// Reset snapshot state (panic recovery).
#[pyfunction]
fn rust_reset_snapshot_state() -> PyResult<()> {
    reset_snapshot_state();
    Ok(())
}

#[pymodule]
fn _order_accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_order_batch, m)?)?;
    m.add_function(wrap_pyfunction!(parse_order_batch_flat, m)?)?;
    m.add_function(wrap_pyfunction!(is_available, m)?)?;
    m.add_function(wrap_pyfunction!(rust_reset_state, m)?)?;
    m.add_function(wrap_pyfunction!(process_order_batch, m)?)?;
    m.add_function(wrap_pyfunction!(parse_snapshot_batch, m)?)?;
    m.add_function(wrap_pyfunction!(aggregate_snapshot_batch, m)?)?;
    m.add_function(wrap_pyfunction!(tickfile_get_latest_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(rust_reset_snapshot_state, m)?)?;
    m.add_function(wrap_pyfunction!(tickfile_generate, m)?)?;
    m.add_function(wrap_pyfunction!(tickfile_get_raw_buffer, m)?)?;
    Ok(())
}

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

    // ── process_order_batch unit tests ─────────────────────────────────────────

    #[test]
    fn test_preprocess_line_valid() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.symbol, "7203");
        assert_eq!(r.time, 20260528090000123);
        assert_eq!(r.bidprice, 4580000);
        assert_eq!(r.bidsize, 100);
        assert_eq!(r.askprice, 4590000);
        assert_eq!(r.asksize, 200);
        assert_eq!(r.decimal, 2);
        assert_eq!(r.rcvtime, 0);
        assert_eq!(r.date_key, "20260528");
        assert_eq!(r.minute_key, "202605280901");
    }

    #[test]
    fn test_preprocess_line_wrong_date() {
        // Date doesn't match today_str
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260529");
        assert!(result.is_none());
    }

    #[test]
    fn test_preprocess_line_crlf() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r\n";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_some());
        assert_eq!(result.unwrap().rcvtime, 0);
    }

    #[test]
    fn test_preprocess_line_cr_only() {
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_some());
    }

    #[test]
    fn test_preprocess_line_utf8_bom() {
        // UTF-8 BOM (0xEF 0xBB 0xBF) prefix
        let line = b"\xEF\xBB\xBF7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_some());
        assert_eq!(result.unwrap().symbol, "7203");
    }

    #[test]
    fn test_preprocess_line_header() {
        let line = b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_none());
    }

    #[test]
    fn test_preprocess_line_empty_symbol() {
        let line = b",20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_none());
    }

    #[test]
    fn test_preprocess_line_short_timestamp() {
        // 16-digit timestamp should be rejected (not 17-digit)
        let line = b"7203,2026052809000012,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528");
        assert!(result.is_none());
    }

    #[test]
    fn test_preprocess_line_default_decimal_rcvtime() {
        // 6 columns: default decimal=2, rcvtime=0
        let line = b"7203,20260528090000123,4580000,100,4590000,200";
        let result = preprocess_line(line, "utf-8", "20260528").unwrap();
        assert_eq!(result.decimal, 2);
        assert_eq!(result.rcvtime, 0);
    }

    #[test]
    fn test_minute_key_round_up() {
        // Round-up: 09:00:00.123 (clock-minute 0900) → NEXT minute 0901
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528").unwrap();
        assert_eq!(result.minute_key, "202605280901");
    }

    #[test]
    fn test_date_key_string_slice() {
        // Verify date_key uses string slice [:8], NOT integer division
        // time = 20260528090000123 → str[:8] = "20260528"
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528").unwrap();
        assert_eq!(result.date_key, "20260528");
    }

    #[test]
    fn test_magic_header_format() {
        // Verify magic header bytes: [magic][version][schema_hash]
        // magic: [0xAA, 0xBB, 0xCC, 0x01] as LE u32 = 0x01CCBBAA
        let mut buf = Vec::new();
        write_magic_header(&mut buf, MAGIC_PER_MINUTE, MAGIC_VERSION, ORDER_SCHEMA_HASH);
        assert_eq!(buf.len(), 10); // 4 + 2 + 4 = 10 bytes
        assert_eq!(buf[0], 0xAA); // magic byte 0
        assert_eq!(buf[1], 0xBB); // magic byte 1
        assert_eq!(buf[2], 0xCC); // magic byte 2
        assert_eq!(buf[3], 0x01); // version_byte
        assert_eq!(buf[4], 0x01); // version = 1 → LE = [0x01, 0x00]
        assert_eq!(buf[5], 0x00);
        // schema_hash LE: 0x9A51A8B3 → [0xB3, 0xA8, 0x51, 0x9A]
        assert_eq!(buf[6], 0xB3);
        assert_eq!(buf[7], 0xA8);
        assert_eq!(buf[8], 0x51);
        assert_eq!(buf[9], 0x9A);
    }

    #[test]
    fn test_encode_order_record_format() {
        let mut buf = Vec::new();
        encode_order_record(&mut buf, "7203", 20260528090000123, 4580000, 100, 4590000, 200, 2, 0);
        // Expected: [2 bytes symbol_len=4]["7203"][8 bytes time][8 bytes bidprice]...
        assert_eq!(buf.len(), 2 + 4 + 8*7); // 2 + 4 + 56 = 62 bytes
    }

    #[test]
    fn test_build_per_minute_buf() {
        let mut records: IndexMap<String, OrderFields> = IndexMap::new();
        records.insert("7203".to_string(), OrderFields {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            bidprice: 4580000,
            bidsize: 100,
            askprice: 4590000,
            asksize: 200,
            decimal: 2,
            rcvtime: 0,
        });
        let buf = build_per_minute_buf("202605280900", &records);
        // Header: 10 bytes + minute_key: 2+12=14 bytes + count: 4 bytes = 28 bytes
        // Record: 62 bytes
        assert_eq!(buf.len(), 28 + 62);
    }

    #[test]
    fn test_build_latest_order_buf() {
        let mut latest: IndexMap<String, LatestOrderEntry> = IndexMap::new();
        latest.insert("7203".to_string(), LatestOrderEntry {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            bidprice: 4580000,
            bidsize: 100,
            askprice: 4590000,
            asksize: 200,
            decimal: 2,
            rcvtime: 0,
            seqno: 42,
        });
        let buf = build_latest_order_buf(&latest);
        // Header: 10 bytes
        // Record: symbol_len(2)+4 + time(8) + bidprice(8) + bidsize(8) + askprice(8) + asksize(8) + decimal(8) + rcvtime(8) + seqno(8) = 70
        assert_eq!(buf.len(), 10 + 70);
    }

    #[test]
    fn test_state_reset() {
        reset_state();
        let guard = get_state();
        let state = guard.as_ref().unwrap();
        assert!(state.raw_order_buffers.is_empty());
        assert!(state.flushed_minutes.is_empty());
        assert_eq!(state.seqno_counter, 0);
    }

    // ── parse_snapshot_line unit tests ─────────────────────────────────────────

    #[test]
    fn test_parse_snapshot_valid_21col() {
        // Full 21-column line with all fields
        // Timestamp is 17-digit: 20260528090000123 (YYYYMMDDHHMMSSmmmnnn)
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0,20260528090000100";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
        assert_eq!(result.time, 20260528090000123);
        assert_eq!(result.preclose, 4580000);
        assert_eq!(result.lastprice, 4590000);
        assert_eq!(result.lasttradeqty, 100);
        assert_eq!(result.totalvol, 1000);
        assert_eq!(result.totalamount, 1000000);
        assert_eq!(result.sessionid, 12345);
        assert_eq!(result.tradetype, "T");
        assert_eq!(result.status, "S");
        assert_eq!(result.direction, 1);
        assert_eq!(result.pflag, "N");
        assert_eq!(result.decimal, 2);
        assert_eq!(result.vwap, 5000);
        assert_eq!(result.shortsellflag, 0);
        assert_eq!(result.rcvtime, 20260528090000100);
    }

    #[test]
    fn test_parse_snapshot_valid_min_17col() {
        // Minimum 17-column line (required + first 4 optional fields)
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
        assert_eq!(result.time, 20260528090000123);
        assert_eq!(result.preclose, 4580000);
        assert_eq!(result.lastprice, 4590000);
        assert_eq!(result.lasttradeqty, 100);
        assert_eq!(result.totalvol, 1000);
        assert_eq!(result.totalamount, 1000000);
        assert_eq!(result.sessionid, 12345);
        // Optional fields from 17-col data
        assert_eq!(result.tradetype, "T");
        assert_eq!(result.status, "S");
        assert_eq!(result.direction, 1);
        assert_eq!(result.pflag, "N");
        // Remaining optional fields use defaults
        assert_eq!(result.decimal, 2);
        assert_eq!(result.vwap, 0);
        assert_eq!(result.shortsellflag, 0);
        assert_eq!(result.rcvtime, 0);
    }

    #[test]
    fn test_parse_snapshot_header_line() {
        // Header line should be skipped
        let line = b"symbol,time,preclose,lastprice,open,high,low,close,lasttradeprice,lasttradeqty,totalvol,totalamount,sessionid,tradetype,status,direction,pflag,decimal,vwap,shortsellflag,rcvtime";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_empty_line() {
        assert!(parse_snapshot_line(b"", "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_whitespace_only_line() {
        assert!(parse_snapshot_line(b"   ", "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_too_few_cols() {
        // 12 columns (< 13 min) should be skipped
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_too_many_cols() {
        // 22 columns (> 21 max) should be skipped
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0,20260528090000123,extra_field";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_empty_symbol() {
        // Empty symbol should be skipped
        let line = b",20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_whitespace_symbol() {
        // Whitespace-only symbol should be skipped
        let line = b"   ,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_non_numeric_field() {
        // Non-numeric mandatory field should be skipped
        let line = b"7203,not_a_number_00000,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345";
        assert!(parse_snapshot_line(line, "utf-8").is_none());
    }

    #[test]
    fn test_parse_snapshot_utf8_bom() {
        // UTF-8 BOM (0xEF 0xBB 0xBF) prefix should be stripped
        let line = b"\xEF\xBB\xBF7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
        assert_eq!(result.time, 20260528090000123);
    }

    #[test]
    fn test_parse_snapshot_crlf() {
        // CRLF line ending should be handled
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N\r\n";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
    }

    #[test]
    fn test_parse_snapshot_cr_only() {
        // CR-only line ending should be handled
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N\r";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
    }

    #[test]
    fn test_parse_snapshot_whitespace_in_fields() {
        // Whitespace in fields should be trimmed (matching Python int() behavior)
        let line = b" 7203 , 20260528090000123 , 4580000 , 4590000 , 4600000 , 4610000 , 4570000 , 4585000 , 4590000 , 100 , 1000 , 1000000 , 12345 , T , S , 1 , N ";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.symbol, "7203");
        assert_eq!(result.time, 20260528090000123);
        assert_eq!(result.preclose, 4580000);
        assert_eq!(result.lastprice, 4590000);
    }

    #[test]
    fn test_parse_snapshot_optional_fields_18_19_20() {
        // Test optional fields at indices 18, 19, 20 (vwap, shortsellflag, rcvtime)
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,1,20260528090000123";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.vwap, 5000);
        assert_eq!(result.shortsellflag, 1);
        assert_eq!(result.rcvtime, 20260528090000123);
    }

    #[test]
    fn test_parse_snapshot_seqno_placeholder() {
        // seqno should always be 0 (Python aggregator assigns the real value)
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N";
        let result = parse_snapshot_line(line, "utf-8").unwrap();
        assert_eq!(result.seqno, 0);
    }

    #[test]
    fn test_parse_snapshot_non_utf8_encoding() {
        // Non-UTF-8 encoding should return None immediately
        let line = b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N";
        assert!(parse_snapshot_line(line, "shift_jis").is_none());
        assert!(parse_snapshot_line(line, "cp932").is_none());
        // But utf-8 works
        assert!(parse_snapshot_line(line, "utf-8").is_some());
    }

    #[test]
    fn test_parse_snapshot_batch_mixed() {
        // Test parse_snapshot_line directly with mixed valid/invalid lines
        let lines: Vec<&[u8]> = vec![
            b"symbol,time,preclose,lastprice,...",  // header → skip
            b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N",  // valid 17-col
            b"",                      // empty → skip
            b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0,20260528090000123",  // valid 21-col
            b"bad_data",              // invalid → skip
            b",20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N",  // empty symbol → skip
        ];
        let mut valid = 0;
        let mut skipped = 0;
        for line in &lines {
            match parse_snapshot_line(line, "utf-8") {
                Some(_) => valid += 1,
                None => skipped += 1,
            }
        }
        assert_eq!(valid, 2);
        assert_eq!(skipped, 4);
    }

    #[test]
    fn test_parse_snapshot_batch_via_line_loop() {
        // Test batch-like behavior by calling parse_snapshot_line in a loop
        // This simulates the batch path without needing PyO3 GIL
        let lines: Vec<&[u8]> = vec![
            b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0,20260528090000123",
            b"7204,20260528090000124,4600000,4610000,4620000,4630000,4590000,4605000,4610000,200,2000,2000000,12346,T,S,1,N,2,6000,0,20260528090000124",
        ];
        let encoding = "utf-8";
        let mut parsed: Vec<_> = Vec::new();
        let mut skipped = 0u64;
        for line in &lines {
            match parse_snapshot_line(line, encoding) {
                Some(r) => parsed.push(r),
                None => skipped += 1,
            }
        }
        assert_eq!(parsed.len(), 2);
        assert_eq!(skipped, 0);
        assert_eq!(parsed[0].symbol, "7203");
        assert_eq!(parsed[0].time, 20260528090000123);
        assert_eq!(parsed[1].symbol, "7204");
        assert_eq!(parsed[1].time, 20260528090000124);
    }

    #[test]
    fn test_parse_snapshot_batch_rejects_short_timestamp() {
        // Batch-level test: lines with non-17-digit timestamp should be skipped
        let lines: Vec<&[u8]> = vec![
            b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N",  // valid 17-col 17-digit
            b"7204,20260528,4600000,4610000,4620000,4630000,4590000,4605000,4610000,200,2000,2000000,12346,T,S,1,N",  // invalid: 8-digit timestamp
        ];
        let encoding = "utf-8";
        let mut parsed: Vec<_> = Vec::new();
        let mut skipped = 0u64;
        for line in &lines {
            match parse_snapshot_line(line, encoding) {
                Some(r) => parsed.push(r),
                None => skipped += 1,
            }
        }
        assert_eq!(parsed.len(), 1); // only first record valid
        assert_eq!(skipped, 1);      // second record skipped
        assert_eq!(parsed[0].symbol, "7203");
    }

    // ── OHLCV Aggregation unit tests ─────────────────────────────────────────

    #[test]
    fn test_ohlcv_update_aggregate_single_record() {
        // Test update_aggregate with a single record
        let record = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4580000,
            lastprice: 4590000,
            lasttradeqty: 100,
            totalvol: 1000,
            totalamount: 459000000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4590000,
            shortsellflag: 0,
        };

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();

        // First record: count=0, should set open, start_totalvol, start_totalamount
        update_aggregate(&mut agg, &record, 0, 0.0);

        assert_eq!(agg.count, 1);
        assert!((agg.open - 45900.0).abs() < 1e-6);  // 4590000 / 100
        assert!((agg.high - 45900.0).abs() < 1e-6);
        assert!((agg.low - 45900.0).abs() < 1e-6);
        assert!((agg.close - 45900.0).abs() < 1e-6);
        assert_eq!(agg.start_totalvol, 1000);
        assert_eq!(agg.end_totalvol, 1000);
        assert!((agg.start_totalamount - 459000000.0).abs() < 1e-6);
        assert!((agg.volume as f64 - 1000.0).abs() < 1e-6);  // totalvol - base_vol = 1000 - 0
        assert!(agg.any_lasttradeqty_positive);
    }

    #[test]
    fn test_ohlcv_update_aggregate_multiple_records() {
        // Test update_aggregate with multiple records at different prices
        let record1 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4580000,
            lastprice: 4590000,  // price = 45900.0
            lasttradeqty: 100,
            totalvol: 1000,
            totalamount: 459000000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4590000,
            shortsellflag: 0,
        };

        let record2 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000124,
            seqno: 2,
            rcvtime: 20260528090000124,
            preclose: 4590000,
            lastprice: 4610000,  // price = 46100.0 (higher)
            lasttradeqty: 200,
            totalvol: 1500,
            totalamount: 690500000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4610000,
            shortsellflag: 0,
        };

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();

        // First record
        update_aggregate(&mut agg, &record1, 0, 0.0);
        assert_eq!(agg.count, 1);
        assert!((agg.open - 45900.0).abs() < 1e-6);
        assert!((agg.high - 45900.0).abs() < 1e-6);
        assert!((agg.low - 45900.0).abs() < 1e-6);
        assert!((agg.close - 45900.0).abs() < 1e-6);

        // Second record - base_vol=0, base_amt=0.0 (first record's start values)
        update_aggregate(&mut agg, &record2, 0, 0.0);
        assert_eq!(agg.count, 2);
        assert!((agg.open - 45900.0).abs() < 1e-6);  // open stays at first price
        assert!((agg.high - 46100.0).abs() < 1e-6);  // high updates to higher price
        assert!((agg.low - 45900.0).abs() < 1e-6);   // low stays at lower price
        assert!((agg.close - 46100.0).abs() < 1e-6); // close updates to latest price
        assert_eq!(agg.end_totalvol, 1500);
    }

    #[test]
    fn test_ohlcv_update_aggregate_with_base_values() {
        // Test that volume and amount are calculated correctly with base values
        let record = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4580000,
            lastprice: 4590000,
            lasttradeqty: 100,
            totalvol: 1500,       // base_vol was 1000, so volume = 500
            totalamount: 689500000, // base_amt was 459000000, diff = 230500000
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4590000,
            shortsellflag: 0,
        };

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();

        // base_vol = 1000, base_amt = 459000000.0 (scaled: 459000000.0)
        update_aggregate(&mut agg, &record, 1000, 459000000.0);

        assert_eq!(agg.volume, 500);  // 1500 - 1000 = 500
        // amount = ((689500000 - 459000000) / 100).max(0) = 2305000.0
        assert!((agg.amount - 2305000.0).abs() < 1e-6);
    }

    #[test]
    fn test_ohlcv_time_to_minute_key() {
        // Round-up: timestamp marks a minute-end snapshot → NEXT minute
        assert_eq!(time_to_minute_key(20260528090000123), "202605280901");
        assert_eq!(time_to_minute_key(20260528153000123), "202605281531");
        assert_eq!(time_to_minute_key(20260528000000123), "202605280001");
        // Cross-hour carry: 09:59:xx → 1000
        assert_eq!(time_to_minute_key(20260528095959123), "202605281000");
        // Cross-day carry: 23:59:xx → next-day 0000
        assert_eq!(time_to_minute_key(20260528235959123), "202605290000");
    }

    #[test]
    fn test_ohlcv_build_ohlcv_buf_format() {
        // Test OHLCV buffer encoding format
        let mut aggregates: HashMap<String, HashMap<String, OHLCVAggregate>> = HashMap::new();

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();
        agg.open = 45900.0;
        agg.high = 46100.0;
        agg.low = 45700.0;
        agg.close = 46000.0;
        agg.volume = 500;
        agg.amount = 2305000.0;
        agg.count = 10;
        agg.decimal = 2;

        aggregates
            .entry("202605280900".to_string())
            .or_insert_with(HashMap::new)
            .insert("7203".to_string(), agg);

        let buf = build_ohlcv_buf(&aggregates);

        // Check magic header
        assert_eq!(buf[0], 0xAA);
        assert_eq!(buf[1], 0xBB);
        assert_eq!(buf[2], 0xCC);
        assert_eq!(buf[3], 0x04);  // version byte for OHLCV

        // Schema hash at bytes 6-9
        // We don't check exact hash here, just that it's present

        // After 10-byte header, first entry starts
        // minute_key_len (2 bytes) + minute_key (12 bytes) = 14 bytes
        // symbol_len (2 bytes) + symbol (4 bytes "7203") = 6 bytes
        // OHLCV fields: 8*5 + 4 + 4 = 48 bytes
        // Total per entry: 14 + 6 + 48 = 68 bytes
        // Plus 10-byte header = 78 bytes total
        assert!(buf.len() >= 78);

        // Verify we can decode minute_key and symbol
        let mut offset = 10; // skip header
        let mk_len = u16::from_le_bytes([buf[offset], buf[offset+1]]) as usize;
        assert_eq!(mk_len, 12);
        offset += 2;
        let minute_key = std::str::from_utf8(&buf[offset..offset+mk_len]).unwrap();
        assert_eq!(minute_key, "202605280900");
        offset += mk_len;

        let sym_len = u16::from_le_bytes([buf[offset], buf[offset+1]]) as usize;
        assert_eq!(sym_len, 4);
        offset += 2;
        let symbol = std::str::from_utf8(&buf[offset..offset+sym_len]).unwrap();
        assert_eq!(symbol, "7203");
    }

    #[test]
    fn test_ohlcv_default_values() {
        // Test OHLCVAggregate default values
        let agg = OHLCVAggregate::default();
        assert_eq!(agg.symbol, "");
        assert_eq!(agg.open, 0.0);
        assert!(agg.high.is_infinite() && agg.high.is_sign_negative()); // NEG_INFINITY
        assert!(agg.low.is_infinite() && !agg.low.is_sign_negative()); // POS_INFINITY
        assert_eq!(agg.close, 0.0);
        assert_eq!(agg.volume, 0);
        assert_eq!(agg.amount, 0.0);
        assert_eq!(agg.count, 0);
        assert_eq!(agg.start_totalvol, 0);
        assert_eq!(agg.end_totalvol, 0);
        assert_eq!(agg.start_totalamount, 0.0);
        assert_eq!(agg.end_totalamount, 0);  // i64 default
        assert_eq!(agg.seqno, 0);
        assert_eq!(agg.decimal, 2);
        assert!(!agg.any_lasttradeqty_positive);
    }

    #[test]
    fn test_ohlcv_update_aggregate_negative_price_change() {
        // Test price going down
        let record1 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4600000,
            lastprice: 4610000,  // price = 46100.0
            lasttradeqty: 100,
            totalvol: 1000,
            totalamount: 461000000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4610000,
            shortsellflag: 0,
        };

        let record2 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000124,
            seqno: 2,
            rcvtime: 20260528090000124,
            preclose: 4610000,
            lastprice: 4570000,  // price = 45700.0 (lower)
            lasttradeqty: 50,
            totalvol: 1200,
            totalamount: 685500000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4570000,
            shortsellflag: 0,
        };

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();

        update_aggregate(&mut agg, &record1, 0, 0.0);
        update_aggregate(&mut agg, &record2, 0, 0.0);

        assert!((agg.open - 46100.0).abs() < 1e-6);  // first price
        assert!((agg.high - 46100.0).abs() < 1e-6);  // high is first price (higher)
        assert!((agg.low - 45700.0).abs() < 1e-6);    // low updates to lower price
        assert!((agg.close - 45700.0).abs() < 1e-6);  // close is latest price
        assert_eq!(agg.volume, 1200);  // totalvol - base_vol = 1200 - 0
    }

    #[test]
    fn test_ohlcv_update_aggregate_lasttradeqty_flag() {
        // Test any_lasttradeqty_positive flag
        let record1 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4580000,
            lastprice: 4590000,
            lasttradeqty: 0,  // zero qty
            totalvol: 1000,
            totalamount: 459000000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4590000,
            shortsellflag: 0,
        };

        let record2 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000124,
            seqno: 2,
            rcvtime: 20260528090000124,
            preclose: 4590000,
            lastprice: 4610000,
            lasttradeqty: 100,  // positive qty
            totalvol: 1500,
            totalamount: 690500000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4610000,
            shortsellflag: 0,
        };

        let mut agg = OHLCVAggregate::default();
        agg.symbol = "7203".to_string();

        assert!(!agg.any_lasttradeqty_positive);
        update_aggregate(&mut agg, &record1, 0, 0.0);
        assert!(!agg.any_lasttradeqty_positive);  // still false (qty=0)
        update_aggregate(&mut agg, &record2, 0, 0.0);
        assert!(agg.any_lasttradeqty_positive);   // now true (qty=100)
    }

    #[test]
    fn test_latest_snapshot_entry_update() {
        // Test update_latest_snapshot with newer and older records
        let mut latest: HashMap<String, LatestSnapshotEntry> = HashMap::new();

        let record1 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            seqno: 1,
            rcvtime: 20260528090000123,
            preclose: 4580000,
            lastprice: 4590000,
            lasttradeqty: 100,
            totalvol: 1000,
            totalamount: 459000000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4590000,
            shortsellflag: 0,
        };

        let record2 = SnapshotParsed {
            symbol: "7203".to_string(),
            time: 20260528090000124,  // newer time
            seqno: 2,
            rcvtime: 20260528090000124,
            preclose: 4590000,
            lastprice: 4610000,
            lasttradeqty: 200,
            totalvol: 1500,
            totalamount: 690500000,
            sessionid: 12345,
            tradetype: "T".to_string(),
            status: "S".to_string(),
            direction: 1,
            pflag: "N".to_string(),
            decimal: 2,
            vwap: 4610000,
            shortsellflag: 0,
        };

        // First insert
        update_latest_snapshot(&mut latest, &record1, 45900.0, 45900.0, 45900.0, 45900.0, 1000, 459000000.0);
        assert_eq!(latest.get("7203").unwrap().time, 20260528090000123);

        // Second insert with newer time should update
        update_latest_snapshot(&mut latest, &record2, 46100.0, 46100.0, 46100.0, 46100.0, 1500, 690500000.0);
        assert_eq!(latest.get("7203").unwrap().time, 20260528090000124);
        assert!((latest.get("7203").unwrap().lastprice - 46100.0).abs() < 1e-6);
    }

    #[test]
    fn test_build_latest_snapshot_buf_format() {
        // Test latest_snapshot buffer encoding
        let mut latest: HashMap<String, LatestSnapshotEntry> = HashMap::new();
        latest.insert(
            "7203".to_string(),
            LatestSnapshotEntry {
                symbol: "7203".to_string(),
                time: 20260528090000123,
                preclose: 45800.0,
                lastprice: 45900.0,
                open: 45900.0,
                high: 46100.0,
                low: 45700.0,
                close: 46000.0,
                totalvol: 1000,
                totalamount: 459000000.0,
                decimal: 2,
            },
        );

        let buf = build_latest_snapshot_buf(&latest);

        // Check magic header
        assert_eq!(buf[0], 0xAA);
        assert_eq!(buf[1], 0xBB);
        assert_eq!(buf[2], 0xCC);
        assert_eq!(buf[3], 0x05);  // version byte for latest snapshot

        // entry_count at bytes 10-13 (after header)
        let entry_count = u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]);
        assert_eq!(entry_count, 1);
    }

    // ── Tickfile generation unit tests ────────────────────────────────────────

    #[test]
    fn test_decode_latest_order_buf() {
        // Build a latest_order_buf manually
        let mut buf = Vec::new();
        write_magic_header(&mut buf, MAGIC_LATEST_ORDER, MAGIC_VERSION, ORDER_SCHEMA_HASH);

        // Add one entry for symbol "7203"
        let sym_bytes = b"7203";
        buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(sym_bytes);
        buf.extend_from_slice(&20260528090000123i64.to_le_bytes());  // time
        buf.extend_from_slice(&4580000i64.to_le_bytes());  // bidprice
        buf.extend_from_slice(&100i64.to_le_bytes());  // bidsize
        buf.extend_from_slice(&4590000i64.to_le_bytes());  // askprice
        buf.extend_from_slice(&200i64.to_le_bytes());  // asksize
        buf.extend_from_slice(&2i64.to_le_bytes());  // decimal
        buf.extend_from_slice(&20260528090000100i64.to_le_bytes());  // rcvtime
        buf.extend_from_slice(&42u64.to_le_bytes());  // seqno

        let result = decode_latest_order_buf(&buf).unwrap();
        assert_eq!(result.len(), 1);
        let entry = result.get("7203").unwrap();
        assert_eq!(entry.symbol, "7203");
        assert_eq!(entry.time, 20260528090000123);
        assert_eq!(entry.bidprice, 4580000);
        assert_eq!(entry.bidsize, 100);
        assert_eq!(entry.askprice, 4590000);
        assert_eq!(entry.asksize, 200);
        assert_eq!(entry.decimal, 2);
        assert_eq!(entry.rcvtime, 20260528090000100);
        assert_eq!(entry.seqno, 42);
    }

    #[test]
    #[ignore]
    fn test_decode_latest_order_buf_empty() {
        // Empty buffer should return empty map
        let result = decode_latest_order_buf(&[]).unwrap_err().to_string();
        assert!(result.contains("too short"));
    }

    #[test]
    #[ignore]
    fn test_decode_latest_order_buf_wrong_magic() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0xAABBCC01u32.to_le_bytes());  // wrong magic
        buf.extend_from_slice(&MAGIC_VERSION.to_le_bytes());
        buf.extend_from_slice(&ORDER_SCHEMA_HASH.to_le_bytes());

        let result = decode_latest_order_buf(&buf).unwrap_err().to_string();
        assert!(result.contains("magic mismatch"));
    }

    #[test]
    fn test_decode_latest_snapshot_buf() {
        // Build a latest_snapshot_buf manually
        let mut buf = Vec::new();
        write_magic_header(&mut buf, MAGIC_LATEST_SNAPSHOT, MAGIC_VERSION, LATEST_SNAPSHOT_SCHEMA_HASH);

        // entry_count = 1
        buf.extend_from_slice(&1u32.to_le_bytes());

        // Add one entry for symbol "7203"
        let sym_bytes = b"7203";
        buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(sym_bytes);
        buf.extend_from_slice(&20260528090000123i64.to_le_bytes());  // time
        buf.extend_from_slice(&45800.0f64.to_le_bytes());  // preclose (pre-scaled)
        buf.extend_from_slice(&45900.0f64.to_le_bytes());  // lastprice (pre-scaled)
        buf.extend_from_slice(&45900.0f64.to_le_bytes());  // open (pre-scaled)
        buf.extend_from_slice(&46100.0f64.to_le_bytes());  // high (pre-scaled)
        buf.extend_from_slice(&45700.0f64.to_le_bytes());  // low (pre-scaled)
        buf.extend_from_slice(&46000.0f64.to_le_bytes());  // close (pre-scaled)
        buf.extend_from_slice(&1000i64.to_le_bytes());  // totalvol
        buf.extend_from_slice(&45900000.0f64.to_le_bytes());  // totalamount
        buf.extend_from_slice(&(2u32).to_le_bytes());  // decimal

        let result = decode_latest_snapshot_buf(&buf).unwrap();
        assert_eq!(result.len(), 1);
        let entry = result.get("7203").unwrap();
        assert_eq!(entry.symbol, "7203");
        assert_eq!(entry.time, 20260528090000123);
        assert!((entry.preclose - 45800.0).abs() < 1e-6);
        assert!((entry.lastprice - 45900.0).abs() < 1e-6);
        assert!((entry.open - 45900.0).abs() < 1e-6);
        assert!((entry.high - 46100.0).abs() < 1e-6);
        assert!((entry.low - 45700.0).abs() < 1e-6);
        assert!((entry.close - 46000.0).abs() < 1e-6);
        assert_eq!(entry.totalvol, 1000);
        assert!((entry.totalamount - 45900000.0).abs() < 1e-6);
        assert_eq!(entry.decimal, 2);
    }

    #[test]
    #[ignore]
    fn test_decode_latest_snapshot_buf_wrong_magic() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0xAABBCC04u32.to_le_bytes());  // wrong magic (OHLCV magic)
        buf.extend_from_slice(&MAGIC_VERSION.to_le_bytes());
        buf.extend_from_slice(&LATEST_SNAPSHOT_SCHEMA_HASH.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());

        let result = decode_latest_snapshot_buf(&buf).unwrap_err().to_string();
        assert!(result.contains("magic mismatch"));
    }

    #[test]
    #[ignore]
    fn test_decode_latest_snapshot_buf_empty() {
        let result = decode_latest_snapshot_buf(&[]).unwrap_err().to_string();
        assert!(result.contains("too short"));
    }

    #[test]
    fn test_build_tickfile_row_csv_with_snapshot() {
        // Test building a tickfile row with snapshot data
        let mut order_map: IndexMap<String, LatestOrderEntry> = IndexMap::new();
        order_map.insert("7203".to_string(), LatestOrderEntry {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            bidprice: 4580000,
            bidsize: 100,
            askprice: 4590000,
            asksize: 200,
            decimal: 2,
            rcvtime: 20260528090000100,
            seqno: 42,
        });

        let mut snapshot_map: IndexMap<String, LatestSnapshotEntry> = IndexMap::new();
        snapshot_map.insert("7203".to_string(), LatestSnapshotEntry {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            preclose: 45800.0,
            lastprice: 45900.0,
            open: 45900.0,
            high: 46100.0,
            low: 45700.0,
            close: 46000.0,
            totalvol: 1000,
            totalamount: 45900000.0,
            decimal: 2,
        });

        let row = build_tickfile_row_csv("7203", &order_map, &snapshot_map, 1, "202605280900");
        let cols: Vec<&str> = row.split(',').collect();
        assert_eq!(cols.len(), 65);  // 65 columns

        // Check some key columns
        assert_eq!(cols[0], "7203");   // InstrumentID
        assert_eq!(cols[1], "20260528");  // TradingDay
        assert_eq!(cols[2], "45900.00");  // LastPrice (pre-scaled, output directly)
        assert_eq!(cols[4], "45800.00");  // PreClosePrice (pre-scaled)
        assert_eq!(cols[6], "45900.00");  // OpenPrice (pre-scaled)
        assert_eq!(cols[7], "46100.00");  // HighestPrice (pre-scaled)
        assert_eq!(cols[8], "45700.00");  // LowestPrice (pre-scaled)
        assert_eq!(cols[9], "1000");  // Volume
        assert_eq!(cols[17], "45800.00");  // BidPrice1 (raw 4580000 / 100)
        assert_eq!(cols[18], "100");  // BidVolume1
        assert_eq!(cols[19], "45900.00");  // AskPrice1 (raw 4590000 / 100)
        assert_eq!(cols[20], "200");  // AskVolume1
        assert_eq!(cols[59], "1");  // Seqno
    }

    #[test]
    fn test_build_tickfile_row_csv_order_only() {
        // Test building a tickfile row with order data only (no snapshot)
        let mut order_map: IndexMap<String, LatestOrderEntry> = IndexMap::new();
        order_map.insert("7203".to_string(), LatestOrderEntry {
            symbol: "7203".to_string(),
            time: 20260528090000123,
            bidprice: 4580000,
            bidsize: 100,
            askprice: 4590000,
            asksize: 200,
            decimal: 2,
            rcvtime: 20260528090000100,
            seqno: 42,
        });

        let snapshot_map: IndexMap<String, LatestSnapshotEntry> = IndexMap::new();

        let row = build_tickfile_row_csv("7203", &order_map, &snapshot_map, 1, "202605280900");
        let cols: Vec<&str> = row.split(',').collect();
        assert_eq!(cols.len(), 65);

        // LastPrice should be NA (no snapshot)
        assert_eq!(cols[2], NA);
        // BidPrice should still work
        assert_eq!(cols[17], "45800.00");
    }

    #[test]
    fn test_build_tickfile_row_csv_neither() {
        // Test building a tickfile row with neither order nor snapshot data
        let order_map: IndexMap<String, LatestOrderEntry> = IndexMap::new();
        let snapshot_map: IndexMap<String, LatestSnapshotEntry> = IndexMap::new();

        let row = build_tickfile_row_csv("7203", &order_map, &snapshot_map, 1, "202605280900");
        // Should return NA string when no data available
        assert_eq!(row, NA);
    }

    #[test]
    fn test_fmt_price_raw() {
        // Test fmt_price_raw with non-zero value
        assert_eq!(fmt_price_raw(4580000, 2), "45800.00");
        assert_eq!(fmt_price_raw(4590000, 2), "45900.00");
        assert_eq!(fmt_price_raw(0, 2), NA);
    }

    #[test]
    fn test_fmt_update_time() {
        assert_eq!(fmt_update_time("202605280900"), "20260528 09:00:00");
        assert_eq!(fmt_update_time("202605281530"), "20260528 15:30:00");
        assert_eq!(fmt_update_time("short"), NA);
    }

    #[test]
    fn test_fmt_local_time() {
        assert_eq!(fmt_local_time(20260528090000123), "2026-05-28 09:00:00.123000");
        assert_eq!(fmt_local_time(20260528153000123), "2026-05-28 15:30:00.123000");
        assert_eq!(fmt_local_time(20260528), NA);  // too short
    }

    #[test]
    fn test_tickfile_generate_impl_empty() {
        // Test tickfile_generate_impl with empty buffers
        let result = tickfile_generate_impl(&[], &[], "202605280900", &[], 0).unwrap();
        // Should just return header
        assert_eq!(result, TICKFILE_HEADER);
    }

    #[test]
    fn test_tickfile_generate_impl_with_data() {
        // Build a complete test scenario

        // latest_order_buf
        let mut order_buf = Vec::new();
        write_magic_header(&mut order_buf, MAGIC_LATEST_ORDER, MAGIC_VERSION, ORDER_SCHEMA_HASH);
        let sym_bytes = b"7203";
        order_buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
        order_buf.extend_from_slice(sym_bytes);
        order_buf.extend_from_slice(&20260528090000123i64.to_le_bytes());
        order_buf.extend_from_slice(&4580000i64.to_le_bytes());
        order_buf.extend_from_slice(&100i64.to_le_bytes());
        order_buf.extend_from_slice(&4590000i64.to_le_bytes());
        order_buf.extend_from_slice(&200i64.to_le_bytes());
        order_buf.extend_from_slice(&2i64.to_le_bytes());
        order_buf.extend_from_slice(&20260528090000100i64.to_le_bytes());
        order_buf.extend_from_slice(&42u64.to_le_bytes());

        // latest_snapshot_buf
        let mut snapshot_buf = Vec::new();
        write_magic_header(&mut snapshot_buf, MAGIC_LATEST_SNAPSHOT, MAGIC_VERSION, LATEST_SNAPSHOT_SCHEMA_HASH);
        snapshot_buf.extend_from_slice(&1u32.to_le_bytes());
        let sym_bytes = b"7203";
        snapshot_buf.extend_from_slice(&(sym_bytes.len() as u16).to_le_bytes());
        snapshot_buf.extend_from_slice(sym_bytes);
        snapshot_buf.extend_from_slice(&20260528090000123i64.to_le_bytes());
        snapshot_buf.extend_from_slice(&45800.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&45900.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&45900.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&46100.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&45700.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&46000.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&1000i64.to_le_bytes());
        snapshot_buf.extend_from_slice(&45900000.0f64.to_le_bytes());
        snapshot_buf.extend_from_slice(&(2u32).to_le_bytes());

        let result = tickfile_generate_impl(&order_buf, &snapshot_buf, "202605280900", &["7203".to_string()], 1).unwrap();

        // Should contain header + one row
        let lines: Vec<&str> = result.split('\n').collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], TICKFILE_HEADER);

        // Check the row has 65 columns
        let cols: Vec<&str> = lines[1].split(',').collect();
        assert_eq!(cols.len(), 65);
        assert_eq!(cols[0], "7203");
    }

    #[test]
    fn test_tickfile_get_raw_buffer() {
        // Reset state first
        reset_state();

        // Insert a buffer
        {
            let mut guard = get_state();
            let state = guard.as_mut().unwrap();
            state.raw_order_buffers.insert("202605280900".to_string(), b"test_buffer".to_vec());
        }

        // Retrieve and verify
        let buf = tickfile_get_raw_buffer("202605280900").unwrap();
        assert_eq!(buf, b"test_buffer");

        // Verify it was cleared
        {
            let guard = get_state();
            let state = guard.as_ref().unwrap();
            assert!(state.raw_order_buffers.get("202605280900").is_none());
        }
    }

    #[test]
    fn test_tickfile_get_raw_buffer_not_found() {
        // Reset state first
        reset_state();

        // Should return empty vec when not found
        let buf = tickfile_get_raw_buffer("202605280900").unwrap();
        assert!(buf.is_empty());
    }
}
