# Minute-Key Round-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change minute_key derivation from round-down (`str(time)[:12]`) to round-up (floor+1) for both order and snapshot paths, so a timestamp `09:00:01.000` (a minute-end snapshot) belongs to minute `0901`.

**Architecture:** Change the canonical `time_to_minute_key` (Python `clock.py` + Rust `lib.rs`) to floor+1, then route ALL 5 separate inline derivations through it (they don't currently call the fn). Shift `minute_key_to_end_time` (→ M's own moment) and `minute_key_to_start_time` (→ M−1min) so bar M covers `[(M−1):00, M:00)` and `is_expired` flush timing stays identical. Update golden tests (+1 literal shifts) and rebuild Rust.

**Tech Stack:** Python 3.12, Rust/PyO3 (no chrono — carry is hand-rolled), pytest, `cargo test`.

**Spec:** `docs/superpowers/specs/2026-06-16-minute-key-round-up-design.md`

---

## CRITICAL: Atomic Semantic Unit

**Tasks 1–5 are ONE atomic semantic change.** Do NOT merge/PR between them — `is_expired` flush timing breaks if `time_to_minute_key(+1)` and `end_time/start_time(−1min)` land separately (spec §5.3, gotcha #6). The branch is red (tests failing) until Task 7 completes the golden updates. Only the final full-regression-green commit is mergeable.

## File Structure

**Modified (source — the semantic change):**
- `src/minute_bar/clock.py` — `time_to_minute_key`, `minute_key_to_end_time`, `minute_key_to_start_time`
- `src/minute_bar/engine.py` — 4 inline derivations routed through the fn (lines 859, 962, 1031, 1050)
- `order_accel/src/lib.rs` — `time_to_minute_key` fn body; order-path inline (line 861) routed through fn

**Modified (tests — golden shifts):**
- `tests/test_clock.py`, `tests/test_order_accel.py`, `tests/test_order_batch_golden.py`, `tests/test_snapshot_ohlcv_golden.py`, `tests/test_phase21_parity_parallel.py`, `tests/test_aggregator.py`, `tests/test_order_late_batch_write.py`
- `order_accel/src/lib.rs` Rust unit tests (lines 2236, 2303-2310, 2778-2780)

**DO NOT TOUCH (date-only slices, drive cross-day detection):** `lib.rs:855` (`&time_str[..8]`), `engine.py:757` (`str(record.time)[:8]`), `clock.py:34`, `flusher.py:173`, and all `[:8]` slices.

---

## Task 1: Python `time_to_minute_key` → floor+1 (TDD)

**Files:**
- Modify: `src/minute_bar/clock.py:49-51`
- Test: `tests/test_clock.py` (add a new test class)

- [ ] **Step 1: Write failing tests** — append to `tests/test_clock.py` (after the existing imports at top, `time_to_minute_key` is NOT yet imported there — add it to the import block):

```python
# Add time_to_minute_key to the existing import from minute_bar.clock (test_clock.py:5-9):
from minute_bar.clock import (
    JST,
    is_data_driven_expired,
    minute_key_to_start_time,
    time_to_minute_key,
)


class TestTimeToMinuteKeyRoundUp:
    """Round-up: timestamp marks a minute-end snapshot → belongs to NEXT minute."""

    def test_second_after_minute_start(self):
        # 09:00:01.000 → 0901
        assert time_to_minute_key(20260528090001000) == "202605280901"

    def test_just_before_minute_end(self):
        # 09:00:59.000 → 0901 (still 0900 clock-minute, +1 → 0901)
        assert time_to_minute_key(20260528090059000) == "202605280901"

    def test_exact_minute_boundary_also_plus_one(self):
        # 09:01:00.000 (exact on-minute) → 0902 (strict floor+1, no special-case)
        assert time_to_minute_key(20260528090100000) == "202605280902"

    def test_cross_hour_carry(self):
        # 09:59:01.000 → 1000 (mm=59+1=60 → hh+1, mm=0)
        assert time_to_minute_key(20260528095901000) == "202605281000"

    def test_last_trading_minute_spillover_allowed(self):
        # 15:30:01.000 → 1531 (allowed, not capped)
        assert time_to_minute_key(20260528153001000) == "202605281531"

    def test_cross_day_carry_defensive(self):
        # 23:59:01.000 → next-day 0000 (hh=24 → date+1, hh=0)
        assert time_to_minute_key(20260528235901000) == "202605290000"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_clock.py::TestTimeToMinuteKeyRoundUp -v`
Expected: 6 FAIL — `time_to_minute_key` returns round-down values (e.g. `"202605280900"` ≠ `"202605280901"`).

- [ ] **Step 3: Implement floor+1** — replace `src/minute_bar/clock.py:49-51`:

Old (verbatim):
```python
def time_to_minute_key(time_17digit: int) -> str:
    s = str(time_17digit)
    return s[:12]
```

New:
```python
def time_to_minute_key(time_17digit: int) -> str:
    """Round-UP: a timestamp marks a minute-end snapshot, so it belongs to the NEXT minute.

    09:00:01.000 → '0901' | 09:59:xx → '1000' | 23:59:xx → next-day '0000'.
    See spec 2026-06-16-minute-key-round-up-design.
    """
    s = str(time_17digit)
    if len(s) < 12:
        return s[:12]  # malformed input — preserve prior best-effort behavior
    base = datetime.strptime(s[:12], "%Y%m%d%H%M")
    return (base + timedelta(minutes=1)).strftime("%Y%m%d%H%M")
```

(Confirm `datetime` and `timedelta` are imported at the top of `clock.py` — they are, used by `minute_key_to_end_time`/`start_time`. If not, add `from datetime import datetime, timedelta`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_clock.py::TestTimeToMinuteKeyRoundUp -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/clock.py tests/test_clock.py
git commit -m "feat(clock): time_to_minute_key round-up (floor+1) with carry"
```

---

## Task 2: Python `end_time`/`start_time` shift (TDD)

**Files:**
- Modify: `src/minute_bar/clock.py:41-46` (`minute_key_to_end_time`), `src/minute_bar/clock.py:70-74` (`minute_key_to_start_time`)
- Modify: `tests/test_clock.py:12-50` (`TestMinuteKeyToStartTime`)

- [ ] **Step 1: Update `TestMinuteKeyToStartTime` to the new semantics** — replace `tests/test_clock.py:12-50` (the whole `TestMinuteKeyToStartTime` class):

Old class (verbatim, lines 12-50):
```python
class TestMinuteKeyToStartTime:
    def test_valid_key(self):
        dt = minute_key_to_start_time("202605210900")
        assert dt.hour == 9
        assert dt.minute == 0
        assert dt.tzinfo == JST

    def test_midnight(self):
        dt = minute_key_to_start_time("202605210000")
        assert dt.hour == 0
        assert dt.minute == 0

    def test_end_of_day(self):
        dt = minute_key_to_start_time("202605202359")
        assert dt.hour == 23
        assert dt.minute == 59

    def test_invalid_length(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090")

    def test_invalid_non_digit(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090X")

    def test_invalid_hour(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605212500")

    def test_invalid_minute(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605210961")

    def test_cross_day_boundary(self):
        dt = minute_key_to_start_time("202605202359")
        next_min = dt + timedelta(minutes=1)
        assert next_min.day == 21
        assert next_min.hour == 0
```

New class (start_time now returns M−1min):
```python
class TestMinuteKeyToStartTime:
    """Round-up: bar M covers [(M-1):00, M:00), so start_time(M) = M-1 minute."""

    def test_valid_key(self):
        # bar 0900 starts at 08:59
        dt = minute_key_to_start_time("202605210900")
        assert dt.hour == 8
        assert dt.minute == 59
        assert dt.tzinfo == JST

    def test_midnight(self):
        # bar 0000 starts at previous-day 23:59
        dt = minute_key_to_start_time("202605210000")
        assert dt.hour == 23
        assert dt.minute == 59

    def test_end_of_day(self):
        # bar 2359 starts at 23:58
        dt = minute_key_to_start_time("202605202359")
        assert dt.hour == 23
        assert dt.minute == 58

    def test_invalid_length(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090")

    def test_invalid_non_digit(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090X")

    def test_invalid_hour(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605212500")

    def test_invalid_minute(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605210961")

    def test_cross_day_boundary(self):
        # bar 0000 starts at prev-day 23:59; +1min crosses into day 21 00:00
        dt = minute_key_to_start_time("202605210000")
        next_min = dt + timedelta(minutes=1)
        assert next_min.day == 21
        assert next_min.hour == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_clock.py::TestMinuteKeyToStartTime -v`
Expected: `test_valid_key`, `test_midnight`, `test_end_of_day`, `test_cross_day_boundary` FAIL (start_time still returns M, not M−1min). The 4 `test_invalid_*` still PASS (strptime validation unchanged).

- [ ] **Step 3: Implement the two shifts**

3a. `minute_key_to_end_time` — `src/minute_bar/clock.py:41-46`. Old (verbatim):
```python
def minute_key_to_end_time(minute_key: str) -> datetime:
    yyyymmdd = minute_key[:8]
    hhmm = minute_key[8:12]
    hh, mm = int(hhmm[:2]), int(hhmm[2:])
    date_part = datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=JST)
    return date_part.replace(hour=hh, minute=mm) + timedelta(minutes=1)
```
New (return M's own moment — bar M's end/cutoff is M:00):
```python
def minute_key_to_end_time(minute_key: str) -> datetime:
    """Round-up: bar M covers [(M-1):00, M:00); the end/cutoff is M's own moment."""
    yyyymmdd = minute_key[:8]
    hhmm = minute_key[8:12]
    hh, mm = int(hhmm[:2]), int(hhmm[2:])
    date_part = datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=JST)
    return date_part.replace(hour=hh, minute=mm)
```

3b. `minute_key_to_start_time` — `src/minute_bar/clock.py:70-74`. Old (verbatim):
```python
def minute_key_to_start_time(minute_key: str) -> datetime:
    """Convert minute_key "YYYYMMDDHHMM" to the start datetime of that minute."""
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST)
```
New (return M−1min — bar M's start is (M−1):00; preserve validation):
```python
def minute_key_to_start_time(minute_key: str) -> datetime:
    """Round-up: bar M covers [(M-1):00, M:00); the start is M-1 minute."""
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST) - timedelta(minutes=1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_clock.py -v`
Expected: ALL PASS (incl. `TestIsDataDrivenExpired` — relative comparisons unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/clock.py tests/test_clock.py
git commit -m "feat(clock): end_time→M, start_time→M-1min for round-up interval"
```

---

## Task 3: Route 4 Python inline derivations through the fn

**Files:** Modify `src/minute_bar/engine.py` at lines 859, 962, 1031, 1050

These derive minute_key WITHOUT calling `time_to_minute_key`. All must use it so they pick up floor+1. (Line 1072 already calls it — no change.)

- [ ] **Step 1: engine.py:859** (inside `_write_late_orders_batch`). Old (verbatim):
```python
        for rec in decoded_late:
            mk = str(rec.time)[:12]
```
New:
```python
        for rec in decoded_late:
            mk = time_to_minute_key(rec.time)
```

- [ ] **Step 2: engine.py:962** (Phase 21 per_minute_buf re-bucketing). Old (verbatim):
```python
                                    for rec in records:
                                        minute_key_for_record = str(rec.time)[:12]
```
New:
```python
                                    for rec in records:
                                        minute_key_for_record = time_to_minute_key(rec.time)
```

- [ ] **Step 3: engine.py:1030-1031** (Rust-accel order loop). Old (verbatim):
```python
                                        # time_to_minute_key: integer division + str() for efficiency
                                        minute_key = str(record.time // 100_000)
```
New:
```python
                                        # minute_key via shared round-up derivation
                                        minute_key = time_to_minute_key(record.time)
```

- [ ] **Step 4: engine.py:1050** (Python per-line fallback branch). This `minute_key = str(record.time // 100_000)` is distinguished from Step 3's by its FOLLOWING comment (`# Feed to SAME shared processing function`). Target the assignment + that comment together. Old (verbatim):
```python
                                        minute_key = str(record.time // 100_000)
                                        # Feed to SAME shared processing function
```
New:
```python
                                        minute_key = time_to_minute_key(record.time)
                                        # Feed to SAME shared processing function
```
(The `record_date = str(record.time)[:8]` line ABOVE this block stays unchanged — it's a date slice.)

- [ ] **Step 5: Verify `time_to_minute_key` is imported in engine.py**

Run: `grep -n "time_to_minute_key" src/minute_bar/engine.py | head -3`
Expected: the import line (near `from minute_bar.clock import ...`) is present. If NOT imported, add `time_to_minute_key` to the existing `from minute_bar.clock import (...)` block at the top of engine.py.

- [ ] **Step 6: Commit**

```bash
git add src/minute_bar/engine.py
git commit -m "refactor(engine): route 4 inline minute_key derivations through time_to_minute_key"
```

---

## Task 4: Rust `time_to_minute_key` → floor+1 + update Rust unit tests (TDD)

**Files:** Modify `order_accel/src/lib.rs:1275-1279` (fn) + `2775-2781` (test)

- [ ] **Step 1: Update the Rust unit test** — `order_accel/src/lib.rs:2775-2781`. Old (verbatim):
```rust
    #[test]
    fn test_ohlcv_time_to_minute_key() {
        // Test minute_key extraction from 17-digit timestamp
        assert_eq!(time_to_minute_key(20260528090000123), "202605280900");
        assert_eq!(time_to_minute_key(20260528153000123), "202605281530");
        assert_eq!(time_to_minute_key(20260528000000123), "202605280000");
    }
```
New (floor+1 expectations + cross-day case):
```rust
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd order_accel && cargo test test_ohlcv_time_to_minute_key --lib`
Expected: FAIL (fn still returns round-down `"202605280900"` ≠ `"202605280901"`).

- [ ] **Step 3: Implement floor+1** — replace `order_accel/src/lib.rs:1275-1279`. Old (verbatim):
```rust
/// Extract minute_key from a snapshot time (17-digit integer).
/// Python: str(time)[:12]
fn time_to_minute_key(time: i64) -> String {
    time.to_string()[..12].to_string()
}
```
New (hand-rolled carry — no chrono dependency):
```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd order_accel && cargo test test_ohlcv_time_to_minute_key --lib`
Expected: PASS.

- [ ] **Step 5: Update the OTHER two Rust minute_key tests**

5a. `lib.rs:2236` (inside `test_preprocess_line_valid`). Old:
```rust
        assert_eq!(r.minute_key, "202605280900");
```
New:
```rust
        assert_eq!(r.minute_key, "202605280901");
```

5b. `lib.rs:2303-2310` (`test_minute_key_string_slice`). Old:
```rust
    #[test]
    fn test_minute_key_string_slice() {
        // Verify minute_key uses string slice [:12], NOT integer division
        // time = 20260528090000123 → str[:12] = "202605280900"
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528").unwrap();
        assert_eq!(result.minute_key, "202605280900");
        // Make sure it doesn't use integer division (time // 100_000_000_000 = 20260 which is wrong)
    }
```
New:
```rust
    #[test]
    fn test_minute_key_round_up() {
        // Round-up: 09:00:00.123 (clock-minute 0900) → NEXT minute 0901
        let line = b"7203,20260528090000123,4580000,100,4590000,200,2,0";
        let result = preprocess_line(line, "utf-8", "20260528").unwrap();
        assert_eq!(result.minute_key, "202605280901");
    }
```

- [ ] **Step 6: Run full Rust test suite**

Run: `cd order_accel && cargo test --lib`
Expected: ALL PASS (75+ tests). Fix any other minute_key assertion that surfaces the same way (shift +1).

- [ ] **Step 7: Commit**

```bash
git add order_accel/src/lib.rs
git commit -m "feat(rust): time_to_minute_key round-up (floor+1) with hand-rolled carry"
```

---

## Task 5: Route Rust order-path inline through the fn

**Files:** Modify `order_accel/src/lib.rs:860-861` + `882`

The order hot path derives minute_key inline (`&time_str[..12]`), NOT via the fn — so Task 4's fn change does NOT reach order records. Route it through.

- [ ] **Step 1: lib.rs:860-861**. Old (verbatim):
```rust
    // minute_key: str(time)[:12] (12-char string slice)
    let minute_key = &time_str[..12];
```
New:
```rust
    // minute_key: round-up (floor+1) via shared fn — keeps order/snapshot parity
    let minute_key = time_to_minute_key(time_val);
```

- [ ] **Step 2: lib.rs:882** (struct field — `minute_key` is now owned `String`, drop the redundant `.to_string()`). Old (verbatim):
```rust
        minute_key: minute_key.to_string(),
```
New:
```rust
        minute_key,
```

- [ ] **Step 3: Verify `time_val` is in scope at line 861**

Run: `grep -n "let time_val\|time_str = time_val" order_accel/src/lib.rs | head`
Expected: `time_val` defined a few lines above (it backs `time_str = time_val.to_string()` at ~line 854). The fn takes `i64` — `time_val` is `i64`. ✓

- [ ] **Step 4: Build + test**

Run: `cd order_accel && cargo test --lib`
Expected: ALL PASS (the `test_minute_key_round_up` from Task 4 now exercises this path; `test_preprocess_line_valid` asserts `"202605280901"`).

- [ ] **Step 5: Commit**

```bash
git add order_accel/src/lib.rs
git commit -m "refactor(rust): route order-path minute_key through time_to_minute_key fn"
```

---

## Task 6: Route 4 test reference-helpers through the fn (auto-tracks parity)

**Files:** 4 test files — each has a `str(time)[:12]` reference-model derivation. Route through `time_to_minute_key` so the reference model uses floor+1 and matches production automatically.

- [ ] **Step 1: test_order_batch_golden.py:69**. Old:
```python
        minute_key = str(time_val)[:12]
```
New (ensure `time_to_minute_key` is imported from `minute_bar.clock` at top of file):
```python
        minute_key = time_to_minute_key(time_val)
```

- [ ] **Step 2: test_snapshot_ohlcv_golden.py:34**. Old:
```python
        minute_key = str(rec.time)[:12]
```
New:
```python
        minute_key = time_to_minute_key(rec.time)
```

- [ ] **Step 3: test_phase21_parity_parallel.py:63**. Old:
```python
            minute_key = str(time_val)[:12]
```
New:
```python
            minute_key = time_to_minute_key(time_val)
```

- [ ] **Step 4: test_order_late_batch_write.py:93**. Old:
```python
                by_minute.setdefault(str(r.time)[:12], []).append(r)
```
New:
```python
                by_minute.setdefault(time_to_minute_key(r.time), []).append(r)
```

- [ ] **Step 5: Ensure the import exists in each of the 4 test files**

For each file, run: `grep -n "time_to_minute_key" tests/test_order_batch_golden.py tests/test_snapshot_ohlcv_golden.py tests/test_phase21_parity_parallel.py tests/test_order_late_batch_write.py | grep import`
If missing in any, add `from minute_bar.clock import time_to_minute_key` to its imports.

- [ ] **Step 6: Commit**

```bash
git add tests/test_order_batch_golden.py tests/test_snapshot_ohlcv_golden.py tests/test_phase21_parity_parallel.py tests/test_order_late_batch_write.py
git commit -m "test: route reference-model minute_key helpers through time_to_minute_key"
```

---

## Task 7: Shift literal minute_key assertions +1 (golden tests)

The reference helpers (Task 6) now auto-track, but many tests still assert **hardcoded literal** minute_key strings. Each must shift +1. Run each file; pytest reports the exact failing line + expected/actual — apply +1.

**The universal rule:** every literal minute_key string in an assertion shifts +1 (e.g. `"202605280900"` → `"202605280901"`). **Late-routing tests:** shift BOTH the record's derived key AND the pre-populated `flushed_*_minutes` set label +1 together (they must stay consistent or the test's late-detection logic breaks).

- [ ] **Step 1: test_order_late_batch_write.py** (your own test — literals at :96-102, :144). Old:
```python
        assert len(by_minute["202605280900"]) == 3
        assert [r.time for r in by_minute["202605280900"]] == \
            [20260528090000100, 20260528090000200, 20260528090000300]
        # minute 0901 got exactly its 2 records
        assert len(by_minute["202605280901"]) == 2
        assert [r.time for r in by_minute["202605280901"]] == \
            [20260528090100100, 20260528090100200]
```
New:
```python
        assert len(by_minute["202605280901"]) == 3
        assert [r.time for r in by_minute["202605280901"]] == \
            [20260528090000100, 20260528090000200, 20260528090000300]
        # minute 0902 got exactly its 2 records (was 0901 input → floor+1 → 0902)
        assert len(by_minute["202605280902"]) == 2
        assert [r.time for r in by_minute["202605280902"]] == \
            [20260528090100100, 20260528090100200]
```
And `:144`. Old:
```python
        assert engine._late_order_minutes == {"202605280900", "202605280901"}
```
New:
```python
        assert engine._late_order_minutes == {"202605280901", "202605280902"}
```

- [ ] **Step 2: test_order_batch_golden.py** — run and fix each failing literal +1.

Run: `python -m pytest tests/test_order_batch_golden.py -v`
Apply +1 to each literal (pytest shows exact line). Known shifts: `test_valid_lines_single_minute` (~:379-380) `202605280900`→`202605280901`; `test_invalid_lines_skipped` (~:421-422) same; `test_cross_day_reset` (~:508) `202605290900`→`202605290901`. For `test_late_order_routing` (~:438,450,451,455,456): shift the flushed list label `202605280900`→`202605280901` AND the per_minute expectations `202605280900`→`202605280901`, `202605280901`→`202605280902` together (input times 0900.0123/0901.0123 now map to 0901/0902).

- [ ] **Step 3: test_snapshot_ohlcv_golden.py** — run and fix.

Run: `python -m pytest tests/test_snapshot_ohlcv_golden.py -v`
Known shifts: ~:195,200,201,233,256,327 `202605280900`→`202605280901`; ~:269 `202605280901`→`202605280902` (input 0901.0123). For `test_late_record_routing` (~:289,292,295): shift flushed list + assertions +1 together. `test_empty_batch` (~:305,308) does NOT shift (explicit label round-trip).

- [ ] **Step 4: test_phase21_parity_parallel.py** — run and fix.

Run: `python -m pytest tests/test_phase21_parity_parallel.py -v`
Known shifts: ~:148 expected_keys `{'202605280900','...901','...902'}`→`{'...901','...902','...903'}`; ~:171 flushed `['202605280900']`→`['202605280901']` (to remain a late-order test, the record's new derived key must be in the flushed set).

- [ ] **Step 5: test_aggregator.py** — run and fix.

Run: `python -m pytest tests/test_aggregator.py -v`
Known shifts (current_minute assertions): ~:194,196,201,203,205,211,228,232,233,241,248,261,262,263,272 — each derived minute_key +1 (e.g. 0900→0901). For `test_late_record_does_not_trigger_snapshot` (~:265-272): the `flushed_snapshot_minutes.add(...)` label AND the assertion both shift `0900`→`0901`.

- [ ] **Step 6: test_order_accel.py** — replace the obsolete integer-div equivalence test (lines 245-257).

Old (verbatim):
```python
def test_time_to_minute_key_integer_div():
    """Verify str(time // 100_000) produces same result as str(time)[:12] for 17-digit timestamps."""
    # This validates the optimization used in the Rust path
    test_times = [
        20260528090000123,
        20260528113000123,
        20260528150000123,
        20260528080000000,
    ]
    for t in test_times:
        str_method = str(t)[:12]
        int_method = str(t // 100_000)
        assert str_method == int_method, f"Mismatch for {t}: str[:12]={str_method}, //100_000={int_method}"
```
New (the round-down equivalence is obsolete after round-up; replace with a Python↔Rust round-up parity check):
```python
def test_time_to_minute_key_round_up_parity():
    """Python time_to_minute_key (floor+1) matches the expected round-up for trading-range times."""
    from minute_bar.clock import time_to_minute_key
    cases = {
        20260528090000123: "202605280901",   # 09:00:00 → 0901
        20260528090100123: "202605280902",   # 09:01:00 → 0902
        20260528095900123: "202605281000",   # 09:59 → 1000 (cross-hour)
        20260528153000123: "202605281531",   # 15:30 → 1531 (spillover)
    }
    for t, expected in cases.items():
        assert time_to_minute_key(t) == expected, f"{t} → {time_to_minute_key(t)}, expected {expected}"
```

- [ ] **Step 7: Commit all test shifts**

```bash
git add tests/
git commit -m "test: shift golden minute_key literals +1 for round-up semantics"
```

---

## Task 8: Full regression + atomicity verification

- [ ] **Step 1: Rebuild the Rust extension** (so Python loads the new `.pyd`).

Run: `pip install -e . --no-build-isolation` (or the project's build command). Confirm no errors.

- [ ] **Step 2: Run the full Python test suite**

Run: `python -m pytest tests/ -q`
Expected: ALL PASS. If any test still fails on a minute_key literal, it was missed in Task 7 — apply +1 and re-run. (Pre-existing failures unrelated to round-up — `test_phase21_parity_parallel::test_parity_single_batch`, `test_order_drain::test_no_data_uses_config_interval`, `test_e2e_tickfile_completeness` — were failing before this change; confirm they're the same ones, not new.)

- [ ] **Step 3: Run Rust suite**

Run: `cd order_accel && cargo test --lib`
Expected: ALL PASS.

- [ ] **Step 4: Atomicity sanity** — confirm `is_expired` timing is right by checking that for a sample minute_key, `end_time(M)` equals `start_time(M) + 1min` (bar width still 1 minute, just shifted).

Run: `python -c "from minute_bar.clock import minute_key_to_end_time, minute_key_to_start_time; from datetime import timedelta; m='202605280901'; print((minute_key_to_end_time(m) - minute_key_to_start_time(m)) == timedelta(minutes=1))"`
Expected: `True`.

- [ ] **Step 5: Commit (if any fixups)** — otherwise nothing to commit.

---

## Task 9: E2E verification (tickfile quality criteria #1/#2/#3)

Rebuild produces real round-up output; verify against the original quality criteria.

- [ ] **Step 1: Clean + run the full-day no-truncation diagnostic**

```bash
rm -rf test/phase21_benchmark/engine_out test/phase21_benchmark/sim_out
python test/phase21_benchmark/full_day_run.py
```
Expected: order reaches 1530 EOF, `Shutdown CHECK 3 PASS`, 0 EXTRA minutes.

- [ ] **Step 2: Verify criterion #3 (round-up boundary)** — a `09:00:01.000` record must land in minute **0901**.

Run:
```bash
TF=test/phase21_benchmark/engine_out/tickfile/2026/20260528/tickfile_20260528.csv
# find rows whose LocalTime is in clock-minute 0900 (09:00:xx) and check their tickfile minute column is 0901
```
Inspect: rows with time-of-day `09:00:xx` must be attributed to minute 0901 (the round-up label), NOT 0900. (Column index for minute: confirm via header; the tickfile row carries minute_key or a timestamp whose `[:12]` after round-up = 0901.)

- [ ] **Step 3: Verify criteria #1 (coverage) + #2 (earliest-record synthesis)** — spot-check a mid-day minute: every symbol present in that minute's snapshot OR order appears; the order value is the earliest `(time, rcvtime)` of the minute.

- [ ] **Step 4: Commit the diagnostic run artifacts are NOT committed** (gitignored under `test/`). No commit step — just report results.

- [ ] **Step 5: Final commit if any spec/plan doc updates are needed** (e.g. note the `is_trading_minute` correction in the spec — it does NOT filter 1531; the pipeline tolerates arbitrary keys). Update spec §6 accordingly.

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Spec §2 (rule: floor+1, uniform order+snapshot, 整点也+1, allow 1531/跨日) → Tasks 1, 4 (+ tests in Task 1 step 1 cover all 4 rule points incl. cross-day).
- Spec §4.1 (algorithm, string parse not int div) → Task 1 (Python strptime) + Task 4 (Rust hand-rolled carry).
- Spec §4.2 (4 code changes) → Tasks 1, 2 (clock.py×3) + Tasks 4, 5 (lib.rs×2).
- Spec §5 (end_time/start_time shift + is_expired invariant) → Task 2 + Task 8 step 4.
- Spec §6 (1531/cross-day — handled by existing logic, is_trading_minute does NOT filter) → Task 9 step 5 (correct the spec's false claim); no new code needed (verified by investigation).
- Spec §7 invariants → covered by Tasks 1/4 tests + Task 8.
- Spec §8 tests → Tasks 1, 2, 4, 7.

**Placeholder scan:** No TBD/TODO. Every code step shows actual code or verbatim old→new. Golden-test literal shifts use the universal +1 rule + pytest-driven pinpointing (deterministic, not guesswork).

**Type/signature consistency:** `time_to_minute_key(int) -> str` (Python), `time_to_minute_key(i64) -> String` (Rust) — unchanged signature, body only. `PreprocessedRecord.minute_key: String` (owned) → Task 5 step 2 drops redundant `.to_string()`. `increment_yyyymmdd` defined in Task 4, used by `time_to_minute_key` in same task (forward-defined in same file — Rust allows).

**Gotchas addressed (from investigation):** 5 separate inline derivations (Tasks 3+5), Rust no-chrono (Task 4 hand-rolled), date slices untouched (DO NOT TOUCH list), is_expired atomicity (header warning), coordinated late-routing +1 (Task 7 rule), test_clock absolute breaks (Task 2), integer-div test replaced (Task 7 step 6).
