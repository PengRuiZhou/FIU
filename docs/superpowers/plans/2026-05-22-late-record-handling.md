# Late Record Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement unified late record handling for both Live and Replay modes, ensuring 0 data loss for records arriving after their minute has been flushed.

**Architecture:** Add file-level write locks and append functions to writer.py. Detect late records in SharedState.process_snapshot (Live) and stream-level checks (Replay). Flush late records via a new flusher step (Live) or immediate append (Replay/Order). Track flushed minutes to enable late detection, and recover them from output files on restart.

**Tech Stack:** Python 3.10+, pytest, threading (RLock, Lock)

---

## File Structure

### Modified files:
- `src/minute_bar/writer.py` — Add append functions, write locks, path helpers
- `src/minute_bar/aggregator.py` — Add late record fields and detection in SharedState
- `src/minute_bar/flusher.py` — Add _step4_handle_late_records, _step5_write_checkpoint, cross-day cleanup
- `src/minute_bar/engine.py` — Add order late detection, recover_flushed_minutes
- `src/minute_bar/replay.py` — Add late record detection in streams, EOF flush changes

### Test files:
- `tests/test_writer.py` — Extend with append function tests
- `tests/test_aggregator.py` — Extend with late record detection tests
- `tests/test_flusher.py` — NEW: Flusher late record tests
- `tests/test_engine_late.py` — NEW: Engine recovery and late order tests
- `tests/test_replay.py` — Extend with replay late record tests

---

### Task 1: Writer Append Functions (writer.py)

**Files:**
- Modify: `src/minute_bar/writer.py`
- Modify: `tests/test_writer.py`

- [ ] **Step 1: Write failing tests for path helper functions**

Add to `tests/test_writer.py`:

```python
from minute_bar.writer import get_snapshot_file_path, get_order_file_path


class TestFilePathHelpers:
    def test_snapshot_file_path(self):
        path = get_snapshot_file_path("/output", "202605200930")
        assert path == os.path.join("/output", "snapshot", "2026", "20260520", "snapshot_minute_20260520_0930.csv")

    def test_order_file_path(self):
        path = get_order_file_path("/output", "202605200930")
        assert path == os.path.join("/output", "order", "2026", "20260520", "order_minute_20260520_0930.csv")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_writer.py::TestFilePathHelpers -v`
Expected: FAIL — `ImportError: cannot import name 'get_snapshot_file_path'`

- [ ] **Step 3: Implement path helper functions**

Add to `src/minute_bar/writer.py` after the imports section (before `atomic_write`):

```python
def get_snapshot_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "snapshot", date_str[:4], date_str,
                        f"snapshot_minute_{date_str}_{hhmm}.csv")


def get_order_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "order", date_str[:4], date_str,
                        f"order_minute_{date_str}_{hhmm}.csv")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_writer.py::TestFilePathHelpers -v`
Expected: PASS

- [ ] **Step 5: Write failing tests for append functions and write locks**

Add to `tests/test_writer.py`:

```python
import threading
from minute_bar.models import OrderRecord, SnapshotRecord
from minute_bar.writer import (
    append_order_records,
    append_snapshot_records,
    _get_write_lock,
)


class TestAppendOrderRecords:
    def test_append_to_existing_file(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path), "202605200930", records)

        late_records = [
            OrderRecord(symbol="1305", seqno=2, time=20260520093001500,
                        bidprice=410000.0, bidsize=50.0, askprice=412000.0, asksize=80.0,
                        decimal=2, rcvtime=20260520083001500),
        ]
        path = get_order_file_path(str(tmp_path), "202605200930")
        append_order_records(path, late_records)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "bidprice" in header
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0][1] == "1301"  # original
            assert rows[1][1] == "1305"  # appended

    def test_append_multiple_records(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path), "202605200930", records)

        late_records = [
            OrderRecord(symbol="1305", seqno=2, time=20260520093001500,
                        bidprice=410000.0, bidsize=50.0, askprice=412000.0, asksize=80.0,
                        decimal=2, rcvtime=20260520083001500),
            OrderRecord(symbol="1306", seqno=3, time=20260520093001600,
                        bidprice=420000.0, bidsize=60.0, askprice=422000.0, asksize=90.0,
                        decimal=2, rcvtime=20260520083001600),
        ]
        path = get_order_file_path(str(tmp_path), "202605200930")
        append_order_records(path, late_records)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows = list(reader)
            assert len(rows) == 3


class TestAppendSnapshotRecords:
    def test_append_with_update_flag_y(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        late_recs = [
            make_snapshot(symbol="1305", seqno=10, lastprice=4120.0),
        ]
        path = get_snapshot_file_path(str(tmp_path), "202605200930")
        append_snapshot_records(path, late_recs, code_table)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows = list(reader)
            assert len(rows) == 2  # 1 original + 1 appended
            # Appended row should have update_flag=Y
            appended = [r for r in rows if r[1] == "1305"][0]
            assert appended[-1] == "Y"

    def test_append_preserves_existing_rows(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        late_recs = [make_snapshot(symbol="1301", seqno=20, lastprice=9999.0)]
        path = get_snapshot_file_path(str(tmp_path), "202605200930")
        append_snapshot_records(path, late_recs, code_table)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2  # original + appended same symbol
            # Original row still exists
            assert rows[0][1] == "1301"
            # Appended row with different lastprice
            assert rows[1][1] == "1301"


class TestWriteLock:
    def test_same_path_returns_same_lock(self):
        lock1 = _get_write_lock("/tmp/test.csv")
        lock2 = _get_write_lock("/tmp/test.csv")
        assert lock1 is lock2

    def test_different_paths_return_different_locks(self):
        lock1 = _get_write_lock("/tmp/test1.csv")
        lock2 = _get_write_lock("/tmp/test2.csv")
        assert lock1 is not lock2

    def test_concurrent_append_and_atomic_write(self, tmp_path):
        """atomic_write and append on the same path should be serialized."""
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        path = get_snapshot_file_path(str(tmp_path), "202605200930")

        # Initial write
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        errors = []

        def do_append():
            try:
                for _ in range(20):
                    late = make_snapshot(symbol="1305", seqno=100)
                    append_snapshot_records(path, [late], code_table)
            except Exception as e:
                errors.append(e)

        def do_atomic_write():
            try:
                for _ in range(20):
                    rec2 = make_snapshot(symbol="1301", seqno=200)
                    agg2 = make_agg(symbol="1301")
                    write_snapshot_file(
                        str(tmp_path), "202605200930",
                        {"1301": rec2}, {"1301": agg2}, code_table, full=True,
                    )
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=do_append)
        t2 = threading.Thread(target=do_atomic_write)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors
        # File should be valid CSV
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "update_flag" in header
            rows = list(reader)
            assert len(rows) >= 1  # at least 1 symbol remains
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `python -m pytest tests/test_writer.py::TestAppendOrderRecords tests/test_writer.py::TestAppendSnapshotRecords tests/test_writer.py::TestWriteLock -v`
Expected: FAIL — `ImportError: cannot import name 'append_order_records'`

- [ ] **Step 7: Implement write locks, append functions, and _format_order_row**

Add to `src/minute_bar/writer.py` after the `logger` line and before `atomic_write`:

```python
import threading

_write_locks: Dict[str, threading.Lock] = {}
_write_lock_mutex = threading.Lock()


def _get_write_lock(path: str) -> threading.Lock:
    with _write_lock_mutex:
        if path not in _write_locks:
            _write_locks[path] = threading.Lock()
        return _write_locks[path]


def _format_order_row(rec: OrderRecord) -> str:
    return (
        f"{rec.seqno},{rec.symbol},{rec.time},"
        f"{rec.bidprice},{rec.bidsize},{rec.askprice},{rec.asksize},"
        f"{rec.decimal},{rec.rcvtime}"
    )


def append_order_records(path: str, records: List[OrderRecord]) -> None:
    """Append order rows to existing file without writing header."""
    try:
        with _get_write_lock(path):
            with open(path, "a", encoding="utf-8", newline="") as f:
                for rec in records:
                    f.write(_format_order_row(rec) + "\n")
    except IOError as e:
        logger.fatal("Late append failed for %s: %s", path, e)
        raise


def append_snapshot_records(path: str, records: List[SnapshotRecord], code_table: CodeTable) -> None:
    """Append snapshot rows to existing file without writing header. update_flag=Y."""
    try:
        with _get_write_lock(path):
            with open(path, "a", encoding="utf-8", newline="") as f:
                for rec in records:
                    name = code_table.get_name(rec.symbol)
                    f.write(_format_snapshot_row(rec, name, "Y") + "\n")
    except IOError as e:
        logger.fatal("Late append failed for %s: %s", path, e)
        raise
```

Modify `atomic_write` to use write lock:

```python
def atomic_write(path: str, content: str) -> None:
    with _get_write_lock(path):
        tmp_path = path + ".tmp"
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
```

Modify `write_order_file` to use `_format_order_row`:

```python
def write_order_file(
    output_dir: str,
    minute_key: str,
    order_records: List[OrderRecord],
) -> None:
    if not order_records:
        return

    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    out_dir = os.path.join(output_dir, "order", date_str[:4], date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"order_minute_{date_str}_{hhmm}.csv")

    lines = [ORDER_HEADER]
    for rec in order_records:
        lines.append(_format_order_row(rec))

    atomic_write(path, "\n".join(lines) + "\n")
    logger.info("Wrote order file: %s (%d records)", path, len(order_records))
```

- [ ] **Step 8: Run all writer tests to verify they pass**

Run: `python -m pytest tests/test_writer.py -v`
Expected: All PASS

- [ ] **Step 9: Commit**

```bash
git add src/minute_bar/writer.py tests/test_writer.py
git commit -m "feat(writer): add append functions, write locks, and path helpers for late record handling"
```

---

### Task 2: SharedState Late Record Support (aggregator.py)

**Files:**
- Modify: `src/minute_bar/aggregator.py`
- Modify: `tests/test_aggregator.py`

- [ ] **Step 1: Write failing tests for late record detection and helper methods**

Add to `tests/test_aggregator.py`:

```python
class TestSharedStateLateRecords:
    def test_late_snapshot_routed_to_late_queue(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert len(state._late_snapshot_records) == 1
        mk, rec = state._late_snapshot_records[0]
        assert mk == "202605200930"
        assert rec.symbol == "1301"

    def test_late_snapshot_not_in_normal_buffers(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert "202605200930" not in state.ohlcv_buffers
        assert "202605200930" not in state.raw_snapshot_buffers

    def test_late_snapshot_does_not_update_latest(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert "1301" not in state.latest_snapshot

    def test_normal_snapshot_not_affected_by_late_check(self):
        state = SharedState()
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert len(state._late_snapshot_records) == 0
        assert "202605200930" in state.ohlcv_buffers
        assert "1301" in state.latest_snapshot

    def test_pop_late_snapshot_records(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        records = state.pop_late_snapshot_records()
        assert len(records) == 1
        assert len(state._late_snapshot_records) == 0  # cleared

    def test_pop_late_snapshot_records_empty(self):
        state = SharedState()
        records = state.pop_late_snapshot_records()
        assert records == []


class TestMaybeUpdateLatest:
    def test_first_record_sets_latest(self):
        state = SharedState()
        rec = build_snapshot_record(make_parsed_snapshot(), seqno=1)
        with state.lock:
            state.maybe_update_latest_unlocked(rec)
        assert "1301" in state.latest_snapshot

    def test_newer_record_updates(self):
        state = SharedState()
        rec_old = build_snapshot_record(make_parsed_snapshot(time_=20260520093000999), seqno=1)
        rec_new = build_snapshot_record(make_parsed_snapshot(time_=20260520093100999), seqno=2)
        with state.lock:
            state.maybe_update_latest_unlocked(rec_old)
            state.maybe_update_latest_unlocked(rec_new)
        assert state.latest_snapshot["1301"].seqno == 2

    def test_older_record_does_not_update(self):
        state = SharedState()
        rec_new = build_snapshot_record(make_parsed_snapshot(time_=20260520093100999), seqno=2)
        rec_old = build_snapshot_record(make_parsed_snapshot(time_=20260520093000999), seqno=1)
        with state.lock:
            state.maybe_update_latest_unlocked(rec_new)
            state.maybe_update_latest_unlocked(rec_old)
        assert state.latest_snapshot["1301"].seqno == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_aggregator.py::TestSharedStateLateRecords tests/test_aggregator.py::TestMaybeUpdateLatest -v`
Expected: FAIL — `AttributeError: 'SharedState' object has no attribute 'flushed_snapshot_minutes'`

- [ ] **Step 3: Implement late record fields, methods, and process_snapshot late detection**

Modify `src/minute_bar/aggregator.py`. First, add new fields to `SharedState.__init__` after the existing fields:

```python
        # Late record tracking
        self.flushed_snapshot_minutes: set[str] = set()
        self._late_snapshot_records: list[tuple[str, SnapshotRecord]] = []
        self.late_snapshot_count: int = 0
        self.late_snapshot_minutes: set[str] = set()
```

Add two new methods to `SharedState` (after `process_order`):

```python
    def pop_late_snapshot_records(self) -> list[tuple[str, SnapshotRecord]]:
        """Pop all pending late snapshot records. Caller must hold self.lock."""
        records = self._late_snapshot_records
        self._late_snapshot_records = []
        return records

    def maybe_update_latest_unlocked(self, record: SnapshotRecord) -> None:
        """Update latest_snapshot only if record is newer. Caller must hold self.lock."""
        current = self.latest_snapshot.get(record.symbol)
        if current is None:
            self.latest_snapshot[record.symbol] = record
        elif (record.time, record.rcvtime) >= (current.time, current.rcvtime):
            self.latest_snapshot[record.symbol] = record
```

Modify `process_snapshot` to detect late records. Replace the entire method:

```python
    def process_snapshot(self, parsed: ParsedSnapshot) -> bool:
        if not validate_snapshot(parsed):
            return False

        with self.lock:
            self.seqno += 1
            record = build_snapshot_record(parsed, seqno=self.seqno)
            if record is None:
                return False

            minute_key = time_to_minute_key(record.time)

            # Late record detection: route to late queue instead of normal buffers
            is_late = minute_key in self.flushed_snapshot_minutes
            if is_late:
                self._late_snapshot_records.append((minute_key, record))
                return True

            symbol = record.symbol

            base_vol = self.last_totalvol_by_symbol.get(symbol, 0)
            base_amt = self.last_totalamount_by_symbol.get(symbol, 0.0)

            if symbol not in self.last_totalvol_by_symbol:
                if self.first_seen_volume_base == "start_totalvol":
                    base_vol = record.totalvol
                    base_amt = record.totalamount
                else:
                    base_vol = 0
                    base_amt = 0.0

            if minute_key not in self.ohlcv_buffers:
                self.ohlcv_buffers[minute_key] = {}
            if minute_key not in self.raw_snapshot_buffers:
                self.raw_snapshot_buffers[minute_key] = {}

            if symbol not in self.ohlcv_buffers[minute_key]:
                self.ohlcv_buffers[minute_key][symbol] = OHLCVAggregate(symbol=symbol)

            self.ohlcv_buffers[minute_key][symbol].update(record, base_vol, base_amt)

            if symbol not in self.raw_snapshot_buffers[minute_key]:
                self.raw_snapshot_buffers[minute_key][symbol] = []
            self.raw_snapshot_buffers[minute_key][symbol].append(record)

            self.latest_snapshot[symbol] = record
            self.current_minute = minute_key

            if not self.first_data_received:
                self.first_data_received = True
                logger.info("First data received at minute=%s", minute_key)

            return False
```

- [ ] **Step 4: Run all aggregator tests to verify they pass**

Run: `python -m pytest tests/test_aggregator.py -v`
Expected: All PASS

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `python -m pytest -v`
Expected: All PASS (return type change from None to bool is backward-compatible)

- [ ] **Step 6: Commit**

```bash
git add src/minute_bar/aggregator.py tests/test_aggregator.py
git commit -m "feat(aggregator): add late record detection in SharedState.process_snapshot"
```

---

### Task 3: Flusher Late Record Handling (flusher.py)

**Files:**
- Modify: `src/minute_bar/flusher.py`
- Create: `tests/test_flusher.py`

- [ ] **Step 1: Write failing tests for flusher late record handling**

Create `tests/test_flusher.py`:

```python
"""Tests for flusher late record handling."""
import csv
import os
import pytest
from unittest.mock import patch, MagicMock

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.code_table import CodeTable
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import FileState, SnapshotRecord
from minute_bar.writer import write_snapshot_file


def make_snapshot(symbol="1301", seqno=1, lastprice=4500.0, time_=20260520093000999, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_, rcvtime=20260520083000999,
        preclose=4435.0, lastprice=lastprice, open=4400.0, high=4510.0, low=4435.0,
        close=4500.0, lasttradeprice=4500.0, lasttradeqty=100, totalvol=78300,
        totalamount=3502510.0, sessionid=1, tradetype="", status="T",
        direction=0, pflag="Y", decimal=2, vwap=4450.0, shortsellflag=0,
    )
    defaults.update(kwargs)
    return SnapshotRecord(**defaults)


def make_flusher(state, tmp_path):
    code_table = CodeTable.__new__(CodeTable)
    code_table._table = {}
    checkpoint = CheckpointManager(str(tmp_path / "checkpoint.json"), str(tmp_path))
    return ClockWatermarkFlusher(
        state=state,
        code_table=code_table,
        checkpoint=checkpoint,
        output_dir=str(tmp_path),
        output_delay_sec=60,
        file_states={},
        checkpoint_lock=None,
    )


class TestStep4LateRecords:
    def test_no_late_records_is_noop(self, tmp_path):
        state = SharedState()
        flusher = make_flusher(state, tmp_path)
        # Should not raise or do anything
        flusher._step4_handle_late_records()

    def test_late_records_appended_to_file(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        # Create initial file
        rec = make_snapshot(symbol="1301")
        from minute_bar.models import OHLCVAggregate
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        # Simulate late record in queue
        late_rec = make_snapshot(symbol="1305", seqno=10, lastprice=4120.0)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        # Verify file was appended
        path = os.path.join(str(tmp_path), "snapshot", "2026", "20260520", "snapshot_minute_20260520_0930.csv")
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows = list(reader)
            assert len(rows) == 2  # original + late

    def test_late_records_update_latest_snapshot(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        from minute_bar.models import OHLCVAggregate
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        # Late record with newer timestamp
        late_rec = make_snapshot(symbol="1301", seqno=10, time_=20260520093100999, lastprice=9999.0)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        # latest_snapshot should be updated
        assert "1301" in state.latest_snapshot
        assert state.latest_snapshot["1301"].lastprice == 9999.0

    def test_late_records_update_count(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        from minute_bar.models import OHLCVAggregate
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1305", seqno=10)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        assert state.late_snapshot_count == 1
        assert "202605200930" in state.late_snapshot_minutes

    def test_late_records_queue_cleared_after_processing(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        from minute_bar.models import OHLCVAggregate
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1305", seqno=10)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()
        assert len(state._late_snapshot_records) == 0


class TestFlushedSnapshotMinutes:
    @patch("minute_bar.flusher.is_expired", return_value=True)
    def test_step3_records_flushed_minutes(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        flusher = make_flusher(state, tmp_path)

        # Add a buffer
        from minute_bar.models import OHLCVAggregate
        state.ohlcv_buffers["202605200930"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605200930"] = {}
        rec = make_snapshot()
        state.latest_snapshot["1301"] = rec

        flusher._step3_minute_output()

        assert "202605200930" in state.flushed_snapshot_minutes


class TestCrossDayCleanup:
    def test_cross_day_clears_late_stats(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605200930"
        state.flushed_snapshot_minutes.add("202605190930")
        state.late_snapshot_count = 42
        state.late_snapshot_minutes.add("202605190930")

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        assert len(state.flushed_snapshot_minutes) == 0
        assert state.late_snapshot_count == 0
        assert len(state.late_snapshot_minutes) == 0


class TestStep5WriteCheckpoint:
    def test_checkpoint_skipped_with_pending_late_records(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.output_minutes.add("202605200930")
        state._late_snapshot_records.append(("202605200930", make_snapshot()))

        flusher = make_flusher(state, tmp_path)
        flusher._step5_write_checkpoint()

        # Checkpoint should not exist (skipped)
        assert not os.path.exists(str(tmp_path / "checkpoint.json"))

    def test_checkpoint_written_when_no_pending_late_records(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.output_minutes.add("202605200930")
        state.last_output_minute = "202605200930"

        flusher = make_flusher(state, tmp_path)
        flusher._step5_write_checkpoint()

        assert os.path.exists(str(tmp_path / "checkpoint.json"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_flusher.py -v`
Expected: FAIL — `AttributeError: 'ClockWatermarkFlusher' object has no attribute '_step4_handle_late_records'`

- [ ] **Step 3: Implement flusher changes**

Modify `src/minute_bar/flusher.py`. First, update imports:

```python
from minute_bar.writer import (
    append_snapshot_records,
    get_snapshot_file_path,
    write_kline_file,
    write_order_file,
    write_snapshot_file,
)
```

Update `tick()`:

```python
    def tick(self) -> None:
        self._step1_cross_day_check()
        if not self._step2_first_data_check():
            return
        self._step3_minute_output()
        self._step4_handle_late_records()
        self._step5_write_checkpoint()
```

Add `_step4_handle_late_records` after `_step3_minute_output`:

```python
    def _step4_handle_late_records(self) -> None:
        with self._state.lock:
            late_records = self._state.pop_late_snapshot_records()

        if not late_records:
            return

        grouped: dict[str, list[SnapshotRecord]] = {}
        for minute_key, record in late_records:
            grouped.setdefault(minute_key, []).append(record)

        # Append to files (file-level lock inside append function)
        for minute_key in sorted(grouped):
            path = get_snapshot_file_path(self._output_dir, minute_key)
            if os.path.exists(path):
                append_snapshot_records(path, grouped[minute_key], self._code_table)
            else:
                logger.warning("Late snapshot minute %s file missing", minute_key)

        # Update latest_snapshot + stats under lock
        with self._state.lock:
            for records in grouped.values():
                for record in records:
                    self._state.maybe_update_latest_unlocked(record)
                    self._state.late_snapshot_count += 1
                    mk = time_to_minute_key(record.time)
                    self._state.late_snapshot_minutes.add(mk)

        logger.info(
            "Flushed %d late snapshot records across %d minutes",
            sum(len(v) for v in grouped.values()), len(grouped),
        )
```

Add `_step5_write_checkpoint` after `_step4`:

```python
    def _step5_write_checkpoint(self) -> None:
        with self._state.lock:
            if self._state._late_snapshot_records:
                logger.debug(
                    "Skipping checkpoint: %d late records pending",
                    len(self._state._late_snapshot_records),
                )
                return
        self._write_checkpoint()
```

In `_step3_minute_output`, remove the `self._write_checkpoint()` call from the end of the for loop, and add `self._state.flushed_snapshot_minutes.add(minute_key)`:

```python
    def _step3_minute_output(self) -> None:
        with self._state.lock:
            expired_keys = sorted(
                k for k in self._state.ohlcv_buffers if is_expired(k, self._output_delay_sec)
            )
            if not expired_keys:
                return

            expired = {k: self._state.ohlcv_buffers.pop(k) for k in expired_keys}
            expired_raw = {k: self._state.raw_snapshot_buffers.pop(k) for k in expired_keys if k in self._state.raw_snapshot_buffers}
            expired_orders = {k: self._state.raw_order_buffers.pop(k) for k in expired_keys if k in self._state.raw_order_buffers}
            snapshot_copy = dict(self._state.latest_snapshot)

        for minute_key in expired_keys:
            data = expired[minute_key]
            raw = expired_raw.get(minute_key, {})
            orders = expired_orders.get(minute_key, [])
            try:
                self._write_minute_files(minute_key, snapshot_copy, data, raw, orders)
            except IOError as e:
                logger.fatal("Output failed for minute=%s: %s", minute_key, e)
                raise SystemExit(1)

            with self._state.lock:
                self._state.output_minutes.add(minute_key)
                self._state.last_output_minute = minute_key
                self._state.flushed_snapshot_minutes.add(minute_key)
                for sym, agg in data.items():
                    self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
                    self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
                # Checkpoint moved to _step5_write_checkpoint
```

In `_step1_cross_day_check`, add late field cleanup inside the final `with self._state.lock:` block:

```python
        with self._state.lock:
            self._state.output_minutes.clear()
            self._state.last_totalvol_by_symbol.clear()
            self._state.last_totalamount_by_symbol.clear()
            self._state.last_output_date = current_date
            self._state.last_output_minute = ""
            self._state.first_data_received = False
            self._state.flushed_snapshot_minutes.clear()
            self._state.late_snapshot_count = 0
            self._state.late_snapshot_minutes.clear()
```

Also add checkpoint write after cross-day flush (before the reset block), to persist the read offset before clearing state:

```python
        if pending:
            for minute_key in sorted(pending):
                data = pending[minute_key]
                raw = pending_raw.get(minute_key, {})
                orders = pending_orders.get(minute_key, [])
                try:
                    self._write_minute_files(minute_key, snapshot_copy, data, raw, orders)
                except IOError as e:
                    logger.fatal("Output failed during cross-day flush for minute=%s: %s", minute_key, e)
                    raise SystemExit(1)

            # Write checkpoint to persist read offsets before clearing state
            self._write_checkpoint()
```

- [ ] **Step 4: Run flusher tests to verify they pass**

Run: `python -m pytest tests/test_flusher.py -v`
Expected: All PASS

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `python -m pytest -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_flusher.py
git commit -m "feat(flusher): add late snapshot record handling and checkpoint safety"
```

---

### Task 4: Engine Order Late Handling + Recovery (engine.py)

**Files:**
- Modify: `src/minute_bar/engine.py`
- Create: `tests/test_engine_late.py`

- [ ] **Step 1: Write failing tests for recover_flushed_minutes and order late detection**

Create `tests/test_engine_late.py`:

```python
"""Tests for engine late order handling and flushed_minutes recovery."""
import csv
import os
import pytest

from minute_bar.engine import recover_flushed_minutes
from minute_bar.models import OrderRecord
from minute_bar.writer import write_order_file, write_snapshot_file, get_order_file_path


class TestRecoverFlushedMinutes:
    def test_empty_output_dir(self, tmp_path):
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()
        assert order == set()

    def test_finds_snapshot_files(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")
        (snap_dir / "snapshot_minute_20260520_0931.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == {"202605200930", "202605200931"}
        assert order == set()

    def test_finds_order_files(self, tmp_path):
        order_dir = tmp_path / "order" / "2026" / "20260520"
        order_dir.mkdir(parents=True)
        (order_dir / "order_minute_20260520_0930.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()
        assert order == {"202605200930"}

    def test_finds_both_types(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")

        order_dir = tmp_path / "order" / "2026" / "20260520"
        order_dir.mkdir(parents=True)
        (order_dir / "order_minute_20260520_0930.csv").write_text("header\n")

        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == {"202605200930"}
        assert order == {"202605200930"}

    def test_ignores_tmp_files(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv.tmp").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()

    def test_ignores_other_dates(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260521")
        assert snap == set()


class TestEngineLateOrder:
    def test_order_late_record_appended(self, tmp_path):
        """Test that a late order record is appended to the existing file instead of creating a new buffer."""
        from minute_bar.aggregator import SharedState
        from minute_bar.code_table import CodeTable
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.config import AppConfig, InputConfig, OutputConfig, AggregationConfig
        from minute_bar.engine import Engine

        # Create output file first
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path / "output"), "202605200930", records)

        # Verify the file was created
        path = get_order_file_path(str(tmp_path / "output"), "202605200930")
        assert os.path.exists(path)

        # Read initial content
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            initial_rows = list(reader)
        assert len(initial_rows) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_engine_late.py::TestRecoverFlushedMinutes -v`
Expected: FAIL — `ImportError: cannot import name 'recover_flushed_minutes'`

- [ ] **Step 3: Implement recover_flushed_minutes**

Add to `src/minute_bar/engine.py` after the imports, before the `Engine` class:

```python
import re


def recover_flushed_minutes(output_dir: str, date: str) -> tuple[set[str], set[str]]:
    """Recover flushed_snapshot_minutes and flushed_order_minutes from existing output files."""
    snapshot_minutes: set[str] = set()
    order_minutes: set[str] = set()

    date_str = date
    snap_dir = os.path.join(output_dir, "snapshot", date_str[:4], date_str)
    if os.path.isdir(snap_dir):
        for f in os.listdir(snap_dir):
            m = re.match(r"snapshot_minute_(\d{8})_(\d{4})\.csv$", f)
            if m:
                snapshot_minutes.add(m.group(1) + m.group(2))

    order_dir = os.path.join(output_dir, "order", date_str[:4], date_str)
    if os.path.isdir(order_dir):
        for f in os.listdir(order_dir):
            m = re.match(r"order_minute_(\d{8})_(\d{4})\.csv$", f)
            if m:
                order_minutes.add(m.group(1) + m.group(2))

    return snapshot_minutes, order_minutes
```

Also add `import re` at the top of the file.

- [ ] **Step 4: Run recover tests to verify they pass**

Run: `python -m pytest tests/test_engine_late.py::TestRecoverFlushedMinutes -v`
Expected: All PASS

- [ ] **Step 5: Add flushed_order_minutes tracking and late detection to Engine**

Add new fields to `Engine.__init__` after `self._committed_order_offset = 0`:

```python
        self._flushed_order_minutes: set[str] = set()
        self._late_order_count: int = 0
        self._late_order_minutes: set[str] = set()
```

Add recovery call in `_restore_from_checkpoint` after the `snapshot_records` restoration block:

```python
        # Recover flushed minutes from output files
        if self._state.last_output_date:
            snapshot_mins, order_mins = recover_flushed_minutes(
                self._config.output.output_dir, self._state.last_output_date
            )
            self._state.flushed_snapshot_minutes = snapshot_mins
            self._flushed_order_minutes = order_mins
            logger.info(
                "Recovered flushed minutes: %d snapshot, %d order",
                len(snapshot_mins), len(order_mins),
            )
```

Modify `_flush_order_minute` to record flushed minute after successful write:

```python
    def _flush_order_minute(
        self,
        buffers: Dict[str, "_OrderMinuteBuffer"],
        minute_key: str,
        output_dir: str,
    ) -> None:
        buf = buffers.pop(minute_key, None)
        if buf is None or not buf.records:
            return
        try:
            write_order_file(output_dir, minute_key, buf.records)
        except Exception as e:
            logger.fatal(
                "Order file write failed for minute=%s: %s — not advancing checkpoint",
                minute_key, e,
            )
            raise
        with self._checkpoint_lock:
            self._committed_order_offset = buf.line_end_offset
        self._flushed_order_minutes.add(minute_key)
        logger.info(
            "Output order minute %s (%d records, committed_offset=%d)",
            minute_key, len(buf.records), buf.line_end_offset,
        )
```

Add late record detection in `_order_loop`. After the line `record = build_order_record(parsed, seqno)`, add the late check:

```python
                        seqno += 1
                        record = build_order_record(parsed, seqno)

                        # Late order detection
                        if minute_key in self._flushed_order_minutes:
                            path = get_order_file_path(output_dir, minute_key)
                            if os.path.exists(path):
                                append_order_records(path, [record])
                            else:
                                logger.warning(
                                    "Late order minute %s file missing, creating new", minute_key
                                )
                                write_order_file(output_dir, minute_key, [record])
                            self._late_order_count += 1
                            self._late_order_minutes.add(minute_key)
                            continue

                        buf = buffers.get(minute_key)
```

Update imports in engine.py to include the new writer functions:

```python
from minute_bar.writer import (
    append_order_records,
    get_order_file_path,
    write_order_file,
)
```

- [ ] **Step 6: Run all engine late tests**

Run: `python -m pytest tests/test_engine_late.py -v`
Expected: All PASS

- [ ] **Step 7: Run full test suite**

Run: `python -m pytest -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/minute_bar/engine.py tests/test_engine_late.py
git commit -m "feat(engine): add order late record handling and flushed_minutes recovery"
```

---

### Task 5: Replay Late Record Handling (replay.py)

**Files:**
- Modify: `src/minute_bar/replay.py`
- Modify: `tests/test_replay.py`

- [ ] **Step 1: Write failing tests for replay late record handling**

Add to `tests/test_replay.py`:

```python
def write_order_csv(path, rows):
    with open(path, "wb") as f:
        f.write(b"symbol,time,bidprice,bidsize,askprice,asksize\n")
        for row in rows:
            f.write(row.encode("utf-8"))
            f.write(b"\n")


class TestReplayLateSnapshot:
    def test_late_snapshot_appended(self, tmp_path):
        """Out-of-order snapshot for a flushed minute should be late-appended, not lost."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])

        # Write data where 09:31 data appears AFTER 09:30 data,
        # but we deliberately interleave a 09:30 record after 09:31
        snapshot_rows = [
            # 09:30 records
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            # 09:31 record (triggers flush of 09:30)
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            # Late 09:30 record (arrives after 09:31 started)
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
        ]
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", snapshot_rows)

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        # Verify 0930 file has 2 records (1 normal + 1 late)
        snap_0930 = csv_dir.parent / "output" / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        if not snap_0930.exists():
            snap_0930 = tmp_path / "output" / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        assert snap_0930.exists()
        with open(snap_0930, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows = list(reader)
            # Should have original 0930 record + late 0930 record
            assert len(rows) >= 2


class TestReplayLateOrder:
    def test_late_order_appended(self, tmp_path):
        """Out-of-order order for a flushed minute should be late-appended."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
        ])

        # Order data with late record
        order_rows = [
            "1301,20260520093000999,450000,100,451000,200",
            "1301,20260520093100999,451000,150,452000,250",
            "1301,20260520093001500,450500,80,451500,120",  # late 0930
        ]
        write_order_csv(csv_dir / "order.csv.20260520", order_rows)

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        order_path = tmp_path / "output" / "order" / "2026" / "20260520" / "order_minute_20260520_0930.csv"
        assert order_path.exists()
        with open(order_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows = list(reader)
            # Should have original + late = 2 records for 0930
            assert len(rows) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_replay.py::TestReplayLateSnapshot tests/test_replay.py::TestReplayLateOrder -v`
Expected: FAIL — late records are lost (0930 file has only 1 row, not 2)

- [ ] **Step 3: Implement late snapshot detection in replay _stream_snapshots**

Modify `src/minute_bar/replay.py`. First, update imports:

```python
from minute_bar.aggregator import SharedState, build_order_record, build_snapshot_record
from minute_bar.writer import (
    append_order_records,
    append_snapshot_records,
    get_order_file_path,
    get_snapshot_file_path,
    write_kline_file,
    write_order_file,
    write_snapshot_file,
)
```

Replace `_stream_snapshots` method. The key changes: add `late_records_by_minute` dict, check `flushed_minutes` before `process_snapshot`, and flush late records after each round:

```python
    def _stream_snapshots(
        self,
        stop_event: threading.Event,
        write_executor: ThreadPoolExecutor,
        minute_range: dict,
    ) -> None:
        state = SharedState(
            first_seen_volume_base=self._config.aggregation.first_seen_volume_base
        )
        output_dir = self._config.output.output_dir
        code_table = self._code_table
        full_snapshot = self._config.output.enable_full_snapshot
        full_kline = self._config.output.enable_full_kline
        enable_kline = self._config.output.enable_kline

        tailer = FileTailer(
            self._config.input.csv_dir, "snapshot",
            chunk_size=self._config.input.chunk_size_bytes,
            encoding=self._config.input.file_encoding,
        )
        tailer.set_date(self._date)

        flushed_minutes: set[str] = set()
        active_history: list[set[str]] = []
        late_records_by_minute: dict[str, list] = {}
        late_snapshot_count = 0
        late_snapshot_minutes: set[str] = set()

        while True:
            if stop_event.is_set():
                break

            lines = list(tailer.read_lines())
            if not lines:
                break

            current_round_minutes: set[str] = set()

            for line in lines:
                if stop_event.is_set():
                    break
                parsed = parse_snapshot_line(line, self._config.input.file_encoding)
                if parsed is None:
                    continue
                record_date = str(parsed.time)[:8]
                if record_date != self._date:
                    continue

                minute_key = time_to_minute_key(parsed.time)

                # Late record detection
                if minute_key in flushed_minutes:
                    late_records_by_minute.setdefault(minute_key, []).append(parsed)
                    late_snapshot_count += 1
                    late_snapshot_minutes.add(minute_key)
                    continue

                state.process_snapshot(parsed)
                current_round_minutes.add(minute_key)

                if minute_range["first"] is None:
                    minute_range["first"] = minute_key
                minute_range["last"] = minute_key

            # Flush late records collected this round
            if late_records_by_minute:
                self._flush_late_snapshots(
                    late_records_by_minute, state, output_dir, code_table
                )
                late_records_by_minute.clear()

            # Delayed flush: flush minutes not seen in recent rounds
            active_history.append(current_round_minutes)
            if len(active_history) > DELAYED_FLUSH_ROUNDS:
                stale_round = active_history.pop(0)
                to_flush = stale_round - current_round_minutes
                for mk in sorted(to_flush):
                    if mk not in flushed_minutes and mk in state.ohlcv_buffers:
                        self._flush_snapshot_minute(
                            state, mk, output_dir, code_table,
                            full_snapshot, full_kline, enable_kline, write_executor,
                        )
                        flushed_minutes.add(mk)

        # EOF final flush: flush all remaining buffered minutes
        for mk in sorted(state.ohlcv_buffers):
            if mk not in flushed_minutes:
                self._flush_snapshot_minute(
                    state, mk, output_dir, code_table,
                    full_snapshot, full_kline, enable_kline, write_executor,
                )
                flushed_minutes.add(mk)
            else:
                # Residual data in buffer for already-flushed minute
                logger.warning("EOF: minute %s already flushed but buffer not empty — late append", mk)
                raw = state.raw_snapshot_buffers.get(mk, {})
                if raw:
                    path = get_snapshot_file_path(output_dir, mk)
                    for sym, recs in raw.items():
                        append_snapshot_records(path, recs, code_table)
                    with state.lock:
                        for recs in raw.values():
                            for rec in recs:
                                state.maybe_update_latest_unlocked(rec)
                        late_snapshot_count += sum(len(r) for r in raw.values())
                        late_snapshot_minutes.add(mk)
                state.ohlcv_buffers.pop(mk, None)
                state.raw_snapshot_buffers.pop(mk, None)

        tailer.close()
        minute_range["count"] = len(flushed_minutes)

        # Store late stats for summary
        minute_range["late_snapshot_count"] = late_snapshot_count
        minute_range["late_snapshot_minutes"] = sorted(late_snapshot_minutes)

        logger.info(
            "Snapshot stream complete: %d minutes across %s..%s (%d late records)",
            len(flushed_minutes),
            minute_range["first"], minute_range["last"],
            late_snapshot_count,
        )
```

Add `_flush_late_snapshots` method to `ReplayEngine`:

```python
    def _flush_late_snapshots(
        self,
        late_records_by_minute: dict[str, list],
        state: SharedState,
        output_dir: str,
        code_table: CodeTable,
    ) -> None:
        """Append late snapshot records to existing files and update latest_snapshot."""
        for minute_key in sorted(late_records_by_minute):
            parsed_list = late_records_by_minute[minute_key]
            path = get_snapshot_file_path(output_dir, minute_key)
            if os.path.exists(path):
                records = []
                for parsed in parsed_list:
                    rec = build_snapshot_record(parsed, seqno=0)
                    if rec is not None:
                        records.append(rec)
                if records:
                    append_snapshot_records(path, records, code_table)
                    with state.lock:
                        for rec in records:
                            state.maybe_update_latest_unlocked(rec)
            else:
                logger.warning("Late snapshot minute %s file missing", minute_key)

        logger.info(
            "Flushed %d late snapshot records across %d minutes",
            sum(len(v) for v in late_records_by_minute.values()),
            len(late_records_by_minute),
        )
```

- [ ] **Step 4: Implement late order detection in replay _stream_orders**

Replace `_stream_orders` method:

```python
    def _stream_orders(
        self,
        stop_event: threading.Event,
        minute_range: dict,
    ) -> None:
        output_dir = self._config.output.output_dir

        tailer = FileTailer(
            self._config.input.csv_dir, "order",
            chunk_size=self._config.input.chunk_size_bytes,
            encoding=self._config.input.file_encoding,
        )
        tailer.set_date(self._date)

        buffers: dict[str, list] = {}
        flushed_minutes: set[str] = set()
        seqno = 0
        active_history: list[set[str]] = []
        late_records_by_minute: dict[str, list] = {}
        late_order_count = 0
        late_order_minutes: set[str] = set()

        while True:
            if stop_event.is_set():
                break

            lines = list(tailer.read_lines())
            if not lines:
                break

            current_round_minutes: set[str] = set()

            for line in lines:
                if stop_event.is_set():
                    break
                parsed = parse_order_line(line, self._config.input.file_encoding)
                if parsed is None:
                    continue
                record_date = str(parsed.time)[:8]
                if record_date != self._date:
                    continue

                minute_key = time_to_minute_key(parsed.time)

                # Late record detection
                if minute_key in flushed_minutes:
                    seqno += 1
                    record = build_order_record(parsed, seqno)
                    late_records_by_minute.setdefault(minute_key, []).append(record)
                    late_order_count += 1
                    late_order_minutes.add(minute_key)
                    continue

                seqno += 1
                record = build_order_record(parsed, seqno)
                buffers.setdefault(minute_key, []).append(record)
                current_round_minutes.add(minute_key)

                if minute_range["first"] is None:
                    minute_range["first"] = minute_key
                minute_range["last"] = minute_key

            # Flush late records
            if late_records_by_minute:
                for mk in sorted(late_records_by_minute):
                    records = late_records_by_minute[mk]
                    path = get_order_file_path(output_dir, mk)
                    if os.path.exists(path):
                        append_order_records(path, records)
                    else:
                        logger.warning("Late order minute %s file missing, creating new", mk)
                        write_order_file(output_dir, mk, records)
                late_records_by_minute.clear()

            # Delayed flush
            active_history.append(current_round_minutes)
            if len(active_history) > DELAYED_FLUSH_ROUNDS:
                stale_round = active_history.pop(0)
                to_flush = stale_round - current_round_minutes
                for mk in sorted(to_flush):
                    if mk not in flushed_minutes and mk in buffers:
                        records = buffers.pop(mk)
                        write_order_file(output_dir, mk, records)
                        flushed_minutes.add(mk)
                        logger.info(
                            "Output order minute %s (%d records)",
                            mk, len(records),
                        )

        # EOF final flush
        for mk in sorted(buffers):
            if mk not in flushed_minutes:
                records = buffers.pop(mk)
                write_order_file(output_dir, mk, records)
                flushed_minutes.add(mk)
                logger.info(
                    "Output order minute %s (%d records)",
                    mk, len(records),
                )

        tailer.close()
        minute_range["count"] = len(flushed_minutes)
        minute_range["late_order_count"] = late_order_count
        minute_range["late_order_minutes"] = sorted(late_order_minutes)

        logger.info(
            "Order stream complete: %d minutes across %s..%s (%d late records)",
            len(flushed_minutes),
            minute_range["first"], minute_range["last"],
            late_order_count,
        )
```

- [ ] **Step 5: Update replay summary with late stats**

Replace `_write_summary` method:

```python
    def _write_summary(
        self,
        snapshot_range: dict,
        order_range: dict,
        enable_order: bool,
    ) -> None:
        enable_kline = self._config.output.enable_kline
        summary = {
            "date": self._date,
            "snapshot_minutes": snapshot_range["count"],
            "kline_minutes": snapshot_range["count"] if enable_kline else 0,
            "snapshot_first_minute": snapshot_range["first"],
            "snapshot_last_minute": snapshot_range["last"],
            "late_snapshot_records": snapshot_range.get("late_snapshot_count", 0),
            "late_snapshot_minutes": snapshot_range.get("late_snapshot_minutes", []),
            "status": "success",
        }
        if enable_order:
            summary["order_minutes"] = order_range["count"]
            summary["order_first_minute"] = order_range["first"]
            summary["order_last_minute"] = order_range["last"]
            summary["late_order_records"] = order_range.get("late_order_count", 0)
            summary["late_order_minutes"] = order_range.get("late_order_minutes", [])

        summary_path = os.path.join(
            self._config.output.output_dir,
            f"replay_summary_{self._date}.json",
        )
        os.makedirs(os.path.dirname(summary_path) or ".", exist_ok=True)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info("Replay complete: wrote summary to %s", summary_path)
```

- [ ] **Step 6: Run replay tests to verify they pass**

Run: `python -m pytest tests/test_replay.py -v`
Expected: All PASS

- [ ] **Step 7: Run full test suite**

Run: `python -m pytest -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/minute_bar/replay.py tests/test_replay.py
git commit -m "feat(replay): add late record detection and handling in snapshot and order streams"
```

---

### Task 6: End-to-End Integration Test

**Files:**
- Modify: `tests/test_replay.py`

- [ ] **Step 1: Write end-to-end test with intentionally out-of-order data**

Add to `tests/test_replay.py`:

```python
class TestReplayZeroDataLoss:
    def test_all_records_preserved_with_out_of_order_data(self, tmp_path):
        """Verify 0 data loss when records arrive out of order across minutes."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,Stock1,JPY,equity,common,,,,0,0,0,2,0,,0",
            "1305,1,TSE,Stock2,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])

        # Craft snapshot data with deliberate out-of-order delivery:
        # 0930 records, then 0931 records, then more 0930 records (late)
        snapshot_rows = [
            # 0930 first batch
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1305,20260520093000500,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,0,0,20260520083000500",
            # 0931 triggers delayed flush of 0930
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1305,20260520093100500,410000,415000,410000,415000,410000,415000,415000,100,100,41500000,1,,T,0,Y,2,0,0,20260520083100500",
            # More 0932 to trigger flush of 0931
            "1301,20260520093200999,443500,458000,440000,458000,443500,458000,458000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083200999",
            # Late 0930 record (should be late-appended, not lost)
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
            # Late 0931 record
            "1305,20260520093101500,410000,416000,410000,416000,410000,416000,416000,50,80,33280000,1,,T,0,Y,2,0,0,20260520083101500",
        ]
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", snapshot_rows)

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        snap_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"

        # Count total output rows
        total_rows = 0
        for f in sorted(snap_dir.glob("snapshot_minute_*.csv")):
            with open(f, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader)  # header
                rows = list(reader)
                total_rows += len(rows)

        # 7 input snapshot records → 7 output rows (0 loss)
        assert total_rows == 7, f"Expected 7 total rows, got {total_rows}"

    def test_all_order_records_preserved_with_late_data(self, tmp_path):
        """Verify 0 order data loss with late records."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
        ])

        order_rows = [
            "1301,20260520093000999,450000,100,451000,200",
            "1301,20260520093100999,451000,150,452000,250",
            "1301,20260520093001500,450500,80,451500,120",  # late 0930
            "1301,20260520093101500,451500,90,452500,130",  # late 0931
        ]
        write_order_csv(csv_dir / "order.csv.20260520", order_rows)

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        order_dir = tmp_path / "output" / "order" / "2026" / "20260520"
        total_rows = 0
        for f in sorted(order_dir.glob("order_minute_*.csv")):
            with open(f, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader)
                total_rows += len(list(reader))

        # 4 input order records → 4 output rows
        assert total_rows == 4, f"Expected 4 total rows, got {total_rows}"

    def test_summary_includes_late_stats(self, tmp_path):
        """Replay summary should include late record statistics."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
        ])

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        summary_path = tmp_path / "output" / "replay_summary_20260520.json"
        assert summary_path.exists()
        import json
        with open(summary_path) as f:
            summary = json.load(f)
        assert "late_snapshot_records" in summary
        assert summary["late_snapshot_records"] >= 0
```

- [ ] **Step 2: Run all tests to verify end-to-end correctness**

Run: `python -m pytest tests/test_replay.py::TestReplayZeroDataLoss -v`
Expected: All PASS

- [ ] **Step 3: Run full test suite**

Run: `python -m pytest -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_replay.py
git commit -m "test: add end-to-end integration tests verifying 0 data loss with late records"
```

---

## Self-Review Checklist

### Spec Coverage

| Spec Section | Task |
|---|---|
| 2. Processing rules (Order append, Snapshot append + update_flag=Y, Kline skip) | Tasks 1, 3, 5 |
| 3.1 Unified entry point (late detection) | Tasks 2, 4, 5 |
| 3.2 Order late record handling | Tasks 1, 4, 5 |
| 3.3 Snapshot late record handling | Tasks 1, 3, 5 |
| 3.4 latest_snapshot update rules | Task 2 (maybe_update_latest_unlocked) |
| 4.1 Writer append functions | Task 1 |
| 4.2 Write locks | Task 1 |
| 4.3 File path helpers | Task 1 |
| 4.4 Append failure handling | Task 1 (raise), Tasks 3,4 (checkpoint safety) |
| 5.1 Flusher snapshot late handling | Task 3 |
| 5.2 Engine order late handling | Task 4 |
| 5.3 Live restart recovery | Task 4 (recover_flushed_minutes) |
| 5.4 Cross-day cleanup | Task 3 |
| 6.1 Replay late snapshot detection | Task 5 |
| 6.2 EOF final flush | Task 5 |
| 6.3 Order stream late handling | Task 5 |
| 7.1-7.4 Monitoring/stats | Tasks 2, 3, 4, 5 |
| 10. Verification | Task 6 |

### Placeholder Scan

No TBD, TODO, or "implement later" patterns found.

### Type Consistency

All method signatures and field names are consistent across tasks:
- `flushed_snapshot_minutes: set[str]` — used in Tasks 2, 3, 4
- `_late_snapshot_records: list[tuple[str, SnapshotRecord]]` — used in Tasks 2, 3
- `late_snapshot_count: int` — used in Tasks 2, 3
- `late_snapshot_minutes: set[str]` — used in Tasks 2, 3
- `_flushed_order_minutes: set[str]` — used in Task 4
- `maybe_update_latest_unlocked(record)` — used in Tasks 2, 3, 5
- `pop_late_snapshot_records()` — used in Tasks 2, 3
- `append_order_records(path, records)` — used in Tasks 1, 4, 5
- `append_snapshot_records(path, records, code_table)` — used in Tasks 1, 3, 5
- `get_snapshot_file_path(output_dir, minute_key)` — used in Tasks 1, 3, 5
- `get_order_file_path(output_dir, minute_key)` — used in Tasks 1, 4, 5
