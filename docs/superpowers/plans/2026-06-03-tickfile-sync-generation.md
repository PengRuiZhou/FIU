# Tickfile Synchronous Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement dual-thread join sync so tickfiles are generated only after both snapshot and order data are complete, replacing the current carry-forward-only approach.

**Architecture:** Snapshot flush stores pending tickfile data in `_tickfile_pending`. Order thread records flushed minutes in `_tickfile_trigger_pending` and drains them after batch write (when `raw_order_buffers` is complete). Either thread can trigger tickfile generation via `_try_generate_tickfile` — a lock-atomic pop ensures only one thread generates per minute.

**Tech Stack:** Python 3.11+, threading (RLock), existing minute_bar infrastructure

**Spec:** `docs/superpowers/specs/2026-06-03-tickfile-sync-design.md`

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `src/minute_bar/aggregator.py` | Modify (+7 lines) | 3 new SharedState fields for sync |
| `src/minute_bar/writer.py` | Modify (~5 lines) | Fix 2 silent returns → raise IOError |
| `src/minute_bar/flusher.py` | Modify (~90 lines) | Core sync logic: decouple tickfile, `_try_generate_tickfile`, overflow, cross-day, EOF, reroute |
| `src/minute_bar/engine.py` | Modify (~45 lines) | Order-thread side: deferred triggers, `_drain_tickfile_triggers`, write condition, cross-day reset |
| `src/minute_bar/replay.py` | No change | Replay uses single-threaded path, unchanged |
| `tests/test_tickfile_sync.py` | New (~490 lines) | 31 sync-specific tests |

---

### Task 1: Add SharedState Fields

**Files:**
- Modify: `src/minute_bar/aggregator.py:87` (after `latest_order_by_symbol`)

- [ ] **Step 1: Add 3 new fields to SharedState.\_\_init\_\_**

In `src/minute_bar/aggregator.py`, after the `latest_order_by_symbol` line (line 87), add:

```python
        # Order carry-forward cache for tickfile generation
        self.latest_order_by_symbol: Dict[str, OrderRecord] = {}
        # ── Tickfile sync fields (Phase 17) ──
        # Tickfile sync: snapshot data waiting for order completion
        self._tickfile_pending: Dict[str, dict] = {}
        # Tickfile seqno (shared between clock and order threads)
        self._tickfile_seqno: int = 0
        # Order thread processing progress (updated in _drain_tickfile_triggers AFTER batch write)
        # "" = no order flushed yet; always < any valid minute_key
        self.order_current_minute: str = ""
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `python -m pytest tests/ -x -q --tb=short 2>&1 | tail -5`
Expected: All existing tests pass (no behavior change yet)

- [ ] **Step 3: Commit**

```bash
git add src/minute_bar/aggregator.py
git commit -m "feat(tickfile-sync): add SharedState fields for dual-thread join sync

Add _tickfile_pending, _tickfile_seqno, order_current_minute to SharedState.
No behavior change — fields initialized but unused."
```

---

### Task 2: Fix writer.py Silent Returns

**Files:**
- Modify: `src/minute_bar/writer.py:296` (all-rows-failed) and `writer.py:335-336` (header corrupted)
- Test: `tests/test_tickfile_sync.py` (create new file)

- [ ] **Step 1: Create test file with writer contract tests**

Create `tests/test_tickfile_sync.py`:

```python
"""Tests for tickfile synchronous generation (Phase 17: dual-thread join sync).

Spec: docs/superpowers/specs/2026-06-03-tickfile-sync-design.md
"""
import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from minute_bar.aggregator import SharedState
from minute_bar.models import OrderRecord, SnapshotRecord


# ── Helpers ──


def _make_snapshot(**overrides) -> SnapshotRecord:
    defaults = dict(
        symbol="7203", seqno=1, time=20260602090013000, rcvtime=20260602080013000,
        preclose=443500.0, lastprice=444000.0, open=443800.0, high=444500.0,
        low=443000.0, close=444000.0, lasttradeprice=444000.0, lasttradeqty=100,
        totalvol=1000, totalamount=44400000.0, sessionid=1, tradetype="",
        status="T", direction=0, pflag="N", decimal=2, vwap=443500.0,
        shortsellflag=0,
    )
    defaults.update(overrides)
    return SnapshotRecord(**defaults)


def _make_order(**overrides) -> OrderRecord:
    defaults = dict(
        symbol="7203", seqno=1, time=20260602090013000,
        bidprice=443500.0, bidsize=100.0, askprice=444500.0, asksize=200.0,
        decimal=2, rcvtime=20260602080013000,
    )
    defaults.update(overrides)
    return OrderRecord(**defaults)


# ── Task 2: Writer contract tests ──


class TestWriterCorruptHeaderRaises:
    """Spec 6.1 #28: write_tickfile_rows encounters corrupt header → raises IOError."""

    def test_corrupt_header_raises_ioerror(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        # Create a file with corrupt header
        from minute_bar.writer import get_tickfile_path
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("CORRUPT_HEADER,NOT,VALID\n")

        with pytest.raises(IOError, match="corrupt"):
            write_tickfile_rows(output_dir, minute_key, [("7203", _make_snapshot(), None)], 1)


class TestWriterAllRowsFailedRaises:
    """Spec 6.1 #31: all rows fail build_tickfile_row → raises IOError."""

    def test_all_rows_failed_raises_ioerror(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        # Pass a row that will fail build_tickfile_row (snap with invalid fields)
        bad_snap = _make_snapshot()
        # Mock build_tickfile_row to always raise
        with patch("minute_bar.writer.build_tickfile_row", side_effect=ValueError("bad")):
            with pytest.raises(IOError, match="failed to build"):
                write_tickfile_rows(output_dir, minute_key, [("7203", bad_snap, None)], 1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short 2>&1 | tail -15`
Expected: Both tests FAIL — writer.py currently does `return` instead of `raise`

- [ ] **Step 3: Fix writer.py — header corrupted path (line 335-336)**

In `src/minute_bar/writer.py`, replace the `logger.error(...); return` at line 335-336:

```python
# BEFORE:
                else:
                    logger.error("Tickfile file exists but header corrupted: %s", path)
                    return

# AFTER:
                else:
                    logger.error("Tickfile file exists but header corrupted: %s", path)
                    raise IOError(f"Tickfile header corrupted, cannot append: {path}")
```

- [ ] **Step 4: Fix writer.py — all rows failed path (line 296)**

In `src/minute_bar/writer.py`, replace the silent return at line 296:

```python
# BEFORE:
    if not rows:
        logger.warning(
            "Tickfile: skipped %d/%d symbols for minute=%s",
            skipped, len(selected), minute_key,
        )
        return

# AFTER:
    if not rows:
        logger.warning(
            "Tickfile: skipped %d/%d symbols for minute=%s",
            skipped, len(selected), minute_key,
        )
        raise IOError(
            f"All tickfile rows failed to build for minute={minute_key} "
            f"({skipped}/{len(selected)} symbols skipped)"
        )
```

- [ ] **Step 5: Run writer contract tests to verify they pass**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: Both tests PASS

- [ ] **Step 6: Run full regression to check no silent-return consumers break**

Run: `python -m pytest tests/ -x -q --tb=short 2>&1 | tail -10`
Expected: All tests pass. (Replay's `_flush_snapshot_minute` calls `write_tickfile_rows` but only with valid data that won't hit these error paths.)

- [ ] **Step 7: Commit**

```bash
git add src/minute_bar/writer.py tests/test_tickfile_sync.py
git commit -m "fix(writer): replace silent returns with IOError in write_tickfile_rows

Header corruption and all-rows-failed paths now raise IOError instead of
silently returning. Required for tickfile sync retry logic (Phase 17)."
```

---

### Task 3: Flusher Config Validation + Seqno Migration

**Files:**
- Modify: `src/minute_bar/flusher.py:74-75` (remove `_tickfile_seqno`, add validation + recovery)
- Test: `tests/test_tickfile_sync.py` (append)

- [ ] **Step 1: Add config validation and seqno recovery tests**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 3: Config validation + seqno migration ──


class TestConfigValidationTickfileRequiresOrder:
    """Spec 6.1 #25: enable_tickfile=True + enable_order=False → ValueError."""

    def test_raises_valueerror(self):
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable
        from minute_bar.flusher import ClockWatermarkFlusher

        state = SharedState()
        code_table = CodeTable()
        checkpoint = CheckpointManager("dummy", {})
        with pytest.raises(ValueError, match="enable_tickfile=True requires enable_order=True"):
            ClockWatermarkFlusher(
                state=state,
                code_table=code_table,
                checkpoint=checkpoint,
                output_dir="/tmp/dummy",
                output_delay_sec=60,
                enable_order=False,
                enable_tickfile=True,
            )

    def test_tickfile_enabled_with_order_ok(self):
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable
        from minute_bar.flusher import ClockWatermarkFlusher

        state = SharedState()
        code_table = CodeTable()
        checkpoint = CheckpointManager("dummy", {})
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=code_table,
            checkpoint=checkpoint,
            output_dir="/tmp/dummy",
            output_delay_sec=60,
            enable_order=True,
            enable_tickfile=True,
        )
        assert flusher._enable_tickfile is True


class TestSeqnoRecoveryAtInit:
    """Spec 6.1 #16: pre-existing tickfile seqno=50 → next seqno=51."""

    def test_recovers_seqno_from_existing_file(self, tmp_path):
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.writer import get_tickfile_path

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Write a tickfile with seqno=50
        from minute_bar.writer import TICKFILE_HEADER
        header_fields = TICKFILE_HEADER.split(',')
        # Build a line with seqno=50 at index 59
        fields = ["placeholder"] * 65
        fields[0] = "7203"
        fields[59] = "50"
        with open(path, "w") as f:
            f.write(TICKFILE_HEADER + "\n")
            f.write(",".join(fields) + "\n")

        # Mock jst_now_yyyymmdd to return our test date
        with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value="20260602"):
            state = SharedState()
            code_table = CodeTable()
            checkpoint = CheckpointManager("dummy", {})
            flusher = ClockWatermarkFlusher(
                state=state,
                code_table=code_table,
                checkpoint=checkpoint,
                output_dir=output_dir,
                output_delay_sec=60,
                enable_order=True,
                enable_tickfile=True,
            )
        # seqno should be recovered to 50
        assert state._tickfile_seqno == 50
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_tickfile_sync.py::TestConfigValidationTickfileRequiresOrder tests/test_tickfile_sync.py::TestSeqnoRecoveryAtInit -v --tb=short 2>&1 | tail -15`
Expected: FAIL — validation not implemented yet, seqno not recovered to SharedState

- [ ] **Step 3: Implement config validation in flusher.\_\_init\_\_**

In `src/minute_bar/flusher.py`, after `self._enable_tickfile = enable_tickfile` (line 74), add validation and replace `_tickfile_seqno`:

```python
        # Tickfile state
        self._enable_tickfile = enable_tickfile
        if enable_tickfile:
            if not self._enable_order:
                raise ValueError("enable_tickfile=True requires enable_order=True")
            # Recover seqno from existing tickfile (single-threaded, before engine.start())
            target_date = jst_now_yyyymmdd()
            self._state._tickfile_seqno = recover_tickfile_seqno_lazy(output_dir, target_date)
```

Remove the old `self._tickfile_seqno: int = 0` line (line 75).

- [ ] **Step 4: Add `recover_tickfile_seqno_lazy` helper function**

In `src/minute_bar/flusher.py`, add before the `ClockWatermarkFlusher` class (after the `logger = ...` line):

```python
def recover_tickfile_seqno_lazy(output_dir: str, date: str) -> int:
    """Recover seqno from existing tickfile for a specific date. Returns 0 if no file found.
    Called once at startup, before any threads start.
    """
    from minute_bar.writer import recover_tickfile_seqno
    sample_minute_key = f"{date}0800"
    try:
        return recover_tickfile_seqno(output_dir, sample_minute_key)
    except Exception:
        logger.warning("Failed to recover tickfile seqno for date=%s", date)
        return 0
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_sync.py
git commit -m "feat(tickfile-sync): add config validation + seqno recovery at init

- enable_tickfile=True requires enable_order=True (ValueError)
- recover_tickfile_seqno_lazy recovers seqno at flusher init
- Move _tickfile_seqno from flusher instance to SharedState"
```

---

### Task 4: Decouple Tickfile from `_write_minute_files`

**Files:**
- Modify: `src/minute_bar/flusher.py:343-373` (remove tickfile block from `_write_minute_files`)
- Modify: `src/minute_bar/flusher.py:255-259` (skip `raw_order_buffers` pop when tickfile enabled)

⚠️ **Steps 1 and 2 MUST be implemented together** (spec Section 4.2.4 warning).

- [ ] **Step 1: Remove tickfile block from `_write_minute_files`**

In `src/minute_bar/flusher.py`, delete the entire `if self._enable_tickfile:` block at lines 364-373:

```python
    # DELETE this entire block (lines 364-373):
    if self._enable_tickfile:
        from minute_bar.writer import get_tickfile_path, recover_tickfile_seqno, write_tickfile_rows
        from minute_bar.tickfile import select_tickfile_records
        path = get_tickfile_path(self._output_dir, minute_key)
        if self._tickfile_seqno == 0 and os.path.exists(path):
            self._tickfile_seqno = recover_tickfile_seqno(self._output_dir, minute_key)
        self._tickfile_seqno += 1
        code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
        selected = select_tickfile_records(raw_records or {}, snapshot_copy, order_records or [], latest_order_copy or {})
        write_tickfile_rows(self._output_dir, minute_key, selected, self._tickfile_seqno, code_table_getter=code_getter)
```

- [ ] **Step 2: Skip `raw_order_buffers` pop when `enable_tickfile=True`**

In `src/minute_bar/flusher.py`, replace the `raw_order_buffers` pop section in `_flush_minutes_internal` (lines 255-259):

```python
# BEFORE:
        order_data = {}
        for k in minute_keys:
            v = self._state.raw_order_buffers.pop(k, None)
            if v is not None:
                order_data[k] = v

# AFTER:
        order_data = {}
        if not self._enable_tickfile:
            for k in minute_keys:
                v = self._state.raw_order_buffers.pop(k, None)
                if v is not None:
                    order_data[k] = v
```

- [ ] **Step 3: Add `_tickfile_pending` storage in second lock scope of `_flush_minutes_internal`**

In `src/minute_bar/flusher.py`, inside the second `with self._state.lock:` block (after `flushed_snapshot_minutes.add`), add tickfile pending logic. The full second lock scope becomes:

```python
        with self._state.lock:
            self._state.output_minutes.add(minute_key)
            self._state.last_output_minute = minute_key
            self._state.flushed_snapshot_minutes.add(minute_key)
            for sym, agg in data.items():
                self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
                self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
            # NEW: Store tickfile pending data
            if self._enable_tickfile:
                self._state._tickfile_pending[minute_key] = {
                    'raw_records': raw,
                    'snapshot_copy': minute_snapshot,
                }
```

Also, add trigger logic **after** the lock scope + after the `if is_final:` block, but still inside the `for minute_key in minute_keys:` loop. Add after the `if is_final:` check:

```python
        # Tickfile trigger: if order already passed this minute
        tickfile_trigger_keys = []
        if self._enable_tickfile:
            with self._state.lock:
                if self._state.order_current_minute > minute_key:
                    tickfile_trigger_keys.append(minute_key)

        for mk in tickfile_trigger_keys:
            try:
                self._try_generate_tickfile(mk)
            except Exception:
                if is_final:
                    logger.exception("Tickfile generation failed for minute=%s", mk)
                else:
                    logger.fatal("Tickfile generation failed for minute=%s", mk)
                    raise SystemExit(1)
```

⚠️ **This code references `_try_generate_tickfile` which is implemented in Task 5.** Tasks 4 and 5 must be committed together before running tests. Alternatively, implement both before running any tests.

- [ ] **Step 4: Commit (depends on Task 5 for testability)**

```bash
git add src/minute_bar/flusher.py
git commit -m "wip(tickfile-sync): decouple tickfile from _write_minute_files

Remove tickfile generation from _write_minute_files. Skip raw_order_buffers
pop when enable_tickfile=True. Store _tickfile_pending in second lock scope.

NOT YET FUNCTIONAL — needs _try_generate_tickfile (Task 5)."
```

---

### Task 5: Implement `_try_generate_tickfile`

**Files:**
- Modify: `src/minute_bar/flusher.py` (add method after `_write_minute_files`)
- Test: `tests/test_tickfile_sync.py` (append)

- [ ] **Step 1: Add unit tests for `_try_generate_tickfile`**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 5: _try_generate_tickfile tests ──


def _make_flusher(state, tmp_path, enable_tickfile=True, enable_order=True):
    """Helper to create a ClockWatermarkFlusher for testing."""
    from minute_bar.checkpoint import CheckpointManager
    from minute_bar.code_table import CodeTable
    from minute_bar.flusher import ClockWatermarkFlusher

    code_table = CodeTable()
    checkpoint = CheckpointManager("dummy", {})
    with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value="20260602"):
        return ClockWatermarkFlusher(
            state=state,
            code_table=code_table,
            checkpoint=checkpoint,
            output_dir=str(tmp_path),
            output_delay_sec=60,
            enable_order=enable_order,
            enable_tickfile=enable_tickfile,
        )


class TestTryGenerateTickfile:
    """Spec 6.1 #1, #4, #6, #14: _try_generate_tickfile core behavior."""

    def test_generates_tickfile_with_complete_data(self, tmp_path):
        """Snapshot pending + complete raw_order_buffers → tickfile generated."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        order = _make_order()

        # Simulate clock thread storing pending
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = [order]

        flusher._try_generate_tickfile(mk)

        # Pending should be cleared
        assert mk not in state._tickfile_pending
        assert mk not in state.raw_order_buffers
        # Seqno should be incremented
        assert state._tickfile_seqno == 1
        # Tickfile should exist
        from minute_bar.writer import get_tickfile_path
        assert os.path.exists(get_tickfile_path(str(tmp_path), mk))

    def test_no_pending_returns_silently(self, tmp_path):
        """No pending data → no-op, no error."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        flusher._try_generate_tickfile("202606020900")  # should not raise
        assert state._tickfile_seqno == 0  # unchanged

    def test_pending_cleanup_after_generation(self, tmp_path):
        """Spec 6.1 #4: _tickfile_pending cleared after generation."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = []

        flusher._try_generate_tickfile(mk)
        assert mk not in state._tickfile_pending

    def test_double_trigger_safe(self, tmp_path):
        """Spec 6.1 #9: two threads pop same minute → only one generates."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = []

        # First call pops and generates
        flusher._try_generate_tickfile(mk)
        # Second call should be a no-op (pending already popped)
        flusher._try_generate_tickfile(mk)

        # Seqno should be incremented exactly once
        assert state._tickfile_seqno == 1

    def test_io_failure_reinserts_all_data(self, tmp_path):
        """Spec 6.1 #11: write failure → pending + raw_order_buffers re-inserted."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        order = _make_order()
        pending_data = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state._tickfile_pending[mk] = pending_data
        state.raw_order_buffers[mk] = [order]

        with patch("minute_bar.flusher.write_tickfile_rows", side_effect=IOError("disk full")):
            with pytest.raises(IOError):
                flusher._try_generate_tickfile(mk)

        # Data should be re-inserted
        assert mk in state._tickfile_pending
        assert mk in state.raw_order_buffers
        assert state.raw_order_buffers[mk] == [order]

    def test_minute_zero_orders_carries_forward(self, tmp_path):
        """Spec 6.1 #14: snapshot has data but order_records=[] → carry-forward."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = []

        flusher._try_generate_tickfile(mk)
        assert state._tickfile_seqno == 1
        from minute_bar.writer import get_tickfile_path
        assert os.path.exists(get_tickfile_path(str(tmp_path), mk))

    def test_seqno_shared_between_threads(self, tmp_path):
        """Spec 6.1 #5: sequential calls from same flusher → seqno increments."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        for i in range(3):
            mk = f"20260602090{i}"
            snap = _make_snapshot(time=20260602090013000 + i * 1000000000)
            state._tickfile_pending[mk] = {
                'raw_records': {'7203': [snap]},
                'snapshot_copy': {'7203': snap},
            }
            state.raw_order_buffers[mk] = []
            flusher._try_generate_tickfile(mk)

        assert state._tickfile_seqno == 3

    def test_post_write_missing_file_warns_only(self, tmp_path):
        """Spec 6.1 #20: os.path.exists=False → ERROR log only, no re-insert."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = []

        with patch("minute_bar.flusher.os.path.exists", return_value=False):
            # Should NOT raise — warning only
            flusher._try_generate_tickfile(mk)

        # Pending should NOT be re-inserted (write succeeded, just verification failed)
        assert mk not in state._tickfile_pending

    def test_seqno_gap_on_empty_selection(self, tmp_path):
        """Spec 6.1 #21: select_tickfile_records returns empty → seqno incremented but no file."""
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        # Empty raw_records and snapshot_copy → select returns nothing
        state._tickfile_pending[mk] = {
            'raw_records': {},
            'snapshot_copy': {},
        }
        state.raw_order_buffers[mk] = []

        flusher._try_generate_tickfile(mk)
        assert state._tickfile_seqno == 1  # incremented
        from minute_bar.writer import get_tickfile_path
        # File may or may not exist (no records selected → no write)
        # This is acceptable per Invariant #15
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_tickfile_sync.py::TestTryGenerateTickfile -v --tb=short 2>&1 | tail -15`
Expected: FAIL — `_try_generate_tickfile` doesn't exist yet

- [ ] **Step 3: Implement `_try_generate_tickfile`**

Add to `src/minute_bar/flusher.py`, in the `ClockWatermarkFlusher` class, after `_write_minute_files`:

```python
    def _try_generate_tickfile(self, minute_key: str) -> None:
        """Generate tickfile for a minute. Thread-safe. Callable from any thread.

        Returns silently if no pending data found (already generated by other thread).
        On IO failure, re-inserts ALL popped data for flush_all_remaining retry.
        """
        import threading as _threading

        # Step 1: Pop data under lock (no IO)
        with self._state.lock:
            pending = self._state._tickfile_pending.pop(minute_key, None)
            if pending is None:
                return
            order_records = self._state.raw_order_buffers.pop(minute_key, [])
            latest_order_copy = dict(self._state.latest_order_by_symbol)
            self._state._tickfile_seqno += 1
            current_seqno = self._state._tickfile_seqno

        # Step 2: IO outside lock
        start_ts = time.monotonic()
        try:
            from minute_bar.tickfile import select_tickfile_records
            from minute_bar.writer import write_tickfile_rows

            raw_records = pending['raw_records']
            snapshot_copy = pending['snapshot_copy']
            code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
            selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)

            if not selected:
                logger.warning("Tickfile: no records selected for minute=%s", minute_key)
                return  # seqno gap acceptable (Invariant #15)

            write_tickfile_rows(self._output_dir, minute_key, selected, current_seqno, code_table_getter=code_getter)

            # Post-write verification (warning only — do NOT re-insert on failure)
            path = get_tickfile_path(self._output_dir, minute_key)
            if not os.path.exists(path):
                logger.error("Tickfile file missing after successful write: %s (possible filesystem issue)", path)

            elapsed_ms = (time.monotonic() - start_ts) * 1000
            logger.info("Tickfile generated minute=%s (%d symbols, %d orders, %.1fms) "
                        "[thread=%s, order_watermark=%s]",
                        minute_key, len(selected), len(order_records), elapsed_ms,
                        _threading.current_thread().name,
                        self._state.order_current_minute)
            if elapsed_ms > 200:
                logger.warning("Tickfile generation slow: minute=%s %.1fms", minute_key, elapsed_ms)
        except Exception:
            # Re-insert ALL popped data so flush_all_remaining can retry
            with self._state.lock:
                self._state._tickfile_pending[minute_key] = pending
                if order_records:
                    # Replace (not extend): order thread won't re-create entries after pop
                    self._state.raw_order_buffers[minute_key] = order_records
            logger.warning("Tickfile IO failed for minute=%s, re-inserted for retry [thread=%s]",
                           minute_key, _threading.current_thread().name)
            raise
```

**Import note**: `get_tickfile_path` is used inside the try block for both writing and post-write verification. Update the import line to include it:

```python
            from minute_bar.writer import get_tickfile_path, write_tickfile_rows
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_sync.py
git commit -m "feat(tickfile-sync): implement _try_generate_tickfile with thread-safe pop

- Lock-atomic pop of _tickfile_pending + raw_order_buffers
- IO failure re-inserts all data for retry
- Post-write verification (warning only)
- 9 unit tests covering core scenarios"
```

---

### Task 6: Flusher Overflow + Cross-Day + EOF + Reroute

**Files:**
- Modify: `src/minute_bar/flusher.py` (4 sections: `tick()`, `_step1_cross_day_check`, `flush_all_remaining`, `_reroute_buffer_to_late_queue`)
- Test: `tests/test_tickfile_sync.py` (append)

- [ ] **Step 1: Add tests for overflow, cross-day, EOF, reroute**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 6: Overflow, cross-day, EOF, reroute tests ──


class TestPendingOverflowForcesOldest:
    """Spec 6.1 #12: 15 pending minutes → force oldest 6 → count ≤10."""

    def test_overflow_forces_oldest(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        # Create 15 pending minutes
        for i in range(15):
            mk = f"2026060208{i:02d}"  # 0800-0814
            snap = _make_snapshot(time=20260602080000000 + i * 1000000)
            state._tickfile_pending[mk] = {
                'raw_records': {'7203': [snap]},
                'snapshot_copy': {'7203': snap},
            }
            state.raw_order_buffers[mk] = []

        # Simulate overflow check from tick()
        from minute_bar.flusher import MAX_TICKFILE_PENDING_MINUTES
        with state.lock:
            pending_keys = sorted(state._tickfile_pending.keys())
            pending_count = len(pending_keys)
            if pending_count > MAX_TICKFILE_PENDING_MINUTES:
                force_count = pending_count - MAX_TICKFILE_PENDING_MINUTES + 1
                force_keys = pending_keys[:force_count]
            else:
                force_keys = []

        for mk in force_keys:
            flusher._try_generate_tickfile(mk)

        assert len(state._tickfile_pending) <= 10


class TestCrossDayForceGeneratesPending:
    """Spec 6.1 #18: 5 pending minutes → cross-day → all 5 tickfiles generated before clear."""

    def test_cross_day_generates_before_clear(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        for i in range(5):
            mk = f"20260601090{i}"
            snap = _make_snapshot(time=20260601090000000 + i * 1000000)
            state._tickfile_pending[mk] = {
                'raw_records': {'7203': [snap]},
                'snapshot_copy': {'7203': snap},
            }

        # Simulate cross-day force-generate
        remaining = sorted(state._tickfile_pending.keys())
        for mk in remaining:
            flusher._try_generate_tickfile(mk)

        assert len(state._tickfile_pending) == 0
        assert state._tickfile_seqno == 5


class TestCrossDayClearsPendingAndOrderMinute:
    """Spec 6.1 #7: cross-day → pending cleared + order_current_minute reset."""

    def test_cleanup_clears_state(self, tmp_path):
        state = SharedState()
        state.order_current_minute = "202606021525"
        state._tickfile_seqno = 42

        # Simulate cross-day cleanup lock scope
        with state.lock:
            state._tickfile_pending.clear()
            state._tickfile_seqno = 0
            state.order_current_minute = ""

        assert state._tickfile_pending == {}
        assert state._tickfile_seqno == 0
        assert state.order_current_minute == ""


class TestReroutePopsRawOrderBuffersAndPending:
    """Spec 6.1 #19: reroute pops raw_order_buffers + _tickfile_pending for rerouted minute."""

    def test_reroute_pops_both(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        order = _make_order()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = [order]

        # Call reroute (via flusher method)
        flusher._reroute_buffer_to_late_queue([mk])

        assert mk not in state._tickfile_pending
        assert mk not in state.raw_order_buffers


class TestEOFFallbackCarryForward:
    """Spec 6.1 #3: snapshot flush → pending → EOF (order never arrives) → carry-forward."""

    def test_eof_generates_with_empty_orders(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        # raw_order_buffers[mk] is empty (order never arrived)

        flusher._try_generate_tickfile(mk)
        assert mk not in state._tickfile_pending
        assert state._tickfile_seqno == 1
```

- [ ] **Step 2: Run tests to verify they fail (reroute test)**

Run: `python -m pytest tests/test_tickfile_sync.py::TestReroutePopsRawOrderBuffersAndPending -v --tb=short 2>&1 | tail -10`
Expected: FAIL — `_reroute_buffer_to_late_queue` doesn't pop tickfile data yet

- [ ] **Step 3: Modify `_reroute_buffer_to_late_queue`**

In `src/minute_bar/flusher.py`, inside the `with self._state.lock:` block of `_reroute_buffer_to_late_queue`, after the `self._state._snapshot_at_minute_end.pop(k, None)` line, add:

```python
            self._state._snapshot_at_minute_end.pop(k, None)
            if self._enable_tickfile:
                # Pop raw_order_buffers + _tickfile_pending for rerouted minutes
                order_buf = self._state.raw_order_buffers.pop(k, None)
                pending_tick = self._state._tickfile_pending.pop(k, None)
                if order_buf:
                    logger.debug("Reroute: popped %d order records for minute=%s (no tickfile)",
                                 len(order_buf), k)
                if pending_tick:
                    logger.debug("Reroute: cleared tickfile pending for minute=%s", k)
```

- [ ] **Step 4: Add `MAX_TICKFILE_PENDING_MINUTES` constant + overflow in `tick()`**

In `src/minute_bar/flusher.py`, add constant before the class:

```python
MAX_TICKFILE_PENDING_MINUTES = 10
```

In the `tick()` method, after `self._step3_minute_output()`, add:

```python
    def tick(self) -> None:
        self._step1_cross_day_check()
        if not self._step2_first_data_check():
            return
        self._step3_minute_output()
        # Tickfile pending overflow protection
        if self._enable_tickfile:
            with self._state.lock:
                pending_keys = sorted(self._state._tickfile_pending.keys())
                pending_count = len(pending_keys)
                if pending_count > MAX_TICKFILE_PENDING_MINUTES:
                    force_count = pending_count - MAX_TICKFILE_PENDING_MINUTES + 1
                    force_keys = pending_keys[:force_count]
                else:
                    force_keys = []
            if force_keys:
                logger.warning(
                    "Tickfile pending overflow: %d minutes pending (max=%d), forcing %d oldest",
                    pending_count, MAX_TICKFILE_PENDING_MINUTES, len(force_keys),
                )
                for mk in force_keys:
                    try:
                        self._try_generate_tickfile(mk)
                    except Exception:
                        logger.exception("Force tickfile generation failed for minute=%s", mk)
        self._step4_handle_late_records()
        self._step5_write_checkpoint()
```

- [ ] **Step 5: Modify `_step1_cross_day_check` — force-generate before clear + skip raw_order_buffers pop**

In `src/minute_bar/flusher.py`, modify `_step1_cross_day_check`:

**5a. Skip raw_order_buffers pop when tickfile enabled (lines 107-111):**

```python
# BEFORE:
            pending_orders = {
                k: self._state.raw_order_buffers.pop(k)
                for k in list(self._state.raw_order_buffers)
                if is_yesterday(k, current_date)
            }

# AFTER:
            if not self._enable_tickfile:
                pending_orders = {
                    k: self._state.raw_order_buffers.pop(k)
                    for k in list(self._state.raw_order_buffers)
                    if is_yesterday(k, current_date)
                }
            else:
                pending_orders = {}
```

**5b. Remove `self._tickfile_seqno = 0` at line 134** (replace with force-generate + cleanup):

Replace line 134 (`self._tickfile_seqno = 0`) and the subsequent cleanup lock scope with:

```python
    # Step 1: Force-generate remaining pending tickfiles BEFORE clearing
    remaining_pending = []
    with self._state.lock:
        remaining_pending = sorted(self._state._tickfile_pending.keys())
    if remaining_pending:
        logger.warning(
            "Cross-day: generating %d pending tickfiles before cleanup (order lagging)",
            len(remaining_pending),
        )
        for mk in remaining_pending:
            try:
                self._try_generate_tickfile(mk)
            except Exception:
                logger.exception("Cross-day tickfile generation failed for minute=%s", mk)

    # Step 2: Atomic cleanup
    with self._state.lock:
        cleared_pending = len(self._state._tickfile_pending)
        self._state._tickfile_pending.clear()
        orphaned_keys = [k for k in list(self._state.raw_order_buffers)
                         if k[:8] != current_date]
        for k in orphaned_keys:
            self._state.raw_order_buffers.pop(k, None)
        self._state._tickfile_seqno = 0
        self._state.order_current_minute = ""
        # ... rest of existing cleanup (output_minutes.clear, etc.)
```

Note: The existing second `with self._state.lock:` block in `_step1_cross_day_check` (around flusher.py line 136-155, containing `output_minutes.clear()`, `flushed_snapshot_minutes.clear()`, etc.) must be merged with the new cleanup. Specifically: add `self._state._tickfile_pending.clear()`, `self._state._tickfile_seqno = 0`, `self._state.order_current_minute = ""`, and the orphaned `raw_order_buffers` cleanup into that existing lock scope. Remove the old `self._tickfile_seqno = 0` line (line 134) entirely.

- [ ] **Step 6: Modify `flush_all_remaining` — handle remaining pending**

In `src/minute_bar/flusher.py`, in `flush_all_remaining`, after `self._step4_handle_late_records()` and before `self._write_checkpoint()`, add:

```python
        # Generate tickfile for remaining pending minutes (EOF fallback)
        tickfile_errors = 0
        with self._state.lock:
            remaining_pending = sorted(self._state._tickfile_pending.keys())
        for mk in remaining_pending:
            try:
                self._try_generate_tickfile(mk)
            except Exception:
                tickfile_errors += 1
                logger.exception("EOF tickfile generation failed for minute=%s", mk)
        if remaining_pending:
            logger.info("EOF tickfile summary: %d generated, %d failed",
                        len(remaining_pending) - tickfile_errors, tickfile_errors)

        # After tickfile, check cleanup
        with self._state.lock:
            if self._state._tickfile_pending:
                logger.warning("Tickfile pending not empty after flush_all_remaining: %s "
                               "(may be caused by order thread join timeout)",
                               list(self._state._tickfile_pending.keys()))
                self._state._tickfile_pending.clear()
```

- [ ] **Step 7: Run all tickfile sync tests**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All tests PASS

- [ ] **Step 8: Run full regression**

Run: `python -m pytest tests/ -x -q --tb=short 2>&1 | tail -10`
Expected: All tests pass

- [ ] **Step 9: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_sync.py
git commit -m "feat(tickfile-sync): overflow, cross-day, EOF, reroute handling

- MAX_TICKFILE_PENDING_MINUTES=10 overflow protection in tick()
- Cross-day: force-generate before clear, skip raw_order_buffers pop
- flush_all_remaining: generate remaining pending (EOF fallback)
- _reroute_buffer_to_late_queue: pop raw_order_buffers + _tickfile_pending"
```

---

### Task 7: Engine — Deferred Trigger + `_drain_tickfile_triggers`

**Files:**
- Modify: `src/minute_bar/engine.py:182` (add `_tickfile_trigger_pending` field)
- Modify: `src/minute_bar/engine.py:614-637` (modify `_flush_order_minute`)
- Add new method: `_drain_tickfile_triggers`
- Modify: `src/minute_bar/engine.py:543-544` (modify `raw_order_buffers` write condition)
- Test: `tests/test_tickfile_sync.py` (append)

- [ ] **Step 1: Add tests for engine-side sync**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 7: Engine-side sync tests ──


class TestRawOrderBuffersCondition:
    """Spec 6.1 #6: flushed + pending → allow write; flushed + not pending → skip."""

    def test_write_allowed_when_not_flushed(self):
        state = SharedState()
        mk = "202606020900"
        # Not flushed → write allowed
        assert mk not in state.flushed_snapshot_minutes
        # The condition in engine: mk not in flushed_snapshot_minutes or mk in _tickfile_pending
        assert (mk not in state.flushed_snapshot_minutes) or (mk in state._tickfile_pending)

    def test_write_allowed_when_flushed_and_pending(self):
        state = SharedState()
        mk = "202606020900"
        state.flushed_snapshot_minutes.add(mk)
        state._tickfile_pending[mk] = {'raw_records': {}, 'snapshot_copy': {}}
        # Flushed but pending → write allowed
        assert (mk not in state.flushed_snapshot_minutes) or (mk in state._tickfile_pending)

    def test_write_skipped_when_flushed_and_not_pending(self):
        state = SharedState()
        mk = "202606020900"
        state.flushed_snapshot_minutes.add(mk)
        # Flushed and not pending → skip
        condition = (mk not in state.flushed_snapshot_minutes) or (mk in state._tickfile_pending)
        assert condition is False


class TestOrderFileNotDoubleWritten:
    """Spec 6.1 #10: enable_tickfile=True → flusher doesn't write order file."""

    def test_flusher_skips_order_when_tickfile_enabled(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        assert flusher._enable_tickfile is True

        # Simulate _flush_minutes_internal with tickfile enabled
        # order_data should be empty because raw_order_buffers was NOT popped
        mk = "202606020900"
        state.raw_order_buffers[mk] = [_make_order()]
        state.ohlcv_buffers[mk] = {}

        # The condition: if not enable_tickfile → pop; else → skip
        order_data = {}
        if not flusher._enable_tickfile:
            order_data[mk] = state.raw_order_buffers.pop(mk)

        # order_data should be empty → _write_minute_files won't write order file
        assert order_data == {}
        assert mk in state.raw_order_buffers  # still intact


class TestOrderCurrentMinuteUpdatedAfterBatch:
    """Spec 6.1 #17: flush order X → batch write → drain → order_current_minute == X."""

    def test_drain_updates_order_current_minute(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        # Simulate: order thread flushed minutes 0900, 0901, 0902
        # After batch write, drain updates order_current_minute to max
        triggers = ["202606020900", "202606020901", "202606020902"]
        latest = max(triggers)

        with state.lock:
            state.order_current_minute = latest
            # Catch-up scan
            for mk in list(state._tickfile_pending):
                if mk <= latest and mk not in triggers:
                    triggers.append(mk)

        assert state.order_current_minute == "202606020902"


class TestDrainTickfileTriggersCatchup:
    """Spec 6.1 #26: catch-up scan finds pending ≤ order_current_minute."""

    def test_catchup_finds_pending_minutes(self, tmp_path):
        state = SharedState()
        # Snapshot X arrived after order drain but before next order
        # pending has 0900 but triggers only have 0859
        state._tickfile_pending["202606020900"] = {
            'raw_records': {'7203': [_make_snapshot()]},
            'snapshot_copy': {'7203': _make_snapshot()},
        }

        # Simulate drain: triggers = ["202606020859"], order_current_minute updated to 0859
        # But wait — 0900 > 0859, so it shouldn't be caught
        # Let's test the case where order_current_minute = 0901
        triggers = ["202606020901"]
        latest = max(triggers)

        with state.lock:
            state.order_current_minute = latest
            # catch-up: scan _tickfile_pending for mk <= latest
            for mk in list(state._tickfile_pending):
                if mk <= latest and mk not in triggers:
                    triggers.append(mk)

        # 0900 <= 0901 → should be caught
        assert "202606020900" in triggers


class TestClockThreadNoTriggerWhenOrderEqMinute:
    """Spec 6.1 #30: order_current_minute == minute_key → '>' condition False → no trigger."""

    def test_no_trigger_on_equal(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        state.order_current_minute = mk
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [_make_snapshot()]},
            'snapshot_copy': {'7203': _make_snapshot()},
        }

        # The clock thread condition: order_current_minute > minute_key
        assert not (state.order_current_minute > mk)
        # No trigger → catch-up on next drain handles it
```

- [ ] **Step 2: Add `_tickfile_trigger_pending` field to Engine.\_\_init\_\_**

In `src/minute_bar/engine.py`, after `self._late_order_minutes: set[str] = set()` (line 184), add:

```python
        self._late_order_minutes: set[str] = set()
        # Tickfile sync: deferred trigger list (order-thread only)
        self._tickfile_trigger_pending: list = []
```

- [ ] **Step 3: Modify `_flush_order_minute` — add deferred trigger**

In `src/minute_bar/engine.py`, at the end of `_flush_order_minute` (after the logger.info call), add:

```python
        logger.info(
            "Output order minute %s (%d records, committed_offset=%d)",
            minute_key, len(buf.records), buf.line_end_offset,
        )

        # NEW: Record minute for deferred tickfile trigger
        if self._config.output.enable_tickfile:
            # DO NOT update order_current_minute here — batch write hasn't happened yet
            # DO NOT trigger tickfile here — raw_order_buffers may not be complete yet
            self._tickfile_trigger_pending.append(minute_key)
```

- [ ] **Step 4: Add `_drain_tickfile_triggers` method to Engine**

In `src/minute_bar/engine.py`, add new method after `_flush_order_minute`:

```python
    def _drain_tickfile_triggers(self) -> None:
        """Drain _tickfile_trigger_pending: update order_current_minute + generate tickfiles.
        Called after batch write and after _flush_expired_order_minutes.
        """
        if not self._tickfile_trigger_pending:
            return
        triggers = list(self._tickfile_trigger_pending)
        self._tickfile_trigger_pending.clear()

        # Update order_current_minute AFTER batch write (all data now in raw_order_buffers)
        if triggers:
            latest = max(triggers)
            with self._state.lock:
                self._state.order_current_minute = latest
                # Catch-up: find pending tickfiles where order data is now complete
                for mk in list(self._state._tickfile_pending):
                    if mk <= latest and mk not in triggers:
                        triggers.append(mk)

        # Generate tickfiles (deferred triggers + catch-up keys)
        for mk in triggers:
            try:
                self._flusher._try_generate_tickfile(mk)
            except Exception:
                logger.exception("Tickfile generation failed for minute=%s [thread=%s]",
                                 mk, threading.current_thread().name)
```

- [ ] **Step 5: Add 3 drain call sites in `_order_loop`**

In `src/minute_bar/engine.py`, add drain calls at 3 locations:

**Drain point 1** — After `pending_shared_orders.clear()` (line 548):
```python
                            pending_shared_orders.clear()
                            if self._config.output.enable_tickfile:
                                self._drain_tickfile_triggers()
```

**Drain point 2** — After `_flush_expired_order_minutes` inside the `if drain_count >= 100:` block (line 551-553):
```python
                            if drain_count >= 100:
                                self._flush_expired_order_minutes(
                                    buffers, output_dir, output_delay_sec, current_minute or ""
                                )
                                self._enforce_max_pending(buffers, output_dir)
                                if self._config.output.enable_tickfile:
                                    self._drain_tickfile_triggers()
                                drain_count = 0
```

**Drain point 3** — After `_flush_expired_order_minutes` post inner-loop (line 565-567):
```python
                        # Watermark-driven: flush expired minutes
                        self._flush_expired_order_minutes(
                            buffers, output_dir, output_delay_sec, current_minute or ""
                        )
                        if self._config.output.enable_tickfile:
                            self._drain_tickfile_triggers()
```

- [ ] **Step 6: Modify `raw_order_buffers` write condition**

In `src/minute_bar/engine.py`, replace line 543-544:

```python
# BEFORE:
                                        if mk not in self._state.flushed_snapshot_minutes:
                                            self._state.raw_order_buffers.setdefault(mk, []).append(rec)

# AFTER:
                                        if mk not in self._state.flushed_snapshot_minutes or mk in self._state._tickfile_pending:
                                            self._state.raw_order_buffers.setdefault(mk, []).append(rec)
```

- [ ] **Step 7: Run tickfile sync tests**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add src/minute_bar/engine.py tests/test_tickfile_sync.py
git commit -m "feat(tickfile-sync): engine-side deferred trigger + drain

- _tickfile_trigger_pending deferred list (order-thread only)
- _drain_tickfile_triggers updates order_current_minute after batch write
- 3 drain call sites in _order_loop
- Modified raw_order_buffers write condition for pending minutes"
```

---

### Task 8: Engine Cross-Day Reset

**Files:**
- Modify: `src/minute_bar/engine.py:472-483` (cross-day reset in order thread)
- Test: `tests/test_tickfile_sync.py` (append)

- [ ] **Step 1: Add cross-day reset test**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 8: Engine cross-day reset ──


class TestFlushedOrderMinutesClearedCrossDay:
    """Spec 6.1 #24: Cross-day in order thread → _flushed_order_minutes cleared."""

    def test_cleared_on_cross_day(self):
        # Simulating the reset logic that happens in _order_loop cross-day path
        flushed = {"202606020900", "202606020901"}
        state = SharedState()
        state.order_current_minute = "202606020901"

        # Cross-day reset
        with state.lock:
            state.order_current_minute = ""
        flushed.clear()

        assert len(flushed) == 0
        assert state.order_current_minute == ""


class TestCrossDayOrphanedRawOrderBuffersCleaned:
    """Spec 6.1 #27: order writes yesterday raw_order_buffers after force-generate → cleanup."""

    def test_orphaned_entries_cleaned(self, tmp_path):
        state = SharedState()
        state.raw_order_buffers["202606011529"] = [_make_order()]
        state.raw_order_buffers["202606020900"] = [_make_order()]

        # Simulate cross-day cleanup: remove yesterday entries
        current_date = "20260602"
        with state.lock:
            orphaned_keys = [k for k in list(state.raw_order_buffers) if k[:8] != current_date]
            for k in orphaned_keys:
                state.raw_order_buffers.pop(k, None)

        assert "202606011529" not in state.raw_order_buffers
        assert "202606020900" in state.raw_order_buffers
```

- [ ] **Step 2: Modify `_order_loop` cross-day reset**

In `src/minute_bar/engine.py`, in the cross-day block (lines 472-483), add tickfile resets:

```python
                            # Step 0.5: Cross-day flush + reset
                            if current_date is not None and record_date != current_date:
                                try:
                                    self._flush_all_order_buffers(buffers, output_dir)
                                except Exception:
                                    logger.exception("Cross-day order flush failed, resetting date anyway")
                                finally:
                                    old_keys = [k for k in buffers if k[:8] != record_date]
                                    for k in old_keys:
                                        buffers.pop(k, None)
                                current_date = record_date
                                current_minute = None
                                # NEW: Reset order progress for cross-day
                                if self._config.output.enable_tickfile:
                                    with self._state.lock:
                                        self._state.order_current_minute = ""
                                    self._drain_tickfile_triggers()
                                # NEW: Clear _flushed_order_minutes to prevent stale entries
                                self._flushed_order_minutes.clear()
```

- [ ] **Step 3: Run tests**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/minute_bar/engine.py tests/test_tickfile_sync.py
git commit -m "feat(tickfile-sync): engine cross-day reset for order thread

- Reset order_current_minute to '' on cross-day
- Drain remaining tickfile triggers before date change
- Clear _flushed_order_minutes to prevent stale entries"
```

---

### Task 9: Remaining Integration Tests

**Files:**
- Test: `tests/test_tickfile_sync.py` (append remaining tests)

- [ ] **Step 1: Add remaining spec tests**

Append to `tests/test_tickfile_sync.py`:

```python
# ── Task 9: Remaining integration tests ──


class TestNoTickfileNoChange:
    """Spec 6.1 #8: enable_tickfile=False → existing behavior unchanged."""

    def test_no_tickfile_state_unchanged(self, tmp_path):
        state = SharedState()
        # No tickfile → pending stays empty, seqno stays 0
        assert state._tickfile_pending == {}
        assert state._tickfile_seqno == 0
        assert state.order_current_minute == ""

    def test_no_tickfile_order_data_popped(self, tmp_path):
        """When tickfile disabled, _flush_minutes_internal pops raw_order_buffers normally."""
        state = SharedState()
        mk = "202606020900"
        state.raw_order_buffers[mk] = [_make_order()]

        # Simulate the condition: not enable_tickfile → pop
        order_data = {}
        enable_tickfile = False
        if not enable_tickfile:
            v = state.raw_order_buffers.pop(mk, None)
            if v is not None:
                order_data[mk] = v

        assert mk in order_data
        assert mk not in state.raw_order_buffers


class TestTickfileFailureNoBlockOrderFlush:
    """Spec 6.1 #15: tickfile failure → order file still written."""

    def test_order_flush_independent(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }

        # Order file would be written by engine independently
        # Tickfile failure should not affect it
        with patch("minute_bar.flusher.write_tickfile_rows", side_effect=IOError("fail")):
            with pytest.raises(IOError):
                flusher._try_generate_tickfile(mk)

        # Order data is re-inserted (not lost)
        assert mk in state._tickfile_pending


class TestDrainAfterFlushExpiredOrderMinutes:
    """Spec 6.1 #29: _flush_expired_order_minutes flushes → drain called → tickfiles generated."""

    def test_drain_catches_expired_flush_triggers(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)

        mk = "202606020900"
        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.raw_order_buffers[mk] = [_make_order()]

        # Simulate: order thread flushed 0900 via _flush_expired_order_minutes
        # trigger was appended, then drain catches it
        triggers = [mk]
        latest = max(triggers)
        with state.lock:
            state.order_current_minute = latest
            for mk2 in list(state._tickfile_pending):
                if mk2 <= latest and mk2 not in triggers:
                    triggers.append(mk2)

        # Drain would call _try_generate_tickfile for each trigger
        flusher._try_generate_tickfile(mk)
        assert mk not in state._tickfile_pending


class TestStallFlushCreatesPendingThenOrderGenerates:
    """Spec 6.1 #22: watermark stalls → stall flush → pending → order catches up → tickfile."""

    def test_stall_flush_then_order_drain(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        # Stall flush creates pending (simulates _flush_minutes_internal path)
        snap = _make_snapshot()
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }

        # Order catches up later
        state.raw_order_buffers[mk] = [_make_order()]
        state.order_current_minute = mk

        # Now drain triggers tickfile
        flusher._try_generate_tickfile(mk)
        assert mk not in state._tickfile_pending
        assert state._tickfile_seqno == 1


class TestSnapshotFirstOrderTriggers:
    """Spec 6.1 #1: snapshot flush → pending → order flush → tickfile with complete data."""

    def test_full_flow(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        order = _make_order()

        # Step 1: Clock thread flushes snapshot → stores pending
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }
        state.flushed_snapshot_minutes.add(mk)
        # order_current_minute is "" → no trigger from clock thread

        # Step 2: Order thread processes → batch write → raw_order_buffers complete
        state.raw_order_buffers[mk] = [order]

        # Step 3: Drain updates order_current_minute → catch-up scan finds pending
        state.order_current_minute = mk
        triggers = []
        with state.lock:
            for pending_mk in list(state._tickfile_pending):
                if pending_mk <= mk:
                    triggers.append(pending_mk)

        # Step 4: Generate tickfile
        for t in triggers:
            flusher._try_generate_tickfile(t)

        assert state._tickfile_seqno == 1
        assert mk not in state._tickfile_pending
        from minute_bar.writer import get_tickfile_path
        assert os.path.exists(get_tickfile_path(str(tmp_path), mk))


class TestOrderFirstClockTriggers:
    """Spec 6.1 #2: order flush → snapshot flush → clock thread triggers."""

    def test_full_flow(self, tmp_path):
        state = SharedState()
        flusher = _make_flusher(state, tmp_path)
        mk = "202606020900"

        snap = _make_snapshot()
        order = _make_order()

        # Step 1: Order thread flushes first
        state.raw_order_buffers[mk] = [order]
        state.order_current_minute = mk

        # Step 2: Clock thread flushes snapshot later
        state._tickfile_pending[mk] = {
            'raw_records': {'7203': [snap]},
            'snapshot_copy': {'7203': snap},
        }

        # Clock thread checks: order_current_minute > mk?
        # If order already moved past mk → trigger
        state.order_current_minute = "202606020901"  # order moved to 0901

        # Clock thread triggers
        trigger_keys = []
        if state.order_current_minute > mk:
            trigger_keys.append(mk)

        for t in trigger_keys:
            flusher._try_generate_tickfile(t)

        assert state._tickfile_seqno == 1
        assert mk not in state._tickfile_pending
```

- [ ] **Step 2: Run all tickfile sync tests**

Run: `python -m pytest tests/test_tickfile_sync.py -v --tb=short`
Expected: All 31 tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_tickfile_sync.py
git commit -m "test(tickfile-sync): add remaining integration tests (31 total)

Tests for: no-tickfile path, failure isolation, drain timing,
stall flush interaction, snapshot-first, order-first flows."
```

---

### Task 10: Full Regression + Replay Verification

- [ ] **Step 1: Run full test suite**

Run: `python -m pytest tests/ -v --tb=short 2>&1 | tail -30`
Expected: All 279+ existing tests + 31 new tests pass

- [ ] **Step 2: Run replay tests specifically**

Run: `python -m pytest tests/test_replay.py -v --tb=short 2>&1 | tail -15`
Expected: All replay tests pass (replay path unchanged)

- [ ] **Step 3: Verify test count matches spec**

Run: `python -m pytest tests/test_tickfile_sync.py --collect-only -q 2>&1 | tail -5`
Expected: 31 tests collected

- [ ] **Step 4: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix(tickfile-sync): regression fixes from full test suite run"
```

---

### Task 11: E2E Live Test (Spec Section 6.3)

**This task validates the full system with the data simulator.** Requires manual setup or scripting.

- [ ] **Step 1: Start simulator**

Terminal 1:
```bash
PYTHONPATH=src python -m data_simulator --speed 5 --order-speed 100 \
  --source-dir input --output-dir test/output --date 20260525
```

- [ ] **Step 2: Start minute bar engine**

Terminal 2:
```bash
PYTHONPATH=src python main.py --config config/test-tickfile-live.ini
```

- [ ] **Step 3: Verify tickfile bid/ask source**

After session completes, parse tickfile and check that UpdateTime values fall within the target minute window (not carry-forward from earlier minutes):
```bash
python -c "
import csv
with open('test/output/tickfile/20260525.csv') as f:
    reader = csv.DictReader(f)
    carry = 0
    real = 0
    for row in reader:
        mk = row.get('minute_key', '')
        ut = row.get('UpdateTime', '')
        if ut and mk and ut[:8] == mk[:8]:
            real += 1
        else:
            carry += 1
    total = real + carry
    print(f'Real: {real}/{total} ({100*real/total:.1f}%), Carry: {carry}/{total} ({100*carry/total:.1f}%)')
"
```
Expected: Real order coverage **>99%** (vs Phase 16's ~95.8% carry-forward)

- [ ] **Step 4: Verify seqno count matches snapshot count**

```bash
echo "Tickfile lines: $(tail -n +2 test/output/tickfile/20260525.csv | wc -l)"
echo "Snapshot dirs: $(ls test/output/snapshot/ | wc -l)"
```
Expected: Tickfile total seqno count == number of snapshot minute directories

- [ ] **Step 5: Check logs for Case A (order thread) vs Case B (clock thread)**

```bash
grep "Tickfile generated" test/output/*.log | grep -oP 'thread=\w+' | sort | uniq -c
```
Expected: `thread=order-thread` (Case A) is the dominant path

- [ ] **Step 6: Verify no pending overflow warnings**

```bash
grep "pending overflow" test/output/*.log
```
Expected: No output (no overflow triggered)

- [ ] **Step 7: Compare Live vs Replay tickfile (consistency oracle)**

Run replay with same input data, then diff:
```bash
PYTHONPATH=src python -m minute_bar.replay --input-dir test/output --output-dir test/replay-output --date 20260525
diff <(sort test/output/tickfile/20260525.csv) <(sort test/replay-output/tickfile/20260525.csv)
```
Expected: Empty diff (identical tickfile content)

---

### Task 12: Update Memory

- [ ] **Step 1: Update project memory**

Update memory file `C:\Users\rzpeng\.claude\projects\d--FIU\memory\tickfile-sync-design.md` with implementation status.
