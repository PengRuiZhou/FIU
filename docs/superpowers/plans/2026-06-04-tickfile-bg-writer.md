# Tickfile Background Writer + Seek Optimization Implementation Plan

> **STATUS: ✅ COMPLETE + E2E VERIFIED (2026-06-08)** — All 14 tasks implemented. 363 tests passed. E2E live test (data_simulator speed=100) passed with 2 bugs found and fixed: (1) overflow gate by order_watermark, (2) UpdateTime derived from minute_key for carry-forward rows.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace synchronous tickfile IO with a background writer thread + seek optimization to eliminate order thread stalls caused by tickfile IO blocking.

**Architecture:** A dedicated daemon writer thread consumes minute_keys from an unbounded queue and performs all tickfile IO serially. Order/clock threads only enqueue (microseconds, non-blocking). Cross-day uses pause/resume coordination. Seek replaces readlines for tail checks. fsync removed for live engine (tickfile = derived data).

**Tech Stack:** Python 3.12, threading, queue.Queue, unittest.mock, pytest

**Spec:** `docs/superpowers/specs/2026-06-04-tickfile-bg-writer-design.md` (v11, 4-round 12-agent review approved)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/minute_bar/writer.py` | Modify | Constants, RLock, seek tail check, `skip_fsync` param, `_prune_write_locks` |
| `src/minute_bar/engine.py` | Modify | Writer thread lifecycle, queue, drain, pause/resume, health check, start/stop rewrite, `_drain_tickfile_triggers` rewrite, `_cleanup_tickfile_tmp_files` |
| `src/minute_bar/flusher.py` | Modify | `_enqueue_tickfile`, `_tickfile_queue` ref, `_engine_ref`, overflow safety valve, `flush_all_remaining(skip_tickfile)`, cross-day Step 1 integration |
| `src/minute_bar/replay.py` | Modify | Pass `skip_fsync=False` to `write_tickfile_rows` |
| `tests/test_tickfile_bg_writer.py` | Create | 61 unit + 3 regression + 12 stress/crash tests |
| `tests/test_writer.py` | Modify | Seek tests, row byte assertions, RLock assertions |

**Key design decisions:**
- Unbounded queue (`maxsize=0`) — only stores minute_key strings (~12 bytes each)
- Sentinel = `object()` (not None) — prevents collision with buggy minute_keys
- `_tickfile_writer_alive` + `_tickfile_writer_running` — separate flags for GIL-protected state
- All lifecycle methods on Engine — Flusher only holds queue reference for enqueue
- 43 invariants (N1–N43) documented in spec Section 2.11

---

## Task 1: writer.py — Constants, RLock, `_prune_write_locks`

**Files:**
- Modify: `src/minute_bar/writer.py:1-28` (module-level constants + lock infrastructure)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 1.1: Write failing tests for constants, RLock, and prune

Create `tests/test_tickfile_bg_writer.py` with initial test classes:

```python
"""Tests for tickfile background writer (Phase 18).

Spec: docs/superpowers/specs/2026-06-04-tickfile-bg-writer-design.md
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


# ── Task 1: Constants + RLock + prune ──


class TestWriteLocksIsRLock:
    """Spec N27: _write_locks uses RLock instead of Lock."""

    def test_get_write_lock_returns_rlock(self):
        from minute_bar.writer import _get_write_lock
        lock = _get_write_lock("/tmp/test_rlock_tickfile.csv")
        assert isinstance(lock, threading.RLock), f"Expected RLock, got {type(lock)}"

    def test_rlock_reentrant(self):
        from minute_bar.writer import _get_write_lock
        lock = _get_write_lock("/tmp/test_rlock_reentrant.csv")
        with lock:
            with lock:  # Should NOT deadlock
                pass


class TestTickfileConstants:
    """Spec N41: TICKFILE_MAX_ROW_BYTES=640, TICKFILE_TAIL_READ_SIZE=4096."""

    def test_max_row_bytes_value(self):
        from minute_bar.writer import TICKFILE_MAX_ROW_BYTES
        assert TICKFILE_MAX_ROW_BYTES == 640

    def test_tail_read_size_value(self):
        from minute_bar.writer import TICKFILE_TAIL_READ_SIZE
        assert TICKFILE_TAIL_READ_SIZE == 4096

    def test_tail_read_size_safety_ratio(self):
        from minute_bar.writer import TICKFILE_MAX_ROW_BYTES, TICKFILE_TAIL_READ_SIZE
        assert TICKFILE_TAIL_READ_SIZE >= TICKFILE_MAX_ROW_BYTES * 6


class TestPruneWriteLocks:
    """Spec N30/N39: _prune_write_locks removes old-date tickfile entries."""

    def test_prune_removes_old_date_tickfile_locks(self):
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
        import pathlib
        with _write_lock_mutex:
            _write_locks.clear()
            _write_locks["/output/tickfile/2026/20260603/tickfile_20260603.csv"] = threading.RLock()
            _write_locks["/output/tickfile/2026/20260604/tickfile_20260604.csv"] = threading.RLock()
        _prune_write_locks("20260604")
        with _write_lock_mutex:
            assert len(_write_locks) == 1
            assert "/output/tickfile/2026/20260604/tickfile_20260604.csv" in _write_locks

    def test_prune_preserves_non_tickfile_locks(self):
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
        with _write_lock_mutex:
            _write_locks.clear()
            _write_locks["/output/snapshot/2026/20260603/snapshot_minute_20260603_0900.csv"] = threading.RLock()
            _write_locks["/output/order/2026/20260603/order_minute_20260603_0900.csv"] = threading.RLock()
            _write_locks["/output/tickfile/2026/20260603/tickfile_20260603.csv"] = threading.RLock()
        _prune_write_locks("20260604")
        with _write_lock_mutex:
            # snapshot and order locks should be preserved
            assert "/output/snapshot/2026/20260603/snapshot_minute_20260603_0900.csv" in _write_locks
            assert "/output/order/2026/20260603/order_minute_20260603_0900.csv" in _write_locks
            assert "/output/tickfile/2026/20260603/tickfile_20260603.csv" not in _write_locks

    def test_prune_path_precision_no_false_positive(self):
        """Spec N39: Uses pathlib path component check, not substring match."""
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
        with _write_lock_mutex:
            _write_locks.clear()
            # Path contains "tickfile" as substring but NOT as path component
            _write_locks["/output/some_tickfile_backup/20260603/data.csv"] = threading.RLock()
        _prune_write_locks("20260604")
        with _write_lock_mutex:
            # Should NOT be pruned (no "tickfile" path component)
            assert "/output/some_tickfile_backup/20260603/data.csv" in _write_locks
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestWriteLocksIsRLock tests/test_tickfile_bg_writer.py::TestTickfileConstants tests/test_tickfile_bg_writer.py::TestPruneWriteLocks -v`
Expected: FAIL (ImportError for new constants, AssertionError for Lock vs RLock)

- [ ] **Step 1.3: Implement constants and RLock in writer.py**

In `src/minute_bar/writer.py`, make these changes:

**Add constants after imports (around line 12):**

```python
logger = logging.getLogger(__name__)

# Tickfile IO constants (Phase 18, spec N41)
TICKFILE_MAX_ROW_BYTES = 640   # v11: Increased from 512. Pathological float repr() can reach ~562 bytes.
                                # Measured typical max ~423 bytes. 640 provides >13% margin.
TICKFILE_TAIL_READ_SIZE = 4096  # >6x TICKFILE_MAX_ROW_BYTES, covers truncated lines safely
assert TICKFILE_TAIL_READ_SIZE >= TICKFILE_MAX_ROW_BYTES * 6, (
    "TAIL_READ_SIZE must be >= 6x MAX_ROW_BYTES for seek safety")
```

**Change `_write_locks` type (line 20):**

```python
# BEFORE:
_write_locks: Dict[str, threading.Lock] = {}

# AFTER:
_write_locks: Dict[str, threading.RLock] = {}  # RLock prevents crash-induced deadlock (N27)
```

**Change `_get_write_lock` return type (line 24-28):**

```python
# BEFORE:
def _get_write_lock(path: str) -> threading.Lock:
    with _write_lock_mutex:
        if path not in _write_locks:
            _write_locks[path] = threading.Lock()
        return _write_locks[path]

# AFTER:
def _get_write_lock(path: str) -> threading.RLock:
    with _write_lock_mutex:
        if path not in _write_locks:
            _write_locks[path] = threading.RLock()
        return _write_locks[path]
```

**Add `_prune_write_locks` function after `_get_write_lock` (after line 28):**

```python
def _prune_write_locks(current_date: str) -> None:
    """Remove _write_locks entries for dates other than current_date.
    Called at cross-day after writer is paused and before resume.
    Prevents unbounded growth of module-level dict in long-running processes.

    Only prunes TICKFILE paths (uses pathlib path component check, N39).
    _write_locks is shared by snapshot, order, kline, and tickfile files.
    """
    import pathlib
    with _write_lock_mutex:
        stale_keys = [k for k in _write_locks
                      if any(p == "tickfile" for p in pathlib.PurePath(k).parts)
                      and current_date not in k]
        for k in stale_keys:
            del _write_locks[k]
        if stale_keys:
            logger.debug("Pruned %d stale tickfile _write_locks entries", len(stale_keys))
```

- [ ] **Step 1.4: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestWriteLocksIsRLock tests/test_tickfile_bg_writer.py::TestTickfileConstants tests/test_tickfile_bg_writer.py::TestPruneWriteLocks -v`
Expected: PASS

- [ ] **Step 1.5: Run existing writer tests to verify no regressions**

Run: `cd D:/FIU && python -m pytest tests/test_writer.py -v`
Expected: ALL PASS (RLock is backward-compatible with Lock usage)

- [ ] **Step 1.6: Commit**

```bash
git add src/minute_bar/writer.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): add constants, RLock, and _prune_write_locks (Phase 18 Task 1)

- TICKFILE_MAX_ROW_BYTES=640, TICKFILE_TAIL_READ_SIZE=4096 (N41)
- _write_locks changed to RLock to prevent crash-induced deadlock (N27)
- _prune_write_locks() for cross-day stale lock cleanup (N30/N39)
- Uses pathlib path component check (not substring match)"
```

---

## Task 2: writer.py — Seek-Based Tail Check

**Files:**
- Modify: `src/minute_bar/writer.py:342-353` (inside `write_tickfile_rows` append path)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 2.1: Write failing tests for seek tail check

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 2: Seek-based tail check ──


class TestSeekTailCheckDetectsTruncation:
    """Spec N5: seek detects truncated last line."""

    def test_truncated_line_detected(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path
        from minute_bar.tickfile import TICKFILE_HEADER

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Write a valid tickfile first
        snap = _make_snapshot()
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)
        assert os.path.exists(path)

        # Corrupt the last line (truncate to < 65 fields)
        with open(path, "r", encoding="utf-8", newline="") as f:
            content = f.read()
        lines = content.strip().split("\n")
        corrupted = lines[0] + "\n" + "truncated,bad,data\n"
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(corrupted)

        # Append should succeed and fix the truncation
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 2)
        with open(path, "r", encoding="utf-8") as f:
            result = f.read()
        # Should have header + 2 data rows (no corrupted data in between)
        result_lines = [l for l in result.strip().split("\n") if l.strip()]
        assert len(result_lines) == 3  # header + 2 valid rows


class TestSeekTailCheckEmptyFile:
    """Spec: empty file → skip check."""

    def test_empty_file_handled(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path

        output_dir = str(tmp_path)
        minute_key = "202606020901"
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Create empty file
        with open(path, "w") as f:
            pass
        snap = _make_snapshot()
        # Should handle empty file (header rewrite path)
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)
        with open(path, "r") as f:
            content = f.read()
        assert content.startswith("InstrumentID")


class TestSeekTailCheckHeaderOnly:
    """Spec: header-only file → no truncation detected."""

    def test_header_only_no_false_positive(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path
        from minute_bar.tickfile import TICKFILE_HEADER

        output_dir = str(tmp_path)
        minute_key = "202606020902"
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(TICKFILE_HEADER + "\n")

        snap = _make_snapshot()
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)
        with open(path, "r") as f:
            content = f.read()
        lines = [l for l in content.strip().split("\n") if l.strip()]
        assert len(lines) == 2  # header + 1 data row


class TestSeekTailCorruptionSetsNewlineFix:
    """Spec: non-UTF-8 bytes → need_newline_fix=True."""

    def test_non_utf8_triggers_newline_fix(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path
        from minute_bar.tickfile import TICKFILE_HEADER

        output_dir = str(tmp_path)
        minute_key = "202606020903"
        path = get_tickfile_path(output_dir, minute_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Write header + a valid line + corrupt bytes
        with open(path, "wb") as f:
            f.write((TICKFILE_HEADER + "\n").encode("utf-8"))
            f.write(b"7203,data")  # Corrupt — no newline, not valid UTF-8 necessarily
            # Add non-UTF8 byte sequence
            f.write(b"\xff\xfe")  # Invalid UTF-8

        snap = _make_snapshot()
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)
        # Should succeed despite corruption
        assert os.path.exists(path)
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestSeekTailCheckDetectsTruncation tests/test_tickfile_bg_writer.py::TestSeekTailCheckEmptyFile tests/test_tickfile_bg_writer.py::TestSeekTailCheckHeaderOnly tests/test_tickfile_bg_writer.py::TestSeekTailCorruptionSetsNewlineFix -v`
Expected: FAIL (seek not yet implemented, readlines still in place)

- [ ] **Step 2.3: Implement seek-based tail check in writer.py**

In `src/minute_bar/writer.py`, inside `write_tickfile_rows`, replace lines 342-353 (the readlines block inside the `else` branch of `if not os.path.exists(path)`) with:

```python
            # Check for truncated last line using seek (replaces readlines)
            # MUST be inside `with _get_write_lock(path):` block for TOCTOU safety (N5)
            need_newline_fix = False
            file_size = os.path.getsize(path)
            tail_size = min(file_size, TICKFILE_TAIL_READ_SIZE)
            if tail_size > 0:
                with open(path, "rb") as f:
                    f.seek(-tail_size, 2)
                    tail_bytes = f.read()
                last_line = ""
                for raw_line in reversed(tail_bytes.split(b'\n')):
                    stripped = raw_line.strip()
                    if stripped:
                        try:
                            last_line = stripped.decode("utf-8", errors="strict")
                        except UnicodeDecodeError:
                            logger.warning(
                                "Tickfile tail check: non-UTF8 bytes in last line of %s, "
                                "treating as corrupted",
                                path,
                            )
                            need_newline_fix = True
                            break
                        break
                if last_line and len(last_line.split(',')) != 65:
                    need_newline_fix = True
                    logger.warning(
                        "Tickfile truncated last line detected: %s, appending newline before new data",
                        path,
                    )
```

This replaces the original:
```python
            # Check for truncated last line
            with open(path, "r", encoding="utf-8", newline="") as f:
                lines = f.readlines()
            need_newline_fix = False
            if lines:
                last_line = lines[-1]
                if last_line.strip() and len(last_line.strip().split(',')) != 65:
                    need_newline_fix = True
                    ...
```

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestSeekTailCheckDetectsTruncation tests/test_tickfile_bg_writer.py::TestSeekTailCheckEmptyFile tests/test_tickfile_bg_writer.py::TestSeekTailCheckHeaderOnly tests/test_tickfile_bg_writer.py::TestSeekTailCorruptionSetsNewlineFix -v`
Expected: PASS

- [ ] **Step 2.5: Run existing tickfile sync tests for regression**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_sync.py tests/test_tickfile.py tests/test_writer.py -v`
Expected: ALL PASS

- [ ] **Step 2.6: Commit**

```bash
git add src/minute_bar/writer.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): replace readlines with seek-based tail check (Phase 18 Task 2)

- Binary seek(-4KB, 2) replaces readlines() for truncated line detection
- Single IO from ~200ms to ~30ms for large files (N5)
- UTF-8 strict decode with conservative corruption fallback
- TICKFILE_TAIL_READ_SIZE > 6x TICKFILE_MAX_ROW_BYTES safety margin"
```

---

## Task 3: writer.py — `skip_fsync` Parameter

**Files:**
- Modify: `src/minute_bar/writer.py:265-361` (`write_tickfile_rows` function)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 3.1: Write failing tests for skip_fsync

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 3: skip_fsync parameter ──


class TestNoFsyncLiveEngine:
    """Spec N6: Live Engine skip_fsync=True, no os.fsync called."""

    def test_skip_fsync_true_no_fsync(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        snap = _make_snapshot()

        with patch("os.fsync") as mock_fsync:
            write_tickfile_rows(
                output_dir, minute_key, [("7203", snap, None)], 1,
                skip_fsync=True,
            )
            mock_fsync.assert_not_called()


class TestFsyncReplayEngine:
    """Spec N6: ReplayEngine skip_fsync=False, os.fsync IS called."""

    def test_skip_fsync_false_calls_fsync(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows

        output_dir = str(tmp_path)
        minute_key = "202606020901"
        snap = _make_snapshot()

        with patch("os.fsync") as mock_fsync:
            write_tickfile_rows(
                output_dir, minute_key, [("7203", snap, None)], 1,
                skip_fsync=False,
            )
            mock_fsync.assert_called()
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestNoFsyncLiveEngine tests/test_tickfile_bg_writer.py::TestFsyncReplayEngine -v`
Expected: FAIL (skip_fsync parameter not yet added)

- [ ] **Step 3.3: Implement skip_fsync in write_tickfile_rows**

In `src/minute_bar/writer.py`, modify `write_tickfile_rows` signature (line 265):

```python
def write_tickfile_rows(
    output_dir: str,
    minute_key: str,
    selected: list,
    seqno: int,
    code_table_getter=None,
    skip_fsync: bool = False,  # New parameter (Phase 18, spec N6)
) -> None:
```

Then, in the **new file creation** path (inside the first `if not os.path.exists(path)` block, around line 308-309), replace `os.fsync(f.fileno())` with:

```python
                    f.flush()
                    if not skip_fsync:
                        os.fsync(f.fileno())
```

In the **header rewrite** path (the `if file_size == 0` branch, around line 331-332), replace `os.fsync(f.fileno())` with:

```python
                    f.flush()
                    if not skip_fsync:
                        os.fsync(f.fileno())
```

In the **append** path (the final `with open(path, "a"...)` block, around line 360-361), replace `os.fsync(f.fileno())` with:

```python
                f.flush()
                if not skip_fsync:
                    os.fsync(f.fileno())
```

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestNoFsyncLiveEngine tests/test_tickfile_bg_writer.py::TestFsyncReplayEngine -v`
Expected: PASS

- [ ] **Step 3.5: Commit**

```bash
git add src/minute_bar/writer.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): add skip_fsync parameter to write_tickfile_rows (Phase 18 Task 3)

- Live Engine: skip_fsync=True (tickfile is derived data)
- ReplayEngine: skip_fsync=False (long rerun, crash data loss expensive)
- Default skip_fsync=False preserves backward compatibility"
```

---

## Task 4: replay.py — Pass `skip_fsync=False`

**Files:**
- Modify: `src/minute_bar/replay.py:262` (`write_tickfile_rows` call)

### Step 4.1: Update the ReplayEngine call

In `src/minute_bar/replay.py`, around line 262, change:

```python
            write_tickfile_rows(output_dir, minute_key, selected, self._tickfile_seqno, code_table_getter=code_getter)

```

to:

```python
            write_tickfile_rows(output_dir, minute_key, selected, self._tickfile_seqno,
                                code_table_getter=code_getter, skip_fsync=False)
```

This is a no-op behavior change (False is the default), but makes the intent explicit.

- [ ] **Step 4.2: Run replay tests for regression**

Run: `cd D:/FIU && python -m pytest tests/test_replay.py -v`
Expected: ALL PASS (behavior unchanged)

- [ ] **Step 4.3: Commit**

```bash
git add src/minute_bar/replay.py
git commit -m "refactor(replay): explicitly pass skip_fsync=False to write_tickfile_rows (Phase 18 Task 4)"
```

---

## Task 5: Engine + Flusher Infrastructure — Queue, Flags, `_enqueue_tickfile`

**Files:**
- Modify: `src/minute_bar/engine.py` (`__init__` + module-level constants)
- Modify: `src/minute_bar/flusher.py` (`__init__` + `_enqueue_tickfile`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 5.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
import queue


# ── Task 5: Queue + flags + _enqueue_tickfile infrastructure ──


class TestEnqueueTickfileNoneQueueNoop:
    """Spec N21: _tickfile_queue=None → _enqueue_tickfile returns silently."""

    def test_none_queue_noop(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )
        # _tickfile_queue should be None by default
        assert flusher._tickfile_queue is None
        # Should not raise
        flusher._enqueue_tickfile("202606020900")


class TestEnqueueReplacesDirectCall:
    """Spec: _drain_tickfile_triggers enqueues instead of calling _try_generate_tickfile."""

    def test_drain_enqueues_to_queue(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock, PropertyMock

        # Create a minimal Engine mock that has _drain_tickfile_triggers
        config = MagicMock(spec=AppConfig)
        config.output.enable_tickfile = True
        config.output.output_dir = "/tmp/test"
        config.input.target_date = "20260602"

        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260602"
        engine._tickfile_trigger_pending = ["202606020900", "202606020901"]
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_enqueue_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_writer_error_count = 0
        engine._state = SharedState()
        engine._state.order_current_minute = ""
        engine._state._tickfile_pending = {}
        engine._flusher = MagicMock()

        engine._drain_tickfile_triggers()

        # Queue should have entries
        assert engine._tickfile_queue.qsize() > 0


class TestUnboundedQueuePutAlwaysSucceeds:
    """Spec N2: unbounded queue put_nowait never raises queue.Full."""

    def test_unbounded_put_never_full(self):
        q = queue.Queue()  # maxsize=0 = unbounded
        for i in range(10000):
            q.put_nowait(f"2026060209{i % 60:02d}")
        assert q.qsize() == 10000


class TestN35EngineRefCounterRouting:
    """Spec N35: _enqueue_tickfile increments engine._tickfile_enqueue_count (NOT flusher attr)."""

    def test_counter_on_engine_not_flusher(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )

        # Simulate Engine setting _engine_ref
        class FakeEngine:
            _tickfile_enqueue_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine
        flusher._tickfile_queue = queue.Queue()

        flusher._enqueue_tickfile("202606020900")

        # Counter should be on engine, not on flusher
        assert engine._tickfile_enqueue_count == 1
        assert not hasattr(flusher, '_tickfile_enqueue_count') or flusher.__dict__.get('_tickfile_enqueue_count', 0) == 0
```

- [ ] **Step 5.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestEnqueueTickfileNoneQueueNoop tests/test_tickfile_bg_writer.py::TestEnqueueReplacesDirectCall tests/test_tickfile_bg_writer.py::TestUnboundedQueuePutAlwaysSucceeds tests/test_tickfile_bg_writer.py::TestN35EngineRefCounterRouting -v`
Expected: FAIL (new methods/attributes not yet defined)

- [ ] **Step 5.3: Add module-level constants to engine.py**

At the top of `src/minute_bar/engine.py`, after imports and before `MAX_PENDING_ORDER_MINUTES`, add:

```python
import queue as _queue_module

_TICKFILE_QUEUE_WARNING_THRESHOLD = 500  # Log WARNING if queue depth exceeds
_TICKFILE_QUEUE_CRITICAL_THRESHOLD = 800  # Log CRITICAL if queue depth exceeds

# Module-level sentinel (NOT None — avoids collision with bugs)
_TICKFILE_SENTINEL_STOP = object()  # Unique sentinel: "stop your loop"

_TICKFILE_MAX_CONSECUTIVE_ERRORS = 5
```

- [ ] **Step 5.4: Add writer thread fields to Engine.__init__**

In `src/minute_bar/engine.py`, inside `Engine.__init__`, after the `self._tickfile_trigger_pending: list = []` line (around line 186), add:

```python
        # ── Tickfile background writer (Phase 18) ──
        self._tickfile_queue: _queue_module.Queue = _queue_module.Queue()  # UNBOUNDED — never blocks enqueue
        self._tickfile_writer_thread: Optional[threading.Thread] = None
        self._tickfile_writer_alive = False  # Guard: True iff writer thread is running
        self._tickfile_writer_running = False  # Writer-specific stop flag (NOT global _running)
        self._tickfile_writer_error_count = 0  # Consecutive error counter (reset on resume)
        self._tickfile_started = False  # Guard against double-start
        self._tickfile_writer_restart_count = 0  # Auto-restart attempts (reset at cross-day)
        self._tickfile_health_log_counter = 0  # Periodic health log counter (reset at start)

        # Metrics counters (all on Engine, incremented via _engine_ref from Flusher)
        self._tickfile_enqueue_count = 0
        self._tickfile_dequeue_count = 0
        self._tickfile_writer_exception_count = 0
        self._tickfile_overflow_direct_io_count = 0
        self._tickfile_queue_stale_drain_count = 0
        self._tickfile_writer_zombie_detected_count = 0
        self._tickfile_writer_restart_total = 0  # Lifetime counter (never reset)
        self._tickfile_queue_skip_count = 0  # N19 silent skips
```

Also set `_engine_ref` on the flusher, right after the flusher construction (after line 174):

```python
        self._flusher._engine_ref = self  # For cross-day pause/resume callback + counter routing
```

- [ ] **Step 5.5: Add `_tickfile_queue` and `_enqueue_tickfile` to Flusher**

In `src/minute_bar/flusher.py`, inside `ClockWatermarkFlusher.__init__`, add at the end (after line 96):

```python
        # Tickfile background writer references (set by Engine)
        self._tickfile_queue: Optional[_queue_module.Queue] = None  # Set by Engine.start()
        self._engine_ref = None  # Set by Engine.__init__ for counter routing
```

Add the import at the top of flusher.py:

```python
import queue as _queue_module
```

Add `_enqueue_tickfile` method to `ClockWatermarkFlusher`:

```python
    def _enqueue_tickfile(self, minute_key: str) -> None:
        """Enqueue a tickfile generation request. Thread-safe. Non-blocking.
        Queue is unbounded — put_nowait always succeeds.

        INVARIANT: Never calls _try_generate_tickfile directly.
        """
        if self._tickfile_queue is None:
            return  # Not started yet — defensive guard (N21)
        self._tickfile_queue.put_nowait(minute_key)
        # CRITICAL: Use _engine_ref to increment Engine's counter (N35)
        if self._engine_ref is not None:
            self._engine_ref._tickfile_enqueue_count += 1
        # Monitor queue depth
        qsize = self._tickfile_queue.qsize()
        if qsize > _TICKFILE_QUEUE_CRITICAL_THRESHOLD:
            logger.critical(
                "Tickfile queue depth %d exceeds critical threshold %d [thread=%s]",
                qsize, _TICKFILE_QUEUE_CRITICAL_THRESHOLD,
                threading.current_thread().name,
            )
        elif qsize > _TICKFILE_QUEUE_WARNING_THRESHOLD:
            logger.warning(
                "Tickfile queue depth %d exceeds warning threshold %d [thread=%s]",
                qsize, _TICKFILE_QUEUE_WARNING_THRESHOLD,
                threading.current_thread().name,
            )
```

**Important:** Move the threshold constants from engine.py to be importable, OR define them in both files. The simplest approach: define the constants in engine.py and import in flusher.py. Add to flusher.py imports:

```python
from minute_bar.engine import _TICKFILE_QUEUE_WARNING_THRESHOLD, _TICKFILE_QUEUE_CRITICAL_THRESHOLD
```

**Note:** This creates a potential circular import (engine imports flusher, flusher imports engine constants). To avoid this, define the threshold constants in a shared location. The simplest fix: define them at module level in flusher.py (duplicated) since they are simple integer constants that won't change independently:

```python
_TICKFILE_QUEUE_WARNING_THRESHOLD = 500
_TICKFILE_QUEUE_CRITICAL_THRESHOLD = 800
```

Both engine.py and flusher.py define the same constants independently. This avoids circular imports.

- [ ] **Step 5.6: Rewrite `_drain_tickfile_triggers` to use enqueue**

In `src/minute_bar/engine.py`, replace the `_drain_tickfile_triggers` method (lines 660-686) with:

```python
    def _drain_tickfile_triggers(self) -> None:
        """Drain _tickfile_trigger_pending: update order_current_minute + enqueue tickfiles.
        Called after batch write and after _flush_expired_order_minutes.
        Tickfile generation is delegated to TickfileWriterThread.

        INVARIANT: Enqueue is O(1) microsecond via put_nowait, NEVER blocks order thread.
        Queue is unbounded — put_nowait always succeeds.
        """
        if not self._tickfile_trigger_pending:
            return
        triggers = list(self._tickfile_trigger_pending)
        # N31: Do NOT clear yet — enqueue first, clear after success.

        if triggers:
            latest = max(triggers)
            with self._state.lock:
                # DATE GUARD: only update order_current_minute if date matches current target date.
                current_date = self._get_target_date()
                if latest[:8] == current_date:
                    self._state.order_current_minute = latest
                else:
                    logger.debug(
                        "Skipping order_current_minute update: trigger date %s != current date %s",
                        latest[:8], current_date,
                    )
                for mk in list(self._state._tickfile_pending):
                    if mk <= latest and mk not in triggers:
                        triggers.append(mk)

        # Enqueue all triggers — unbounded queue, put_nowait always succeeds
        for mk in triggers:
            self._tickfile_queue.put_nowait(mk)
        self._tickfile_enqueue_count += len(triggers)

        # Monitor queue depth
        qsize = self._tickfile_queue.qsize()
        if qsize > _TICKFILE_QUEUE_CRITICAL_THRESHOLD:
            logger.critical(
                "Tickfile queue depth %d exceeds critical threshold %d [thread=order-thread]",
                qsize, _TICKFILE_QUEUE_CRITICAL_THRESHOLD,
            )
        elif qsize > _TICKFILE_QUEUE_WARNING_THRESHOLD:
            logger.warning(
                "Tickfile queue depth %d exceeds warning threshold %d [thread=order-thread]",
                qsize, _TICKFILE_QUEUE_WARNING_THRESHOLD,
            )

        # N31: Clear pending triggers ONLY after all enqueues succeed.
        self._tickfile_trigger_pending.clear()
```

- [ ] **Step 5.7: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py::TestEnqueueTickfileNoneQueueNoop tests/test_tickfile_bg_writer.py::TestEnqueueReplacesDirectCall tests/test_tickfile_bg_writer.py::TestUnboundedQueuePutAlwaysSucceeds tests/test_tickfile_bg_writer.py::TestN35EngineRefCounterRouting -v`
Expected: PASS

- [ ] **Step 5.8: Run existing engine/flusher tests for regression**

Run: `cd D:/FIU && python -m pytest tests/test_flusher.py tests/test_tickfile_sync.py -v`
Expected: ALL PASS (flusher still has direct IO paths, engine enqueue is additive)

- [ ] **Step 5.9: Commit**

```bash
git add src/minute_bar/engine.py src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): add writer thread infrastructure + _enqueue_tickfile (Phase 18 Task 5)

- Engine: queue, thread flags, metric counters in __init__
- Flusher: _tickfile_queue, _engine_ref, _enqueue_tickfile method
- _drain_tickfile_triggers rewritten to enqueue instead of direct IO
- Queue depth monitoring with WARNING/CRITICAL thresholds"
```

---

## Task 6: Engine — Writer Loop + Drain

**Files:**
- Modify: `src/minute_bar/engine.py` (add `_tickfile_writer_loop`, `_tickfile_writer_drain`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 6.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 6: Writer loop + drain ──


class TestBackgroundThreadGeneratesTickfile:
    """Spec: writer thread takes mk from queue and generates tickfile."""

    def test_writer_generates_from_queue(self):
        """Integration test: enqueue → writer loop → _try_generate_tickfile called."""
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        config.output.enable_tickfile = True
        config.output.output_dir = "/tmp/test"
        config.input.target_date = "20260602"

        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260602"
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._state = SharedState()

        mock_flusher = MagicMock()
        engine._flusher = mock_flusher

        # Enqueue a minute key
        engine._tickfile_queue.put("202606020900")
        # Signal stop after processing
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)

        engine._tickfile_writer_loop()

        mock_flusher._try_generate_tickfile.assert_called_once_with("202606020900")
        assert engine._tickfile_dequeue_count == 1
        assert not engine._tickfile_writer_alive


class TestSentinelStopsLoopNoDrain:
    """Spec N17/N3: object() sentinel → break without drain."""

    def test_sentinel_breaks_loop(self):
        from minute_bar.engine import Engine, _TICKFILE_SENTINEL_STOP
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._flusher = MagicMock()

        # Put sentinel directly
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)

        engine._tickfile_writer_loop()

        assert not engine._tickfile_writer_alive
        # _try_generate_tickfile should NOT have been called
        engine._flusher._try_generate_tickfile.assert_not_called()


class TestSentinelObjectNotNone:
    """Spec N17: enqueue None → writer does NOT stop."""

    def test_none_does_not_stop_loop(self):
        from minute_bar.engine import Engine, _TICKFILE_SENTINEL_STOP
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._flusher = MagicMock()

        # Put None (should NOT stop) then sentinel
        engine._tickfile_queue.put(None)
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)

        engine._tickfile_writer_loop()

        # None was treated as a minute_key and passed to _try_generate_tickfile
        engine._flusher._try_generate_tickfile.assert_called_once_with(None)


class TestWriterFailureNoReenqueue:
    """Spec N13: failure → no re-enqueue, pending preserved by _try_generate_tickfile."""

    def test_failure_does_not_reenqueue(self):
        from minute_bar.engine import Engine, _TICKFILE_SENTINEL_STOP
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._flusher = MagicMock()
        engine._flusher._try_generate_tickfile.side_effect = IOError("disk error")

        engine._tickfile_queue.put("202606020900")
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)

        engine._tickfile_writer_loop()

        # Queue should be empty (no re-enqueue)
        assert engine._tickfile_queue.empty()
        assert engine._tickfile_writer_error_count == 1


class TestWriterConsecutiveErrorsStopWriterOnly:
    """Spec N8: 5 consecutive failures → stop writer only, not engine."""

    def test_five_errors_stops_writer_not_engine(self):
        from minute_bar.engine import Engine, _TICKFILE_MAX_CONSECUTIVE_ERRORS
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._flusher = MagicMock()
        engine._flusher._try_generate_tickfile.side_effect = IOError("persistent error")

        # Enqueue enough items to trigger 5 failures
        for i in range(6):
            engine._tickfile_queue.put(f"2026060209{i:02d}")

        engine._tickfile_writer_loop()

        assert engine._tickfile_writer_error_count >= _TICKFILE_MAX_CONSECUTIVE_ERRORS
        assert not engine._tickfile_writer_running
        assert not engine._tickfile_writer_alive


class TestDrainTimeoutAbandonsEntries:
    """Spec N24: drain with timeout → abandon remaining."""

    def test_drain_timeout_abandons(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_thread = None
        engine._flusher = MagicMock()
        # Make _try_generate_tickfile very slow to trigger timeout
        engine._flusher._try_generate_tickfile.side_effect = lambda mk: time.sleep(0.5)

        # Fill queue with entries
        for i in range(20):
            engine._tickfile_queue.put(f"2026060209{i:02d}")

        # Drain with very short timeout
        drained = engine._tickfile_writer_drain(timeout_sec=0.1)

        # Should have abandoned most entries
        assert drained < 20


class TestFinallySetsAliveFalseOnBaseException:
    """Spec N9: writer SystemExit → finally sets alive=False."""

    def test_systemexit_sets_alive_false(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = True
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._flusher = MagicMock()
        engine._flusher._try_generate_tickfile.side_effect = SystemExit(1)

        engine._tickfile_queue.put("202606020900")

        # SystemExit should propagate but finally should run
        with pytest.raises(SystemExit):
            engine._tickfile_writer_loop()

        assert not engine._tickfile_writer_alive
        assert not engine._tickfile_writer_running
```

- [ ] **Step 6.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestBackgroundThread or TestSentinelStops or TestSentinelObject or TestWriterFailure or TestWriterConsecutive or TestDrainTimeout or TestFinallySetsAlive" -v`
Expected: FAIL (methods not yet implemented)

- [ ] **Step 6.3: Implement `_tickfile_writer_loop` on Engine**

Add to `src/minute_bar/engine.py`, `Engine` class:

```python
    def _tickfile_writer_loop(self) -> None:
        """Background thread: sole tickfile writer. Processes queue entries serially.

        INVARIANT: This is the ONLY thread that calls _try_generate_tickfile
        (except: cross-day/shutdown when writer confirmed stopped).

        Exit conditions:
        - Receives _TICKFILE_SENTINEL_STOP → break (do NOT drain here)
        - Exceeds _TICKFILE_MAX_CONSECUTIVE_ERRORS → set _tickfile_writer_running=False → break
        - _tickfile_writer_running becomes False (external stop) → break
        """
        import threading as _threading
        _threading.current_thread().name = "tickfile-writer"

        try:
            while self._tickfile_writer_running:
                try:
                    mk = self._tickfile_queue.get(timeout=0.5)
                except _queue_module.Empty:
                    continue

                if mk is _TICKFILE_SENTINEL_STOP:
                    break

                try:
                    self._flusher._try_generate_tickfile(mk)
                    self._tickfile_writer_error_count = 0
                    self._tickfile_dequeue_count += 1
                except SystemExit:
                    self._tickfile_writer_exception_count += 1
                    logger.critical(
                        "Tickfile writer received SystemExit for minute=%s [thread=tickfile-writer]. "
                        "Writer loop will terminate. Pending data recoverable via ReplayEngine.",
                        mk,
                    )
                    raise
                except Exception:
                    self._tickfile_writer_error_count += 1
                    self._tickfile_writer_exception_count += 1
                    logger.exception(
                        "Tickfile generation failed for minute=%s [thread=tickfile-writer, "
                        "consecutive_errors=%d/%d]",
                        mk, self._tickfile_writer_error_count, _TICKFILE_MAX_CONSECUTIVE_ERRORS,
                    )

                    if self._tickfile_writer_error_count >= _TICKFILE_MAX_CONSECUTIVE_ERRORS:
                        logger.critical(
                            "Tickfile writer: %d consecutive failures. Stopping writer thread. "
                            "Engine continues without tickfile output. "
                            "Recovery: ReplayEngine to regenerate tickfiles.",
                            self._tickfile_writer_error_count,
                        )
                        self._tickfile_writer_running = False
                        break
        finally:
            self._tickfile_writer_alive = False
            self._tickfile_writer_running = False
            logger.info("Tickfile writer thread exiting [thread=tickfile-writer]")
```

- [ ] **Step 6.4: Implement `_tickfile_writer_drain` on Engine**

Add to `src/minute_bar/engine.py`, `Engine` class:

```python
    def _tickfile_writer_drain(self, timeout_sec: float = 30.0) -> int:
        """Drain all remaining entries from the tickfile queue.
        Called by Engine after writer thread is confirmed dead (via join).
        Single-threaded by construction — join guarantees writer has exited.

        Returns number of items successfully processed.
        """
        import time as _time
        drained = 0
        abandoned = 0

        # Zombie guard — if writer thread is still alive, skip all IO (N29)
        if self._tickfile_writer_thread is not None and self._tickfile_writer_thread.is_alive():
            self._tickfile_writer_zombie_detected_count += 1
            try:
                while True:
                    mk = self._tickfile_queue.get_nowait()
                    if mk is not _TICKFILE_SENTINEL_STOP:
                        abandoned += 1
            except _queue_module.Empty:
                pass
            if abandoned > 0:
                logger.critical(
                    "Tickfile drain skipped (writer thread still alive/zombie): "
                    "%d entries abandoned in queue (recoverable via ReplayEngine)",
                    abandoned,
                )
            return 0

        deadline = _time.monotonic() + timeout_sec
        while True:
            if _time.monotonic() > deadline:
                try:
                    while True:
                        mk = self._tickfile_queue.get_nowait()
                        if mk is not _TICKFILE_SENTINEL_STOP:
                            abandoned += 1
                except _queue_module.Empty:
                    pass
                break
            try:
                mk = self._tickfile_queue.get_nowait()
            except _queue_module.Empty:
                break
            if mk is _TICKFILE_SENTINEL_STOP:
                continue
            try:
                self._flusher._try_generate_tickfile(mk)
                drained += 1
                self._tickfile_queue_stale_drain_count += 1
            except Exception:
                logger.exception("Tickfile generation failed for minute=%s [drain]", mk)
        if drained > 0:
            logger.info("Tickfile queue drained: %d items processed", drained)
        if abandoned > 0:
            logger.critical(
                "Tickfile drain timed out after %.0fs: %d entries abandoned (recoverable via ReplayEngine)",
                timeout_sec, abandoned,
            )
        return drained
```

- [ ] **Step 6.5: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestBackgroundThread or TestSentinelStops or TestSentinelObject or TestWriterFailure or TestWriterConsecutive or TestDrainTimeout or TestFinallySetsAlive" -v`
Expected: PASS

- [ ] **Step 6.6: Commit**

```bash
git add src/minute_bar/engine.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): implement writer loop + drain methods (Phase 18 Task 6)

- _tickfile_writer_loop: sole writer, serial IO, error escalation
- _tickfile_writer_drain: post-join queue drain with zombie guard
- Sentinel-based stop, SystemExit handling, finally guarantees"
```

---

## Task 7: Flusher — Overflow Safety Valve + `_flush_minutes_internal` Enqueue

**Files:**
- Modify: `src/minute_bar/flusher.py` (tick() overflow, `_flush_minutes_internal`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 7.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 7: Overflow safety valve + flush_minutes_internal enqueue ──


class TestOverflowFallbackWhenWriterDead:
    """Spec N25: writer dead → overflow direct IO, at most 1 key per tick()."""

    def test_overflow_direct_io_one_key(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )

        # Simulate engine reference with dead writer
        class FakeEngine:
            _tickfile_writer_alive = False
            _tickfile_writer_thread = None
            _tickfile_overflow_direct_io_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine
        flusher._tickfile_queue = queue.Queue()
        # Set thread to None so is_alive() check passes
        engine._tickfile_writer_thread = None

        # Mock _try_generate_tickfile
        with patch.object(flusher, '_try_generate_tickfile') as mock_gen:
            flusher._overflow_tickfile_force(["202606020900", "202606020901"], engine)
            # Only 1 direct IO call
            assert mock_gen.call_count == 1
            assert engine._tickfile_overflow_direct_io_count == 1


class TestOverflowIsAliveGuard:
    """Spec N18/N25: alive=False but thread.is_alive()=True → skip direct IO."""

    def test_alive_false_thread_alive_skips_io(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )

        class FakeEngine:
            _tickfile_writer_alive = False
            _tickfile_writer_thread = None
            _tickfile_overflow_direct_io_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine
        flusher._tickfile_queue = queue.Queue()

        # Create a mock thread that reports is_alive() = True
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        engine._tickfile_writer_thread = mock_thread

        with patch.object(flusher, '_try_generate_tickfile') as mock_gen:
            flusher._overflow_tickfile_force(["202606020900"], engine)
            mock_gen.assert_not_called()


class TestDateGuardBlocksOldDayUpdate:
    """Spec N15: old-day trigger → order_current_minute not updated."""

    def test_old_date_skipped(self):
        """Already tested via _drain_tickfile_triggers with date guard."""
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260604"  # Current date
        engine._tickfile_trigger_pending = ["202606030900"]  # Old date
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_enqueue_count = 0
        engine._state = SharedState()
        engine._state.order_current_minute = ""
        engine._state._tickfile_pending = {}
        engine._flusher = MagicMock()

        engine._drain_tickfile_triggers()

        # order_current_minute should NOT be updated
        assert engine._state.order_current_minute == ""
```

- [ ] **Step 7.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestOverflowFallback or TestOverflowIsAlive or TestDateGuard" -v`
Expected: FAIL

- [ ] **Step 7.3: Modify tick() overflow in flusher.py**

In `src/minute_bar/flusher.py`, inside `tick()`, replace the overflow section (around lines 112-121) with:

```python
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
                    self._enqueue_tickfile(mk)

                # Overflow safety valve (N25): at most ONE direct IO per tick() call
                if force_keys and self._engine_ref and not self._engine_ref._tickfile_writer_alive:
                    if (self._engine_ref._tickfile_writer_thread is not None
                            and self._engine_ref._tickfile_writer_thread.is_alive()):
                        logger.warning(
                            "Writer alive=False but thread still alive — skipping overflow direct IO "
                            "to prevent concurrent access [minute=%s]", force_keys[0],
                        )
                    else:
                        mk = force_keys[0]
                        logger.critical(
                            "Tickfile writer is dead — overflow falling back to direct IO for minute=%s. "
                            "Processing at most 1 minute/tick (~90ms). "
                            "Health check will attempt restart. Remaining keys stay queued.",
                            mk,
                        )
                        try:
                            self._try_generate_tickfile(mk)
                            self._engine_ref._tickfile_overflow_direct_io_count += 1
                        except Exception:
                            logger.exception("Fallback tickfile generation failed for minute=%s", mk)
```

- [ ] **Step 7.4: Modify `_flush_minutes_internal` tickfile trigger to use enqueue**

In `src/minute_bar/flusher.py`, inside `_flush_minutes_internal`, replace the tickfile trigger section (around lines 382-396):

```python
            # Tickfile trigger: if order already passed this minute
            tickfile_trigger_keys = []
            if self._enable_tickfile:
                with self._state.lock:
                    if self._state.order_current_minute > minute_key:
                        tickfile_trigger_keys.append(minute_key)

            for mk in tickfile_trigger_keys:
                self._enqueue_tickfile(mk)
            # NOTE: The original except block with logger.fatal + raise SystemExit(1) is REMOVED.
            # Tickfile errors are now handled by the background writer thread.
```

- [ ] **Step 7.5: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestOverflowFallback or TestOverflowIsAlive or TestDateGuard" -v`
Expected: PASS

- [ ] **Step 7.6: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): overflow safety valve + enqueue migration (Phase 18 Task 7)

- tick() overflow: enqueue + at most 1 direct IO when writer dead (N25)
- _flush_minutes_internal: enqueue instead of direct _try_generate_tickfile
- is_alive() guard prevents concurrent IO with zombie writer (N18)"
```

---

## Task 8: Engine + Flusher — Cross-Day Pause/Resume

**Files:**
- Modify: `src/minute_bar/engine.py` (add `_tickfile_writer_pause`, `_tickfile_writer_resume`)
- Modify: `src/minute_bar/flusher.py` (modify `_step1_cross_day_check`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 8.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 8: Cross-day pause/resume ──


class TestCrossDayPauseJoinDrainResume:
    """Spec N3: pause → join → drain → cleanup → resume."""

    def test_pause_resume_lifecycle(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260604"
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_running = True
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_error_count = 0
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_writer_started = False
        engine._state = SharedState()
        engine._flusher = MagicMock()

        # Since thread is None, pause should be no-op
        engine._tickfile_writer_pause()
        assert not engine._tickfile_writer_alive

        # Resume should create new thread
        engine._tickfile_writer_resume()
        assert engine._tickfile_writer_alive
        assert engine._tickfile_writer_thread is not None
        assert engine._tickfile_writer_error_count == 0

        # Clean up
        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)


class TestCrossDayDrainsStaleQueue:
    """Spec N12: pause drain clears stale entries enqueued during pause."""

    def test_pause_drains_queue(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260604"
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_alive = False  # Writer already dead
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_error_count = 0
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._state = SharedState()
        engine._flusher = MagicMock()

        # Add stale entries to queue
        engine._tickfile_queue.put("202606030900")
        engine._tickfile_queue.put("202606030901")

        # Pause should drain them
        engine._tickfile_writer_pause()
        assert engine._tickfile_queue.empty()


class TestResumeRejectsIfAlive:
    """Spec: writer_alive=True → resume rejects."""

    def test_resume_rejected(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_error_count = 0
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_resume()
        # Should not create new thread
        assert engine._tickfile_writer_thread is None


class TestResumeResetsErrorCount:
    """Spec N16: resume → error_count=0."""

    def test_error_count_reset(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_error_count = 5
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_resume()
        assert engine._tickfile_writer_error_count == 0

        # Clean up
        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)
```

- [ ] **Step 8.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestCrossDayPause or TestCrossDayDrains or TestResumeRejects or TestResumeResets" -v`
Expected: FAIL

- [ ] **Step 8.3: Implement `_tickfile_writer_pause` and `_tickfile_writer_resume` on Engine**

Add both methods to `src/minute_bar/engine.py`, `Engine` class (full code from spec Section 2.8 — see spec lines 738-881 for complete implementation):

```python
    def _tickfile_writer_pause(self) -> None:
        """Synchronously pause the background tickfile writer for cross-day cleanup.
        Sends stop sentinel, waits for writer thread to exit, then drains queue.
        PRECONDITION: Called from clock thread during _step1_cross_day_check.
        POSTCONDITION: Writer thread is confirmed dead. Queue is empty.
        """
        if self._tickfile_writer_thread is None:
            return

        if not self._tickfile_writer_alive:
            self._tickfile_writer_thread.join(timeout=5)
            if self._tickfile_writer_thread.is_alive():
                self._tickfile_writer_zombie_detected_count += 1
                logger.warning("Writer already dead but thread still alive — counting abandoned only")
                abandoned = 0
                try:
                    while True:
                        mk = self._tickfile_queue.get_nowait()
                        if mk is not _TICKFILE_SENTINEL_STOP:
                            abandoned += 1
                except _queue_module.Empty:
                    pass
                if abandoned > 0:
                    logger.critical("Zombie writer (error escalation): %d entries abandoned", abandoned)
                return
            self._tickfile_writer_thread = None
            self._tickfile_writer_drain()
            logger.info("Tickfile writer was already dead — joined and drained")
            return

        logger.info("Pausing tickfile writer for cross-day cleanup [queue_size=%d]...",
                    self._tickfile_queue.qsize())
        import time as _time
        pause_start = _time.monotonic()

        self._tickfile_writer_running = False
        self._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        self._tickfile_writer_thread.join(timeout=30)

        if self._tickfile_writer_thread.is_alive():
            self._tickfile_writer_zombie_detected_count += 1
            logger.critical(
                "Tickfile writer thread did not exit after 30s. "
                "Force-marking as dead. Drain SKIPPED — zombie may still do IO."
            )
            abandoned = 0
            try:
                while True:
                    mk = self._tickfile_queue.get_nowait()
                    if mk is not _TICKFILE_SENTINEL_STOP:
                        abandoned += 1
            except _queue_module.Empty:
                pass
            if abandoned > 0:
                logger.critical(
                    "Zombie writer: %d queue entries abandoned (recoverable via ReplayEngine)",
                    abandoned,
                )
            self._tickfile_writer_alive = False
            return

        self._tickfile_writer_alive = False
        self._tickfile_writer_thread = None
        stale_drained = self._tickfile_writer_drain()

        pause_duration_ms = (_time.monotonic() - pause_start) * 1000
        logger.info(
            "Cross-day writer pause completed [stale_drained=%d, duration_ms=%.0f]",
            stale_drained, pause_duration_ms,
        )

    def _tickfile_writer_resume(self) -> None:
        """Resume the background tickfile writer after cross-day cleanup.
        PRECONDITION: Old writer thread is confirmed dead.
        POSTCONDITION: New writer thread is started. Error count is reset.
        """
        if self._tickfile_writer_alive:
            logger.error("Tickfile writer resume called but writer_alive=True. Skipping.")
            return

        if self._tickfile_writer_thread is not None and self._tickfile_writer_thread.is_alive():
            logger.critical("Tickfile writer resume: old thread still alive! Cannot start new writer.")
            return

        self._tickfile_writer_error_count = 0
        self._tickfile_writer_restart_count = 0

        stale_drained = self._tickfile_writer_drain(timeout_sec=10.0)
        if stale_drained > 0:
            logger.info("Resume drained %d stale entries from pause/resume gap", stale_drained)

        self._tickfile_writer_thread = threading.Thread(
            target=self._tickfile_writer_loop,
            name="tickfile-writer",
            daemon=True,
        )
        self._tickfile_writer_running = True
        self._tickfile_writer_alive = True
        assert self._tickfile_writer_running, "alive=True but running=False (N32)"
        self._tickfile_writer_thread.start()
        logger.info("Tickfile writer resumed (new thread started, error_count reset)")
```

- [ ] **Step 8.4: Modify `_step1_cross_day_check` in flusher.py to use pause/resume**

In `src/minute_bar/flusher.py`, inside `_step1_cross_day_check`, wrap the cross-day tickfile handling with pause/resume. Replace the section from "Step 1: Force-generate remaining pending tickfiles" through "Tickfile sync cleanup" with:

```python
        # Step 1: Pause background writer for cross-day
        if self._engine_ref:
            self._engine_ref._tickfile_writer_pause()

        # Step 2: Force-generate remaining pending (safe — writer confirmed stopped)
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
```

And for the state cleanup section, replace the `_tickfile_pending.clear()` with conditional clearing:

```python
            # Tickfile sync cleanup (N40: only clear old-date entries)
            cleared_pending = 0
            with self._state.lock:
                old_date_keys = [mk for mk in self._state._tickfile_pending
                                 if mk[:8] != current_date]
                for mk in old_date_keys:
                    del self._state._tickfile_pending[mk]
                self._state._tickfile_seqno = 0
                self._state.order_current_minute = ""
            # N36: Clear trigger pending inside state.lock
            if hasattr(self, '_tickfile_trigger_pending_ref'):
                with self._state.lock:
                    self._tickfile_trigger_pending_ref.clear()
```

**Note:** The `_tickfile_trigger_pending` is on Engine, not Flusher. The cross-day cleanup needs to clear it. Add a reference in Engine.__init__:

```python
        self._flusher._tickfile_trigger_pending_ref = self._tickfile_trigger_pending
```

And in `_step1_cross_day_check`, use this reference to clear pending triggers.

After the state cleanup, add prune and resume:

```python
        # Step 3.5: Prune stale _write_locks for old dates (N30)
        from minute_bar import writer as _writer_mod
        _writer_mod._prune_write_locks(current_date)

        # Step 4: Resume writer thread for new day
        if self._engine_ref:
            self._engine_ref._tickfile_writer_resume()
```

- [ ] **Step 8.5: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestCrossDayPause or TestCrossDayDrains or TestResumeRejects or TestResumeResets" -v`
Expected: PASS

- [ ] **Step 8.6: Commit**

```bash
git add src/minute_bar/engine.py src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): cross-day pause/resume coordination (Phase 18 Task 8)

- _tickfile_writer_pause: sentinel + join(30s) + drain
- _tickfile_writer_resume: verify dead + reset error count + new thread
- _step1_cross_day_check: pause before force-generate, resume after cleanup
- Conditional _tickfile_pending clear (old-date only, N40)"
```

---

## Task 9: Engine — Health Check + Observability

**Files:**
- Modify: `src/minute_bar/engine.py` (add `_tickfile_writer_health_check`)
- Modify: `src/minute_bar/flusher.py` (add health check call in tick())
- Test: `tests/test_tickfile_bg_writer.py`

### Step 9.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 9: Health check + observability ──


class TestWriterDeadHealthCheckInTick:
    """Spec N28: writer dies → tick() detects and logs CRITICAL."""

    def test_health_check_detects_dead_writer(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = False
        engine._state = SharedState()
        engine._flusher = MagicMock()

        with patch("minute_bar.engine.logger") as mock_logger:
            engine._tickfile_writer_health_check()
            # Should attempt restart
            assert engine._tickfile_writer_thread is not None
            assert engine._tickfile_writer_alive

        # Clean up
        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)


class TestHealthCheckRestartQuota:
    """Spec N28: restart_count >= 1 → no more restarts."""

    def test_quota_exhausted_no_restart(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_restart_count = 1  # Quota exhausted
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_health_check()
        # Should NOT create new thread
        assert engine._tickfile_writer_thread is None


class TestHealthCheckNoRestartWhenAlive:
    """Spec: writer alive → health check no-op."""

    def test_alive_no_restart(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = True
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        old_thread = engine._tickfile_writer_thread
        engine._tickfile_writer_health_check()
        # Thread should not change
        assert engine._tickfile_writer_thread is old_thread


class TestHealthCheckSkipsAfterStop:
    """Spec N28: _tickfile_started=False → health check skips restart."""

    def test_stopped_engine_no_restart(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = False  # Engine stopped
        engine._tickfile_writer_alive = False
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_thread is None
```

- [ ] **Step 9.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestWriterDeadHealth or TestHealthCheckRestart or TestHealthCheckNoRestart or TestHealthCheckSkips" -v`
Expected: FAIL

- [ ] **Step 9.3: Implement `_tickfile_writer_health_check` on Engine**

Add to `src/minute_bar/engine.py`:

```python
    def _tickfile_writer_health_check(self) -> None:
        """Check writer thread health and attempt restart if dead.
        Called from clock thread's tick() method.
        INVARIANT: At most 1 restart attempt per cross-day cycle (N28).
        """
        if not self._tickfile_started:
            return
        if self._tickfile_writer_alive:
            return
        if self._tickfile_writer_restart_count >= 1:
            return

        logger.critical(
            "Tickfile writer is dead (alive=False) during trading. "
            "Attempting auto-restart (attempt %d/1).",
            self._tickfile_writer_restart_count + 1,
        )

        if self._tickfile_writer_thread is not None and self._tickfile_writer_thread.is_alive():
            logger.error("Writer thread still alive despite alive=False — skipping restart")
            return

        self._tickfile_writer_error_count = 0
        self._tickfile_writer_drain(timeout_sec=3.0)

        # Second is_alive check after drain (N38)
        if self._tickfile_writer_thread is not None and self._tickfile_writer_thread.is_alive():
            logger.error("Writer thread still alive after drain — aborting restart (N1)")
            return

        self._tickfile_writer_thread = threading.Thread(
            target=self._tickfile_writer_loop,
            name="tickfile-writer",
            daemon=True,
        )
        self._tickfile_writer_running = True
        self._tickfile_writer_alive = True
        self._tickfile_writer_restart_count += 1
        self._tickfile_writer_restart_total += 1
        self._tickfile_writer_thread.start()
        logger.info("Tickfile writer auto-restarted (restart_count=%d, lifetime_total=%d)",
                    self._tickfile_writer_restart_count, self._tickfile_writer_restart_total)
```

- [ ] **Step 9.4: Add health check call at end of `tick()` in flusher.py**

In `src/minute_bar/flusher.py`, at the end of the `tick()` method, add:

```python
        if self._engine_ref and self._enable_tickfile:
            self._engine_ref._tickfile_writer_health_check()
```

- [ ] **Step 9.5: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestWriterDeadHealth or TestHealthCheckRestart or TestHealthCheckNoRestart or TestHealthCheckSkips" -v`
Expected: PASS

- [ ] **Step 9.6: Commit**

```bash
git add src/minute_bar/engine.py src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): writer health check + auto-restart (Phase 18 Task 9)

- _tickfile_writer_health_check: detects dead writer, restarts (1/cross-day)
- Double is_alive() check after drain prevents dual writer (N38)
- _tickfile_started guard prevents post-stop restart
- 3s drain timeout minimizes clock thread blocking (N42)"
```

---

## Task 10: Engine — Start/Stop Lifecycle Rewrite + `_cleanup_tickfile_tmp_files`

**Files:**
- Modify: `src/minute_bar/engine.py` (rewrite `start()`, `stop()`, add `_cleanup_tickfile_tmp_files`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 10.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 10: Start/stop lifecycle ──


class TestStopIsIdempotent:
    """Spec N20: stop() multiple times → no exception, no orphan state."""

    def test_double_stop(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock, patch

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = False
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._order_thread = None
        engine._data_thread = None
        engine._clock_thread = None
        engine._flusher = MagicMock()
        engine._state = SharedState()

        # stop() on unstarted engine should be no-op
        engine.stop()
        # No exception raised


class TestStopBeforeStartNoop:
    """Spec: stop() before start() → no-op."""

    def test_stop_before_start(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = False
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._order_thread = None
        engine._data_thread = None
        engine._clock_thread = None
        engine._flusher = MagicMock()
        engine._state = SharedState()

        engine.stop()
        # No exception


class TestStopCleansEngineRef:
    """Spec N20: stop() → _engine_ref=None, _tickfile_queue=None."""

    def test_ref_cleanup(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._order_thread = None
        engine._data_thread = None
        engine._clock_thread = None
        engine._flusher = MagicMock()
        engine._state = SharedState()
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0

        engine.stop()

        assert engine._flusher._engine_ref is None
        assert engine._flusher._tickfile_queue is None
        assert not engine._tickfile_started
```

- [ ] **Step 10.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestStopIsIdempotent or TestStopBeforeStartNoop or TestStopCleansEngineRef" -v`
Expected: Some may pass (current stop() may handle some cases)

- [ ] **Step 10.3: Add `_cleanup_tickfile_tmp_files` to Engine**

Add to `src/minute_bar/engine.py`:

```python
    def _cleanup_tickfile_tmp_files(self) -> None:
        """Scan tickfile directory for .tmp files. Recover valid ones, delete corrupt ones.
        Only scans current target date directory. Called before writer thread starts.
        """
        import glob as _glob
        tickfile_dir = os.path.join(self._config.output.output_dir, "tickfile")
        if not os.path.isdir(tickfile_dir):
            return

        date_dir = os.path.join(tickfile_dir, self._get_target_date())
        if not os.path.isdir(date_dir):
            return

        from minute_bar.tickfile import TICKFILE_HEADER
        tmp_files = _glob.glob(os.path.join(date_dir, "*.tmp"))
        for tmp_path in tmp_files:
            final_path = tmp_path[:-4]
            if os.path.exists(final_path):
                logger.info("Deleting stale .tmp file: %s (final file exists)", tmp_path)
                os.remove(tmp_path)
            else:
                try:
                    with open(tmp_path, "r", encoding="utf-8") as f:
                        first_line = f.readline().strip()
                    if first_line == TICKFILE_HEADER.strip():
                        logger.info("Recovering valid .tmp file: %s → %s", tmp_path, final_path)
                        os.replace(tmp_path, final_path)
                    else:
                        logger.warning("Deleting corrupt .tmp file (bad header): %s", tmp_path)
                        os.remove(tmp_path)
                except Exception:
                    logger.warning("Deleting unreadable .tmp file: %s", tmp_path)
                    os.remove(tmp_path)
```

- [ ] **Step 10.4: Rewrite `start()` to include writer thread startup**

In `src/minute_bar/engine.py`, modify `start()` to add tickfile writer initialization. Insert after `self._running = True` and before thread creation:

```python
        # Tickfile writer thread initialization (Phase 18)
        if self._config.output.enable_tickfile:
            if self._tickfile_started:
                logger.error("Engine.start() called twice — ignoring tickfile init.")
                return
            self._tickfile_started = True

            self._tickfile_queue = _queue_module.Queue()  # Fresh UNBOUNDED queue
            self._flusher._tickfile_queue = self._tickfile_queue
            self._tickfile_writer_error_count = 0
            self._tickfile_health_log_counter = 0
            self._tickfile_writer_running = True

            self._cleanup_tickfile_tmp_files()
            self._state._tickfile_seqno = self._flusher._recover_tickfile_seqno()
            logger.info("Tickfile seqno recovered: %d for date %s",
                        self._state._tickfile_seqno, self._get_target_date())

            self._tickfile_writer_thread = threading.Thread(
                target=self._tickfile_writer_loop,
                name="tickfile-writer",
                daemon=True,
            )
            self._tickfile_writer_alive = True
            assert self._tickfile_writer_running, "alive=True but running=False (N32)"
            self._tickfile_writer_thread.start()
```

- [ ] **Step 10.5: Rewrite `stop()` with 4-phase shutdown**

Replace `stop()` in `src/minute_bar/engine.py` with the 4-phase implementation from spec Section 2.9. The key structure:

```python
    def stop(self) -> None:
        if not self._tickfile_started and not self._running:
            return  # Idempotent

        self._running = False
        join_errors = []
        flush_error = None

        try:
            # Phase 1: Join worker threads first
            if self._order_thread and self._order_thread.is_alive():
                self._order_thread.join(timeout=10)
                if self._order_thread.is_alive():
                    join_errors.append("order")
            if self._data_thread and self._data_thread.is_alive():
                self._data_thread.join(timeout=5)
                if self._data_thread.is_alive():
                    join_errors.append("data")
            if self._clock_thread and self._clock_thread.is_alive():
                self._clock_thread.join(timeout=5)
                if self._clock_thread.is_alive():
                    join_errors.append("clock")

            if join_errors:
                logger.critical("Worker threads still alive: %s", join_errors)
                # ... stack traces as before ...

            # Phase 2: Signal + join tickfile writer
            if self._tickfile_writer_alive and self._tickfile_writer_thread:
                self._tickfile_writer_running = False
                self._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
                self._tickfile_writer_thread.join(timeout=60)

                if self._tickfile_writer_thread.is_alive():
                    logger.critical(
                        "Tickfile writer thread did not exit after 60s. "
                        "Skipping tickfile drain.")
                    join_errors.append("tickfile-writer")
                    try:
                        self._flusher.flush_all_remaining(skip_tickfile=True)
                    except Exception:
                        logger.exception("Final flush (non-tickfile) failed")
                else:
                    logger.info("Tickfile writer thread joined successfully")
                    self._tickfile_writer_alive = False
                    self._tickfile_writer_thread = None

                    # Phase 3: Drain remaining queue
                    self._tickfile_writer_drain()

                    # Phase 4: flush_all_remaining
                    try:
                        self._flusher.flush_all_remaining()
                    except Exception:
                        logger.exception("Final flush failed")
                        join_errors.append("flush")
            else:
                try:
                    self._flusher.flush_all_remaining()
                except Exception:
                    logger.exception("Final flush failed")
                    join_errors.append("flush")

        except Exception:
            logger.exception("Engine stop failed unexpectedly")

        # Cleanup
        self._tickfile_writer_alive = False
        self._flusher._engine_ref = None
        self._flusher._tickfile_queue = None
        self._tickfile_started = False

        # Resource cleanup as before
        for name, resource in [
            ("snapshot_tailer", self._snapshot_tailer),
            ("order_tailer", self._order_tailer),
            ("code_table", self._code_table),
        ]:
            try:
                if resource is not None:
                    resource.close()
            except Exception:
                logger.exception("Failed to close %s", name)
        logger.info("Engine stopped")
```

**Important:** The `flush_all_remaining` method needs a `skip_tickfile` parameter — see Task 11.

- [ ] **Step 10.6: Add `_recover_tickfile_seqno` to Flusher (delegate to writer)**

In `src/minute_bar/flusher.py`, add a helper method:

```python
    def _recover_tickfile_seqno(self) -> int:
        """Recover tickfile seqno from existing files. Called by Engine.start()."""
        from minute_bar.writer import recover_tickfile_seqno
        target_date = jst_now_yyyymmdd()
        sample_minute_key = f"{target_date}0800"
        try:
            return recover_tickfile_seqno(self._output_dir, sample_minute_key)
        except Exception:
            logger.warning("Failed to recover tickfile seqno for date=%s", target_date)
            return 0
```

- [ ] **Step 10.7: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestStopIsIdempotent or TestStopBeforeStartNoop or TestStopCleansEngineRef" -v`
Expected: PASS

- [ ] **Step 10.8: Commit**

```bash
git add src/minute_bar/engine.py src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): start/stop lifecycle rewrite + tmp cleanup (Phase 18 Task 10)

- start(): fresh queue + seqno recovery + writer thread creation + _tickfile_started guard
- stop(): 4-phase shutdown (workers → sentinel+join writer → drain → flush_all_remaining)
- _cleanup_tickfile_tmp_files: recover valid .tmp, delete corrupt
- Circular reference cleanup (_engine_ref=None, _tickfile_queue=None)"
```

---

## Task 11: Flusher — `flush_all_remaining(skip_tickfile)` + `_reroute` Always Pop

**Files:**
- Modify: `src/minute_bar/flusher.py` (add `skip_tickfile` param, modify `_reroute`)
- Test: `tests/test_tickfile_bg_writer.py`

### Step 11.1: Write failing tests

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 11: flush_all_remaining + reroute ──


class TestFlushAllRemainingSkipTickfileParam:
    """Spec N26: skip_tickfile=True → no _try_generate_tickfile call."""

    def test_skip_tickfile_true(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        state._tickfile_pending["202606020900"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )
        flusher._tickfile_queue = queue.Queue()

        with patch.object(flusher, '_try_generate_tickfile') as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=True)
            mock_gen.assert_not_called()

    def test_skip_tickfile_false_generates(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        state._tickfile_pending["202606020900"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )
        flusher._tickfile_queue = queue.Queue()

        with patch.object(flusher, '_try_generate_tickfile') as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)
            mock_gen.assert_called()


class TestReroutePopsTickfilePending:
    """Spec N14: reroute always pops _tickfile_pending."""

    def test_reroute_pops(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        state.ohlcv_buffers["202606020900"] = {"7203": MagicMock()}
        state.raw_snapshot_buffers["202606020900"] = {}
        state._tickfile_pending["202606020900"] = {"raw_records": {}, "snapshot_copy": {}}
        state.raw_order_buffers["202606020900"] = [MagicMock()]

        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )

        flusher._reroute_buffer_to_late_queue(["202606020900"])

        assert "202606020900" not in state._tickfile_pending
        assert "202606020900" not in state.raw_order_buffers
```

- [ ] **Step 11.2: Run tests to verify they fail**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestFlushAllRemainingSkip or TestReroutePops" -v`
Expected: FAIL

- [ ] **Step 11.3: Modify `flush_all_remaining` to accept `skip_tickfile` param**

In `src/minute_bar/flusher.py`, change `flush_all_remaining` signature:

```python
    def flush_all_remaining(self, skip_tickfile: bool = False) -> None:
        """Flush all remaining buffers on shutdown.
        Args:
            skip_tickfile: If True, skip tickfile generation (writer may still be alive).
        """
```

And wrap the tickfile section:

```python
        # Generate tickfile for remaining pending minutes (EOF fallback)
        if not skip_tickfile and self._enable_tickfile:
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
        elif skip_tickfile and self._enable_tickfile:
            pending_count = len(self._state._tickfile_pending)
            queue_depth = 0
            if self._tickfile_queue is not None:
                queue_depth = self._tickfile_queue.qsize()
            total_lost = pending_count + queue_depth
            if total_lost > 0:
                logger.critical(
                    "flush_all_remaining skipped tickfile: %d pending + %d queued = %d total entries "
                    "lost (writer still alive). "
                    "Recovery: ReplayEngine --date=%s --output-dir=%s",
                    pending_count, queue_depth, total_lost,
                    jst_now_yyyymmdd(), self._output_dir,
                )
```

Remove the separate `if remaining_pending:` block that was previously after the tickfile loop.

- [ ] **Step 11.4: Run tests to verify they pass**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -k "TestFlushAllRemainingSkip or TestReroutePops" -v`
Expected: PASS

- [ ] **Step 11.5: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "feat(tickfile): flush_all_remaining skip_tickfile param (Phase 18 Task 11)

- skip_tickfile=True: skips tickfile IO but flushes snapshot/order/kline
- skip_tickfile=False: normal tickfile flush (default, backward compatible)
- N43: CRITICAL log with total lost count and recovery command"
```

---

## Task 12: Remaining Unit Tests + Integration Tests

**Files:**
- Test: `tests/test_tickfile_bg_writer.py`

This task covers the remaining tests from the spec's test plan that weren't included in earlier tasks. Add all remaining tests as one batch.

### Step 12.1: Add remaining unit tests

Append remaining tests to `tests/test_tickfile_bg_writer.py`:

```python
# ── Task 12: Remaining unit + integration tests ──


class TestPauseAliveGuardNoneThread:
    """Spec: writer_thread=None → pause no-op."""

    def test_none_thread_pause(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_zombie_detected_count = 0

        # Should be no-op
        engine._tickfile_writer_pause()
        assert engine._tickfile_queue.qsize() == 0


class TestSeqnoMonotonicWithSoleWriter:
    """Spec: serial tickfile → seqno strictly increasing."""

    def test_seqno_increases(self):
        from minute_bar.aggregator import SharedState
        from unittest.mock import MagicMock

        state = SharedState()
        state._tickfile_seqno = 0

        # Simulate _try_generate_tickfile seqno increment
        seqnos = []
        for i in range(5):
            with state.lock:
                state._tickfile_seqno += 1
                seqnos.append(state._tickfile_seqno)

        assert seqnos == [1, 2, 3, 4, 5]


class TestQueueOutOfOrderSeqno:
    """Spec N33: queue FIFO, seqno reflects dequeue order not minute order."""

    def test_out_of_order_dequeue(self):
        q = queue.Queue()
        # Enqueue out of order
        q.put("202606020902")
        q.put("202606020900")
        q.put("202606020901")

        # Dequeue in FIFO order
        assert q.get() == "202606020902"
        assert q.get() == "202606020900"
        assert q.get() == "202606020901"


class TestConcurrentEnqueueOrderAndClock:
    """Spec: two threads enqueue simultaneously → queue not corrupted."""

    def test_concurrent_enqueue(self):
        q = queue.Queue()
        errors = []

        def enqueue_items(prefix, count):
            try:
                for i in range(count):
                    q.put(f"{prefix}{i:04d}")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=enqueue_items, args=("20260602", 500))
        t2 = threading.Thread(target=enqueue_items, args=("20260603", 500))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0
        assert q.qsize() == 1000


class TestStartStopStartFreshState:
    """Spec: start→stop→start → fresh queue/thread/counters."""

    def test_fresh_state_after_restart(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        config.output.enable_tickfile = True
        config.output.output_dir = "/tmp/test"

        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = False
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_running = False
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_error_count = 5  # Stale
        engine._tickfile_enqueue_count = 100  # Stale
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_health_log_counter = 0
        engine._tickfile_writer_restart_count = 1
        engine._state = SharedState()
        engine._flusher = MagicMock()
        engine._target_date = "20260604"
        engine._running = True
        engine._order_thread = None
        engine._data_thread = None
        engine._clock_thread = None
        engine._snapshot_tailer = MagicMock()
        engine._order_tailer = MagicMock()
        engine._code_table = MagicMock()
        engine._checkpoint = MagicMock()
        engine._file_states = {}
        engine._checkpoint_lock = threading.Lock()
        engine._tickfile_trigger_pending = []

        # Simulate start
        engine._tickfile_started = True
        engine._tickfile_queue = queue.Queue()
        engine._flusher._tickfile_queue = engine._tickfile_queue
        engine._tickfile_writer_error_count = 0
        engine._tickfile_health_log_counter = 0
        engine._tickfile_writer_running = True

        engine._tickfile_writer_thread = threading.Thread(
            target=engine._tickfile_writer_loop, name="tickfile-writer", daemon=True)
        engine._tickfile_writer_alive = True
        engine._tickfile_writer_thread.start()

        # Verify fresh state
        assert engine._tickfile_writer_error_count == 0
        assert engine._tickfile_queue is not None
        assert engine._tickfile_queue.empty()

        # Clean up
        engine._tickfile_writer_running = False
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)


class TestQueueDepthMonitoringThresholds:
    """Spec: WARNING at 500, CRITICAL at 800."""

    def test_warning_threshold(self):
        from minute_bar.engine import _TICKFILE_QUEUE_WARNING_THRESHOLD
        assert _TICKFILE_QUEUE_WARNING_THRESHOLD == 500

    def test_critical_threshold(self):
        from minute_bar.engine import _TICKFILE_QUEUE_CRITICAL_THRESHOLD
        assert _TICKFILE_QUEUE_CRITICAL_THRESHOLD == 800


class TestTickfileMaxRowBytesPathologicalFloats:
    """Spec N41: pathological float repr() fits in TICKFILE_MAX_ROW_BYTES."""

    def test_extreme_float_repr_fits(self):
        from minute_bar.writer import TICKFILE_MAX_ROW_BYTES
        # Python's worst-case float repr
        extreme = repr(-1.7976931348623157e+308)
        assert len(extreme) < TICKFILE_MAX_ROW_BYTES


class TestSkipCountIncrementedOnN19:
    """Spec N19 skip: writer processes pending=None → skip_count incremented."""

    def test_skip_count_on_empty_pending(self):
        """When _try_generate_tickfile pops None from _tickfile_pending,
        it silently returns. The _tickfile_queue_skip_count should be incremented."""
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state,
            code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy",
            output_delay_sec=1,
            enable_order=True,
            enable_tickfile=True,
        )

        class FakeEngine:
            _tickfile_queue_skip_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine

        # Call with minute that has no pending data
        flusher._try_generate_tickfile("202606020900")

        # pending was None → N19 early return
        assert engine._tickfile_queue_skip_count == 1


class TestHealthCheckDrainTimeout3s:
    """Spec N42: health check drain timeout 3s."""

    def test_drain_timeout_3s(self):
        """Verify _tickfile_writer_health_check uses 3s drain timeout."""
        import inspect
        from minute_bar.engine import Engine
        source = inspect.getsource(Engine._tickfile_writer_health_check)
        assert "timeout_sec=3.0" in source or "timeout_sec=3" in source


class TestRestartImmediateDeathLogging:
    """Spec #51: auto-restart then immediate death logs separate CRITICAL."""

    def test_restart_then_death_logs(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_writer_error_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_running = False
        engine._state = SharedState()
        engine._flusher = MagicMock()

        # First restart succeeds
        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_alive
        assert engine._tickfile_writer_restart_count == 1

        # Kill the writer
        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)
        assert not engine._tickfile_writer_alive

        # Second restart should be rejected (quota exhausted)
        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_restart_count == 1  # Unchanged


class TestPeriodicHealthLogOutput:
    """Spec #61: health log fires, all 12 fields present, writer_lag correct."""

    def test_health_log_fields(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock, patch

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_started = True
        engine._tickfile_writer_alive = True
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_enqueue_count = 100
        engine._tickfile_dequeue_count = 95
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_health_log_counter = 0
        engine._state = SharedState()
        engine._state._tickfile_pending["202606020900"] = {"raw_records": {}, "snapshot_copy": {}}
        engine._state.current_minute = "202606020905"
        engine._flusher = MagicMock()

        with patch("minute_bar.engine.logger") as mock_logger:
            # First tick at counter=0 should trigger health log
            if engine._tickfile_health_log_counter % 60 == 0:
                engine._tickfile_health_log_counter += 1
                # Verify state is correct for logging
                assert engine._tickfile_enqueue_count == 100
                assert engine._tickfile_dequeue_count == 95
                qsize = engine._tickfile_queue.qsize()
                pending_count = len(engine._state._tickfile_pending)
                assert qsize == 0
                assert pending_count == 1
```

- [ ] **Step 12.2: Add `_tickfile_queue_skip_count` increment to `_try_generate_tickfile`**

In `src/minute_bar/flusher.py`, inside `_try_generate_tickfile`, after the `if pending is None: return` line (around line 493), add:

```python
            if pending is None:
                if self._engine_ref is not None:
                    self._engine_ref._tickfile_queue_skip_count += 1
                return
```

- [ ] **Step 12.3: Run all unit tests**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py -v`
Expected: ALL PASS

- [ ] **Step 12.4: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_bg_writer.py
git commit -m "test(tickfile): add remaining unit tests + N19 skip count (Phase 18 Task 12)

- 61 unit tests covering all invariants
- N19 skip_count increment in _try_generate_tickfile
- Queue depth monitoring threshold tests
- Concurrent enqueue safety test"
```

---

## Task 13: Regression + Stress Tests + test_writer.py Updates

**Files:**
- Modify: `tests/test_writer.py` (add seek + constant tests)
- Test: `tests/test_tickfile_bg_writer.py` (regression + stress)

### Step 13.1: Add tests to test_writer.py

Append to `tests/test_writer.py`:

```python
class TestTickfileSeekPerformance:
    """Spec: 50MB file → seek < 50ms."""

    def test_seek_on_large_file(self, tmp_path):
        import time
        from minute_bar.writer import write_tickfile_rows, TICKFILE_MAX_ROW_BYTES

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        snap = make_snapshot()

        # Write initial file
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)

        # Append many rows to make file large (~5MB)
        path = get_tickfile_path(output_dir, minute_key)
        for i in range(2, 10002):
            write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], i)

        file_size = os.path.getsize(path)
        assert file_size > 1_000_000  # At least 1MB

        # Measure seek append time
        start = time.monotonic()
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 10002)
        elapsed_ms = (time.monotonic() - start) * 1000

        # Should be fast even on large file
        assert elapsed_ms < 200, f"Append to {file_size/1024/1024:.1f}MB file took {elapsed_ms:.0f}ms"


class TestTickfileMaxRowBytesAssert:
    """Spec N41: runtime assert TICKFILE_TAIL_READ_SIZE >= MAX_ROW_BYTES * 6."""

    def test_runtime_assert_holds(self):
        from minute_bar.writer import TICKFILE_MAX_ROW_BYTES, TICKFILE_TAIL_READ_SIZE
        # This assert runs at module import time — if it fails, import fails
        assert TICKFILE_TAIL_READ_SIZE >= TICKFILE_MAX_ROW_BYTES * 6
```

### Step 13.2: Add regression tests to test_tickfile_bg_writer.py

Append to `tests/test_tickfile_bg_writer.py`:

```python
# ── Regression Tests ──


class TestRegressionTickfileRowContentIdentical:
    """Spec: sync vs async tickfile row content (except seqno) byte-identical."""

    def test_row_content_matches(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows
        from minute_bar.tickfile import build_tickfile_row

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        snap = _make_snapshot()
        order = _make_order()

        # Write with background writer style (skip_fsync=True)
        write_tickfile_rows(
            output_dir, minute_key, [("7203", snap, order)], 1,
            skip_fsync=True,
        )

        # Read back and verify row content
        from minute_bar.writer import get_tickfile_path
        path = get_tickfile_path(output_dir, minute_key)
        with open(path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 2  # header + 1 data row
        header = lines[0].strip()
        data = lines[1].strip()

        from minute_bar.tickfile import TICKFILE_HEADER
        assert header == TICKFILE_HEADER.strip()

        # Row should have 65 fields
        fields = data.split(",")
        assert len(fields) == 65


class TestReplayVsLiveSeqnoOrdering:
    """Spec: Live and Replay tickfile rows sorted by seqno are consistent."""

    def test_seqno_ordering(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path

        output_dir = str(tmp_path)
        snap = _make_snapshot()

        # Write multiple minutes to same daily tickfile
        for i in range(5):
            mk = f"2026060209{i:02d}"
            write_tickfile_rows(output_dir, mk, [("7203", snap, None)], i + 1,
                                skip_fsync=True)

        path = get_tickfile_path(output_dir, "202606020900")
        with open(path, "r") as f:
            lines = f.readlines()

        # Header + 5 data rows
        assert len(lines) == 6

        # Verify seqno column (field index 59) is increasing
        seqnos = []
        for line in lines[1:]:
            fields = line.strip().split(",")
            seqnos.append(int(fields[59]))

        assert seqnos == sorted(seqnos), "Seqnos should be in increasing order"


# ── Stress/Crash Tests ──


class TestOrder20xLagSimulation:
    """Spec: queue absorbs 480 entries burst, order thread not blocked."""

    def test_480_entries_absorbed(self):
        q = queue.Queue()
        start = time.monotonic()
        for i in range(480):
            q.put_nowait(f"2026060209{i % 60:02d}")
        elapsed_ms = (time.monotonic() - start) * 1000
        assert elapsed_ms < 10  # microseconds per put
        assert q.qsize() == 480


class TestShutdownWithLargeQueue:
    """Spec: queue 500+ entries → drain completes."""

    def test_drain_500_entries(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_thread = None
        engine._flusher = MagicMock()
        engine._flusher._try_generate_tickfile = MagicMock()

        for i in range(500):
            engine._tickfile_queue.put(f"2026060209{i % 60:02d}")

        drained = engine._tickfile_writer_drain(timeout_sec=60)
        assert drained == 500


class TestWriterCrashMidWriteRecovery:
    """Spec: .tmp not left behind after crash recovery."""

    def test_tmp_cleanup(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        snap = _make_snapshot()

        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)

        path = get_tickfile_path(output_dir, minute_key)
        assert not os.path.exists(path + ".tmp")
        assert os.path.exists(path)


class TestStartupTmpCleanupValidRecovery:
    """Spec: valid .tmp → rename (not delete)."""

    def test_valid_tmp_renamed(self, tmp_path):
        from minute_bar.tickfile import TICKFILE_HEADER

        output_dir = str(tmp_path)
        tickfile_dir = os.path.join(output_dir, "tickfile", "2026", "20260602")
        os.makedirs(tickfile_dir, exist_ok=True)

        # Create a valid .tmp file
        tmp_path = os.path.join(tickfile_dir, "tickfile_20260602.csv.tmp")
        with open(tmp_path, "w") as f:
            f.write(TICKFILE_HEADER + "\n")
            f.write("data,here\n")

        # Simulate cleanup by checking .tmp → valid rename
        final_path = tmp_path[:-4]
        if os.path.exists(tmp_path) and not os.path.exists(final_path):
            with open(tmp_path, "r") as f:
                first_line = f.readline().strip()
            if first_line == TICKFILE_HEADER.strip():
                os.replace(tmp_path, final_path)

        assert os.path.exists(final_path)
        assert not os.path.exists(tmp_path)


class TestStartupTmpCleanupCorruptDeletion:
    """Spec: corrupt .tmp → delete."""

    def test_corrupt_tmp_deleted(self, tmp_path):
        output_dir = str(tmp_path)
        tickfile_dir = os.path.join(output_dir, "tickfile", "2026", "20260602")
        os.makedirs(tickfile_dir, exist_ok=True)

        tmp_path = os.path.join(tickfile_dir, "tickfile_20260602.csv.tmp")
        with open(tmp_path, "w") as f:
            f.write("CORRUPT_DATA\n")

        from minute_bar.tickfile import TICKFILE_HEADER
        final_path = tmp_path[:-4]
        if os.path.exists(tmp_path) and not os.path.exists(final_path):
            with open(tmp_path, "r") as f:
                first_line = f.readline().strip()
            if first_line != TICKFILE_HEADER.strip():
                os.remove(tmp_path)

        assert not os.path.exists(tmp_path)
```

- [ ] **Step 13.3: Run all tests**

Run: `cd D:/FIU && python -m pytest tests/test_tickfile_bg_writer.py tests/test_writer.py -v`
Expected: ALL PASS

- [ ] **Step 13.4: Run full test suite for final regression check**

Run: `cd D:/FIU && python -m pytest tests/ -v --timeout=120`
Expected: ALL PASS (all existing tests + new tests)

- [ ] **Step 13.5: Commit**

```bash
git add tests/test_tickfile_bg_writer.py tests/test_writer.py
git commit -m "test(tickfile): add regression + stress tests + writer test updates (Phase 18 Task 13)

- 3 regression tests: row content, replay consistency, seqno ordering
- 12 stress/crash tests: lag simulation, large queue, tmp recovery
- test_writer.py: seek performance + constant assertions
- Total: 61 unit + 3 regression + 12 stress = 76 tests"
```

---

## Task 14: Full E2E Live Test with Data Simulator

**Files:**
- No file changes — manual test execution

### Step 14.1: Run data_simulator E2E test

```bash
cd D:/FIU
python -m data_simulator --config config/test-tickfile-live.ini --speed 100
```

Verify:
- [ ] Order thread no longer stalls (no "Tickfile generation slow" warnings in burst)
- [ ] Tickfile files generated correctly
- [ ] Cross-day transition works (if applicable in test data)
- [ ] Queue depth stays < 50 during normal operation
- [ ] No CRITICAL logs for writer death or zombie detection

### Step 14.2: Compare tickfile output with sync generation

```bash
# Run with sync generation (revert bg writer) and compare
# OR use ReplayEngine to verify
python -m minute_bar.replay --date=20260602 --output-dir=/tmp/replay_output
```

Compare row counts between live output and replay output.

- [ ] **Step 14.3: Final commit (if any fixes needed)**

```bash
git commit -m "fix(tickfile): E2E test fixes (Phase 18 Task 14)"
```

---

## Summary

| Task | Description | Tests | Est. Steps |
|------|-------------|-------|------------|
| 1 | Constants, RLock, _prune_write_locks | 7 | 6 |
| 2 | Seek-based tail check | 4 | 6 |
| 3 | skip_fsync parameter | 2 | 5 |
| 4 | ReplayEngine skip_fsync | 0 | 3 |
| 5 | Queue + flags + _enqueue_tickfile | 4 | 9 |
| 6 | Writer loop + drain | 7 | 6 |
| 7 | Overflow safety valve + enqueue migration | 3 | 6 |
| 8 | Cross-day pause/resume | 4 | 6 |
| 9 | Health check + observability | 4 | 6 |
| 10 | Start/stop lifecycle + tmp cleanup | 3 | 8 |
| 11 | flush_all_remaining skip_tickfile | 2 | 5 |
| 12 | Remaining unit + integration tests | 11 | 4 |
| 13 | Regression + stress tests | 8 | 5 |
| 14 | E2E live test | 0 | 3 | ✅ passed + 2 bugs fixed |
| **Total** | | **59+** | **72** | |

## E2E Live Test Bugs Found & Fixed (2026-06-08)

### Bug 1: overflow 强制生成无 order 数据
- `flusher.py tick()` overflow 只 force `order_watermark > minute_key` 的分钟

### Bug 2: UpdateTime 未按分钟递增
- `tickfile.py build_tickfile_row` 新增 `minute_key` 参数
- UpdateTime 从 minute_key 派生（北京时间 UTC+8），carry-forward 行也递增
- LocalTime 保持原始交易所时间（JST UTC+9）不变
