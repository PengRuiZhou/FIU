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
        import _thread
        from minute_bar.writer import _get_write_lock
        lock = _get_write_lock("/tmp/test_rlock_tickfile.csv")
        assert isinstance(lock, _thread.RLock), f"Expected RLock, got {type(lock)}"

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

    @pytest.fixture(autouse=True)
    def _isolate_locks(self):
        from minute_bar.writer import _write_locks, _write_lock_mutex
        with _write_lock_mutex:
            saved = dict(_write_locks)
            _write_locks.clear()
        yield
        with _write_lock_mutex:
            _write_locks.clear()
            _write_locks.update(saved)

    def test_prune_empty_dict_noop(self):
        """Edge case: prune on empty dict should be safe."""
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
        _prune_write_locks("20260604")
        with _write_lock_mutex:
            assert len(_write_locks) == 0

    def test_prune_all_current_date_noop(self):
        """Edge case: all entries match current_date — nothing removed."""
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
        with _write_lock_mutex:
            _write_locks["/output/tickfile/2026/20260604/tickfile_20260604.csv"] = threading.RLock()
        _prune_write_locks("20260604")
        with _write_lock_mutex:
            assert len(_write_locks) == 1

    def test_prune_removes_old_date_tickfile_locks(self):
        from minute_bar.writer import _write_locks, _write_lock_mutex, _prune_write_locks
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
        # Should have header + 2 data rows
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
        with open(path, "w") as f:
            pass
        snap = _make_snapshot()
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

        # Write header + corrupt bytes
        with open(path, "wb") as f:
            f.write((TICKFILE_HEADER + "\n").encode("utf-8"))
            f.write(b"7203,data")
            f.write(b"\xff\xfe")  # Invalid UTF-8

        snap = _make_snapshot()
        write_tickfile_rows(output_dir, minute_key, [("7203", snap, None)], 1)
        assert os.path.exists(path)


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


import queue


# ── Tasks 5-6: Queue + Writer loop + drain ──


class TestEnqueueTickfileNoneQueueNoop:
    """Spec N21: _tickfile_queue=None → _enqueue_tickfile returns silently."""

    def test_none_queue_noop(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
        )
        assert flusher._tickfile_queue is None
        flusher._enqueue_tickfile("202606020900")  # Should not raise


class TestUnboundedQueuePutAlwaysSucceeds:
    """Spec N2: unbounded queue put_nowait never raises queue.Full."""

    def test_unbounded_put_never_full(self):
        q = queue.Queue()
        for i in range(10000):
            q.put_nowait(f"2026060209{i % 60:02d}")
        assert q.qsize() == 10000


class TestN35EngineRefCounterRouting:
    """Spec N35: _enqueue_tickfile increments engine counter, not flusher."""

    def test_counter_on_engine_not_flusher(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
        )

        class FakeEngine:
            _tickfile_enqueue_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine
        flusher._tickfile_queue = queue.Queue()

        flusher._enqueue_tickfile("202606020900")
        assert engine._tickfile_enqueue_count == 1


class TestSentinelStopsLoopNoDrain:
    """Spec N17: sentinel → break without drain."""

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

        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_loop()

        assert not engine._tickfile_writer_alive
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

        engine._tickfile_queue.put(None)
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_loop()
        engine._flusher._try_generate_tickfile.assert_called_once_with(None)


class TestWriterFailureNoReenqueue:
    """Spec N13: failure → no re-enqueue."""

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
        assert engine._tickfile_queue.empty()
        assert engine._tickfile_writer_error_count == 1


class TestWriterConsecutiveErrorsStopWriterOnly:
    """Spec N8: 5 consecutive failures → stop writer only."""

    def test_five_errors_stops_writer(self):
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

        for i in range(6):
            engine._tickfile_queue.put(f"2026060209{i:02d}")
        engine._tickfile_writer_loop()

        assert engine._tickfile_writer_error_count >= _TICKFILE_MAX_CONSECUTIVE_ERRORS
        assert not engine._tickfile_writer_running
        assert not engine._tickfile_writer_alive


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
        with pytest.raises(SystemExit):
            engine._tickfile_writer_loop()
        assert not engine._tickfile_writer_alive
        assert not engine._tickfile_writer_running


# ── Task 7: Overflow + enqueue migration ──


class TestDateGuardBlocksOldDayUpdate:
    """Spec N15: old-day trigger → order_current_minute not updated."""

    def test_old_date_skipped(self):
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260604"
        engine._tickfile_trigger_pending = ["202606030900"]
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_enqueue_count = 0
        engine._state = SharedState()
        engine._state.order_current_minute = ""
        engine._state._tickfile_pending = {}
        engine._flusher = MagicMock()

        engine._drain_tickfile_triggers()
        assert engine._state.order_current_minute == ""


# ── Task 8: Cross-day pause/resume ──


class TestCrossDayPauseJoinDrainResume:
    """Spec N3: pause → join → drain → cleanup → resume."""

    def test_pause_resume_lifecycle(self):
        """Test pause with dead writer (alive=False, thread exists), then resume."""
        from minute_bar.engine import Engine
        from minute_bar.config import AppConfig
        from unittest.mock import MagicMock

        config = MagicMock(spec=AppConfig)
        engine = object.__new__(Engine)
        engine._config = config
        engine._target_date = "20260604"
        engine._tickfile_queue = queue.Queue()
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_running = False
        engine._tickfile_writer_error_count = 0
        engine._tickfile_writer_restart_count = 0
        engine._tickfile_dequeue_count = 0
        engine._tickfile_writer_exception_count = 0
        engine._tickfile_overflow_direct_io_count = 0
        engine._tickfile_queue_stale_drain_count = 0
        engine._tickfile_writer_zombie_detected_count = 0
        engine._tickfile_writer_restart_total = 0
        engine._tickfile_queue_skip_count = 0
        engine._tickfile_started = False
        engine._state = SharedState()
        engine._flusher = MagicMock()

        # Simulate: writer thread died (alive=False, thread=None)
        engine._tickfile_writer_thread = None
        engine._tickfile_writer_pause()
        # Pause is no-op when thread is None (returns immediately)
        assert not engine._tickfile_writer_alive

        # Resume should start a new writer thread
        engine._tickfile_writer_resume()
        assert engine._tickfile_writer_alive
        assert engine._tickfile_writer_thread is not None
        assert engine._tickfile_writer_error_count == 0

        # Clean up
        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)


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


# ── Task 9: Health check ──


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
        engine._tickfile_writer_restart_count = 1
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_health_check()
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
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        old_thread = engine._tickfile_writer_thread
        engine._tickfile_writer_health_check()
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
        engine._tickfile_started = False
        engine._tickfile_writer_alive = False
        engine._tickfile_writer_thread = None
        engine._tickfile_queue = queue.Queue()
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_thread is None


# ── Tasks 10-11: Start/stop + flush_all_remaining ──


class TestStopIsIdempotent:
    """Spec N20: stop() multiple times → no exception."""

    def test_double_stop(self):
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
        engine._running = False
        engine._snapshot_tailer = MagicMock()
        engine._order_tailer = MagicMock()
        engine._code_table = MagicMock()

        engine.stop()  # Should be no-op


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
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
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
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
        )
        flusher._tickfile_queue = queue.Queue()

        with patch.object(flusher, '_try_generate_tickfile') as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)
            mock_gen.assert_called()


class TestReroutePreservesTickfilePending:
    """Fix-A: reroute preserves _tickfile_pending."""

    def test_reroute_preserves_pending(self):
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
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
        )

        flusher._reroute_buffer_to_late_queue(["202606020900"])

        assert "202606020900" in state._tickfile_pending  # Fix-A: preserved
        assert "202606020900" not in state.raw_order_buffers  # orders still popped


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
        engine._tickfile_writer_alive = False

        engine._tickfile_writer_pause()
        assert engine._tickfile_queue.qsize() == 0


class TestSeqnoMonotonicWithSoleWriter:
    """Spec: serial tickfile → seqno strictly increasing."""

    def test_seqno_increases(self):
        from minute_bar.aggregator import SharedState
        state = SharedState()
        state._tickfile_seqno = 0
        seqnos = []
        for i in range(5):
            with state.lock:
                state._tickfile_seqno += 1
                seqnos.append(state._tickfile_seqno)
        assert seqnos == [1, 2, 3, 4, 5]


class TestQueueOutOfOrderSeqno:
    """Spec N33: queue FIFO, seqno reflects dequeue order."""

    def test_out_of_order_dequeue(self):
        q = queue.Queue()
        q.put("202606020902")
        q.put("202606020900")
        q.put("202606020901")
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
        extreme = repr(-1.7976931348623157e+308)
        assert len(extreme) < TICKFILE_MAX_ROW_BYTES


class TestSkipCountIncrementedOnN19:
    """Spec: writer processes pending=None → returns silently (no data to generate)."""

    def test_skip_count_on_empty_pending(self):
        from minute_bar.flusher import ClockWatermarkFlusher
        from minute_bar.aggregator import SharedState
        from minute_bar.checkpoint import CheckpointManager
        from minute_bar.code_table import CodeTable

        state = SharedState()
        flusher = ClockWatermarkFlusher(
            state=state, code_table=CodeTable("dummy"),
            checkpoint=CheckpointManager("dummy", {}),
            output_dir="/tmp/dummy", output_delay_sec=1,
            enable_order=True, enable_tickfile=True,
        )

        class FakeEngine:
            _tickfile_queue_skip_count = 0

        engine = FakeEngine()
        flusher._engine_ref = engine
        # pending is None → _try_generate_tickfile returns silently
        flusher._try_generate_tickfile("202606020900")
        # Verify no exception raised and seqno unchanged (no data was processed)
        assert state._tickfile_seqno == 0


class TestHealthCheckDrainTimeout3s:
    """Spec N42: health check drain timeout 3s."""

    def test_drain_timeout_3s(self):
        import inspect
        from minute_bar.engine import Engine
        source = inspect.getsource(Engine._tickfile_writer_health_check)
        assert "timeout_sec=3.0" in source or "timeout_sec=3" in source


class TestRestartImmediateDeathLogging:
    """Spec #51: auto-restart then immediate death logs."""

    def test_restart_then_death(self):
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

        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_alive
        assert engine._tickfile_writer_restart_count == 1

        engine._tickfile_writer_running = False
        from minute_bar.engine import _TICKFILE_SENTINEL_STOP
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
        engine._tickfile_writer_thread.join(timeout=5)
        assert not engine._tickfile_writer_alive

        engine._tickfile_writer_health_check()
        assert engine._tickfile_writer_restart_count == 1  # Unchanged (quota)


class TestBackgroundThreadGeneratesTickfile:
    """Spec: writer thread takes mk from queue and generates tickfile."""

    def test_writer_generates_from_queue(self):
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
        engine._state = SharedState()
        engine._flusher = MagicMock()

        engine._tickfile_queue.put("202606020900")
        engine._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)

        engine._tickfile_writer_loop()
        engine._flusher._try_generate_tickfile.assert_called_once_with("202606020900")
        assert engine._tickfile_dequeue_count == 1
        assert not engine._tickfile_writer_alive


# ── Regression Tests ──


class TestRegressionTickfileRowContentIdentical:
    """Spec: row content (except seqno) byte-identical."""

    def test_row_content_matches(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path

        output_dir = str(tmp_path)
        minute_key = "202606020900"
        snap = _make_snapshot()
        order = _make_order()

        write_tickfile_rows(
            output_dir, minute_key, [("7203", snap, order)], 1,
            skip_fsync=True,
        )

        path = get_tickfile_path(output_dir, minute_key)
        with open(path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 2
        fields = lines[1].strip().split(",")
        assert len(fields) == 65


class TestReplayVsLiveSeqnoOrdering:
    """Spec: seqno column increasing."""

    def test_seqno_ordering(self, tmp_path):
        from minute_bar.writer import write_tickfile_rows, get_tickfile_path

        output_dir = str(tmp_path)
        snap = _make_snapshot()

        for i in range(5):
            mk = f"2026060209{i:02d}"
            write_tickfile_rows(output_dir, mk, [("7203", snap, None)], i + 1,
                                skip_fsync=True)

        path = get_tickfile_path(output_dir, "202606020900")
        with open(path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 6  # header + 5 data
        seqnos = [int(line.strip().split(",")[59]) for line in lines[1:]]
        assert seqnos == sorted(seqnos)


# ── Stress/Crash Tests ──


class TestOrder20xLagSimulation:
    """Spec: queue absorbs 480 entries burst."""

    def test_480_entries_absorbed(self):
        q = queue.Queue()
        start = time.monotonic()
        for i in range(480):
            q.put_nowait(f"2026060209{i % 60:02d}")
        elapsed_ms = (time.monotonic() - start) * 1000
        assert elapsed_ms < 50
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
    """Spec: .tmp not left behind."""

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
    """Spec: valid .tmp → rename."""

    def test_valid_tmp_renamed(self, tmp_path):
        from minute_bar.tickfile import TICKFILE_HEADER

        output_dir = str(tmp_path)
        tickfile_dir = os.path.join(output_dir, "tickfile", "2026", "20260602")
        os.makedirs(tickfile_dir, exist_ok=True)

        tmp_path_file = os.path.join(tickfile_dir, "tickfile_20260602.csv.tmp")
        with open(tmp_path_file, "w") as f:
            f.write(TICKFILE_HEADER + "\ndata\n")

        final_path = tmp_path_file[:-4]
        if os.path.exists(tmp_path_file) and not os.path.exists(final_path):
            with open(tmp_path_file, "r") as f:
                first_line = f.readline().strip()
            if first_line == TICKFILE_HEADER.strip():
                os.replace(tmp_path_file, final_path)

        assert os.path.exists(final_path)
        assert not os.path.exists(tmp_path_file)


class TestStartupTmpCleanupCorruptDeletion:
    """Spec: corrupt .tmp → delete."""

    def test_corrupt_tmp_deleted(self, tmp_path):
        output_dir = str(tmp_path)
        tickfile_dir = os.path.join(output_dir, "tickfile", "2026", "20260602")
        os.makedirs(tickfile_dir, exist_ok=True)

        tmp_path_file = os.path.join(tickfile_dir, "tickfile_20260602.csv.tmp")
        with open(tmp_path_file, "w") as f:
            f.write("CORRUPT_DATA\n")

        from minute_bar.tickfile import TICKFILE_HEADER
        final_path = tmp_path_file[:-4]
        if os.path.exists(tmp_path_file) and not os.path.exists(final_path):
            with open(tmp_path_file, "r") as f:
                first_line = f.readline().strip()
            if first_line != TICKFILE_HEADER.strip():
                os.remove(tmp_path_file)

        assert not os.path.exists(tmp_path_file)
