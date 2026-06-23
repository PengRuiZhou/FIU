from __future__ import annotations

import csv
import logging
import os
import queue as _queue_module
import re
import threading
import time
from typing import Dict, List, Optional

from minute_bar.aggregator import SharedState, build_order_record
from minute_bar.checkpoint import CheckpointManager
from minute_bar.clock import (
    extract_date_from_minute_key,
    get_poll_interval_ms,
    is_data_driven_expired,
    is_expired,
    jst_now_yyyymmdd,
    time_to_minute_key,
)
from minute_bar.code_table import CodeTable
from minute_bar.config import AppConfig
from minute_bar.csv_parser import parse_order_line, parse_order_record, parse_snapshot_line
from minute_bar.csv_parser import use_rust_accel, _RUST_ACCEL_AVAILABLE
from minute_bar.csv_parser import _rust_parse_batch
# Phase 21 imports
from minute_bar.csv_parser import (
    use_phase21_order_batch,
    use_phase21_snapshot_batch,
    use_phase21_tickfile,
    decode_order_per_minute_buf,
    decode_late_order_buf,
    decode_latest_order_buf,
)
from minute_bar._order_accel import (
    process_order_batch as _rust_process_order_batch,
    aggregate_snapshot_batch as _rust_aggregate_snapshot_batch,
    rust_reset_state as _rust_reset_order_state,
)
from minute_bar.file_tailer import FileTailer
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import FileState, OrderRecord, SnapshotRecord
from minute_bar.writer import (
    append_order_records,
    get_order_file_path,
    write_order_file,
)

logger = logging.getLogger(__name__)

_TICKFILE_QUEUE_WARNING_THRESHOLD = 500
_TICKFILE_QUEUE_CRITICAL_THRESHOLD = 800
_TICKFILE_SENTINEL_STOP = object()
_TICKFILE_MAX_CONSECUTIVE_ERRORS = 5

MAX_PENDING_ORDER_MINUTES = 3


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


def restore_latest_snapshot_from_file(
    output_dir: str, last_output_minute: str, encoding: str = "utf-8"
) -> Dict[str, SnapshotRecord]:
    if not last_output_minute:
        return {}

    date_str = last_output_minute[:8]
    hhmm = last_output_minute[8:12]
    path = os.path.join(output_dir, "snapshot", date_str[:4], date_str, f"snapshot_minute_{date_str}_{hhmm}.csv")

    if not os.path.exists(path) or path.endswith(".tmp"):
        logger.warning("Snapshot file not found or is .tmp: %s", path)
        return {}

    records = {}
    try:
        with open(path, "r", encoding=encoding) as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                logger.error("Empty snapshot file: %s", path)
                return {}

            for row in reader:
                if len(row) < 24:
                    continue
                try:
                    symbol = row[1]
                    rec = SnapshotRecord(
                        symbol=symbol,
                        seqno=int(row[0]),
                        time=int(row[3]),
                        rcvtime=int(row[22]) if row[22] else 0,
                        preclose=float(row[4]),
                        lastprice=float(row[5]),
                        open=float(row[6]),
                        high=float(row[7]),
                        low=float(row[8]),
                        close=float(row[9]),
                        lasttradeprice=float(row[10]),
                        lasttradeqty=int(row[11]),
                        totalvol=int(row[12]),
                        totalamount=float(row[13]),
                        sessionid=int(row[14]),
                        tradetype=row[15],
                        status=row[16],
                        direction=int(row[17]),
                        pflag=row[18],
                        decimal=int(row[19]),
                        vwap=float(row[20]),
                        shortsellflag=int(row[21]),
                    )
                    records[symbol] = rec
                except (ValueError, IndexError) as e:
                    logger.warning("Failed to restore snapshot row: %s, error: %s", row[:5], e)
                    continue

        logger.info("Restored %d symbols from snapshot file: %s", len(records), path)
    except OSError as e:
        logger.error("Failed to read snapshot file %s: %s", path, e)

    return records


class Engine:
    def __init__(self, config: AppConfig):
        self._config = config
        self._target_date = config.input.target_date
        self._state = SharedState(
            first_seen_volume_base=config.aggregation.first_seen_volume_base
        )
        self._code_table = CodeTable(
            config.input.csv_dir,
            encoding=config.input.file_encoding,
            chunk_size=config.input.chunk_size_bytes,
        )
        self._checkpoint = CheckpointManager(
            config.recovery.checkpoint_file,
            config.output.output_dir,
        )
        self._snapshot_tailer = FileTailer(
            config.input.csv_dir, "snapshot",
            chunk_size=config.input.chunk_size_bytes,
            encoding=config.input.file_encoding,
        )
        self._order_tailer = FileTailer(
            config.input.csv_dir, "order",
            chunk_size=config.input.order_chunk_size_bytes,
            encoding=config.input.file_encoding,
        )
        self._file_states: Dict[str, FileState] = {
            "snapshot": self._snapshot_tailer.state,
            "code": self._code_table.get_state(),
            "order": self._order_tailer.state,
        }
        self._checkpoint_lock = threading.Lock()
        self._flusher = ClockWatermarkFlusher(
            state=self._state,
            code_table=self._code_table,
            checkpoint=self._checkpoint,
            output_dir=config.output.output_dir,
            output_delay_sec=config.recovery.output_delay_sec,
            enable_full_snapshot=config.output.enable_full_snapshot,
            enable_full_kline=config.output.enable_full_kline,
            enable_kline=config.output.enable_kline,
            enable_order=config.output.enable_order,
            file_states=self._file_states,
            checkpoint_lock=self._checkpoint_lock,
            data_flush_delay_minutes=config.recovery.data_flush_delay_minutes,
            enable_time_fallback=config.recovery.enable_time_fallback,
            stall_flush_sec=config.recovery.stall_flush_sec,
            enable_tickfile=config.output.enable_tickfile,
            enable_tickfile_commit_marker=config.recovery.enable_tickfile_commit_marker,
        )
        self._flusher._engine_ref = self  # For cross-day pause/resume + counter routing
        self._running = False
        self._data_thread: Optional[threading.Thread] = None
        self._clock_thread: Optional[threading.Thread] = None
        self._order_thread: Optional[threading.Thread] = None
        self._last_code_refresh = 0.0
        self._order_thread_error: Optional[Exception] = None
        self._committed_order_offset = 0
        self._flushed_order_minutes: set[str] = set()
        self._late_order_count: int = 0
        self._late_order_minutes: set[str] = set()
        # Tickfile sync: deferred trigger list (order-thread only)
        self._tickfile_trigger_pending: list = []
        # ── Tickfile background writer (Phase 18) ──
        self._tickfile_queue: _queue_module.Queue = _queue_module.Queue()
        self._tickfile_writer_thread: Optional[threading.Thread] = None
        self._tickfile_writer_alive = False
        self._tickfile_writer_running = False
        self._tickfile_writer_error_count = 0
        self._tickfile_started = False
        self._tickfile_writer_restart_count = 0
        self._tickfile_health_log_counter = 0

        # Metrics counters
        self._tickfile_enqueue_count = 0
        self._tickfile_dequeue_count = 0
        self._tickfile_writer_exception_count = 0
        self._tickfile_overflow_direct_io_count = 0
        self._tickfile_queue_stale_drain_count = 0
        self._tickfile_writer_zombie_detected_count = 0
        self._tickfile_writer_restart_total = 0
        self._tickfile_queue_skip_count = 0
        self._data_flush_delay_minutes = config.recovery.data_flush_delay_minutes
        self._enable_time_fallback = config.recovery.enable_time_fallback
        self._stall_flush_sec = config.recovery.stall_flush_sec
        self._max_late_order_records = config.recovery.max_late_order_records_per_minute

        if config.recovery.data_flush_delay_minutes < 0:
            raise ValueError(
                f"data_flush_delay_minutes must be >= 0, got {config.recovery.data_flush_delay_minutes}"
            )
        if config.recovery.data_flush_delay_minutes > 10:
            logger.warning(
                "data_flush_delay_minutes=%d is unusually large; minutes may accumulate in buffer for extended time",
                config.recovery.data_flush_delay_minutes,
            )
        if not config.recovery.enable_time_fallback:
            logger.warning(
                "enable_time_fallback is DISABLED — only for test environments; "
                "if data thread stalls, buffers may not flush automatically."
            )

        # ── Rust warmup: exercise PyO3 return path before first real use ──
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

        # ── Phase 21 warmup ──
        # Define warmup_today once for all Phase 21 warmup checks
        warmup_today = self._target_date or jst_now_yyyymmdd()

        # Phase 21 warmup: process_order_batch
        if use_phase21_order_batch(config):
            try:
                result = _rust_process_order_batch([], 'utf-8', warmup_today, 0, 0, [])
                assert isinstance(result, tuple) and len(result) == 5, (
                    f"Phase 21 warmup: expected 5-tuple, got {type(result)}"
                )
                # Validate magic bytes in each buffer
                assert result[0][0:4] == b'\xaa\xbb\xcc\x01', (
                    f"per_minute magic invalid: {result[0][0:4].hex()}"
                )
                assert result[1][0:4] == b'\xaa\xbb\xcc\x02', (
                    f"late_order magic invalid: {result[1][0:4].hex()}"
                )
                assert result[2][0:4] == b'\xaa\xbb\xcc\x03', (
                    f"latest_order magic invalid: {result[2][0:4].hex()}"
                )
                logger.info("Phase 21 warmup passed: process_order_batch=True")
            except Exception as e:
                logger.error("Phase 21 warmup FAILED: %s", e)
                # Disable Phase 21 by setting the flag to False
                config.input.enable_rust_order_full_batch = False

        # Phase 21 warmup: aggregate_snapshot_batch
        if use_phase21_snapshot_batch(config):
            try:
                result = _rust_aggregate_snapshot_batch([], warmup_today, [], [], [])
                assert isinstance(result, tuple) and len(result) == 5, (
                    f"Phase 21 snapshot warmup: expected 5-tuple, got {type(result)}"
                )
                assert result[0][0:4] == b'\xaa\xbb\xcc\x04', (
                    f"ohlcv magic invalid: {result[0][0:4].hex()}"
                )
                logger.info("Phase 21 warmup passed: aggregate_snapshot_batch=True")
            except Exception as e:
                logger.error("Phase 21 snapshot warmup FAILED: %s", e)
                config.input.enable_rust_snapshot_batch = False

        # ── Rust order acceleration startup validation ──
        # ALWAYS log Rust status for operational visibility.
        _accel_enabled = config.input.enable_order_accel
        if _RUST_ACCEL_AVAILABLE and _accel_enabled:
            logger.info(
                "Order acceleration: ENABLED (Rust _order_accel loaded, self-test passed)"
            )
        elif _RUST_ACCEL_AVAILABLE and not _accel_enabled:
            logger.warning(
                "Order acceleration: DISABLED by config (Rust available but enable_order_accel=false). "
                "Set enable_order_accel=true for peak minute performance."
            )
        else:
            if _accel_enabled:
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

    def _cleanup_tickfile_tmp_files(self) -> None:
        """Scan tickfile directory for .tmp files. Recover valid ones, delete corrupt."""
        import glob as _glob
        from minute_bar.tickfile import TICKFILE_HEADER
        tickfile_dir = os.path.join(self._config.output.output_dir, "tickfile")
        if not os.path.isdir(tickfile_dir):
            return
        date_dir = os.path.join(tickfile_dir, self._get_target_date())
        if not os.path.isdir(date_dir):
            return
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

    def start(self) -> None:
        # INV-CM-FS-CHECK-RUNTIME (T11): reject network filesystems at startup, before any
        # tickfile recovery / writer thread runs. No-op on Windows dev/test (_HAS_FCNTL=False).
        from minute_bar.writer import check_output_fs_local
        check_output_fs_local(self._config.output.output_dir)

        self._restore_from_checkpoint()
        self._running = True

        # Tickfile writer thread startup (Phase 18)
        if self._config.output.enable_tickfile:
            self._tickfile_started = True
            self._tickfile_queue = _queue_module.Queue()  # Fresh UNBOUNDED queue
            self._flusher._tickfile_queue = self._tickfile_queue
            self._tickfile_writer_error_count = 0
            self._tickfile_health_log_counter = 0
            self._tickfile_writer_running = True

            self._cleanup_tickfile_tmp_files()
            # tickfile recovery + seqno handled in flusher.__init__ (INV-CM-ORDER-1)
            logger.info("Tickfile seqno at init: %d for date %s",
                        self._state._tickfile_seqno, self._get_target_date())

            self._tickfile_writer_thread = threading.Thread(
                target=self._tickfile_writer_loop,
                name="tickfile-writer",
                daemon=True,
            )
            self._tickfile_writer_alive = True
            assert self._tickfile_writer_running, "alive=True but running=False (N32)"
            self._tickfile_writer_thread.start()

        self._data_thread = threading.Thread(target=self._data_loop, name="data-thread", daemon=True)
        self._clock_thread = threading.Thread(target=self._clock_loop, name="clock-thread", daemon=True)
        self._order_thread = threading.Thread(target=self._order_loop, name="order-thread", daemon=True)

        self._data_thread.start()
        self._clock_thread.start()
        self._order_thread.start()

        logger.info("Engine started")

        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupted, shutting down...")
            self.stop()

    def stop(self) -> None:
        logger.info("Engine stop: beginning graceful shutdown")
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
                logger.critical(
                    "Threads still alive after join timeout: %s; skip final flush",
                    join_errors,
                )
                import sys as _sys
                import traceback as _traceback
                for name, thread in [
                    ("order", self._order_thread),
                    ("data", self._data_thread),
                    ("clock", self._clock_thread),
                ]:
                    if thread and thread.is_alive():
                        frame = _sys._current_frames().get(thread.ident)
                        if frame:
                            logger.error("Thread %s stack:\n%s", name, ''.join(
                                f"  File \"{fn}\", line {ln}, in {func}\n    {line.strip()}\n"
                                for fn, ln, func, line in _traceback.extract_stack(frame)
                            ))

            # Phase 2: Signal + join tickfile writer
            if self._tickfile_started and self._tickfile_writer_alive and self._tickfile_writer_thread:
                self._tickfile_writer_running = False
                self._tickfile_queue.put(_TICKFILE_SENTINEL_STOP)
                self._tickfile_writer_thread.join(timeout=60)

                if self._tickfile_writer_thread.is_alive():
                    logger.critical(
                        "Tickfile writer thread did not exit after 60s. "
                        "Skipping tickfile drain."
                    )
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

                    # Snapshot pending count for later comparison
                    if self._config.output.enable_tickfile:
                        with self._state.lock:
                            pre_flush_pending_count = len(self._state._tickfile_pending)
                    else:
                        pre_flush_pending_count = 0

                    # Phase 4: flush_all_remaining
                    try:
                        self._flusher.flush_all_remaining()
                    except Exception as e:
                        logger.exception("Final flush failed")
                        flush_error = e

                    # Fix-C: 3-layer shutdown completeness check
                    if self._config.output.enable_tickfile:
                        # Note: pre_flush_pending_count is informational context only.
                        with self._state.lock:
                            pending = set(self._state._tickfile_pending.keys())
                        if pending:
                            logger.warning(
                                "Shutdown CHECK 1 FAIL: %d tickfile minutes still pending after flush_all_remaining "
                                "(info: %d before flush): %s",
                                len(pending), pre_flush_pending_count, sorted(pending)[:20],
                            )

                        # Layer 2: Queue residual check
                        qdepth = self._tickfile_queue.qsize() if self._tickfile_queue else 0
                        if qdepth > 0:
                            logger.warning(
                                "Shutdown CHECK 2 FAIL: %d minute keys still in tickfile queue after drain",
                                qdepth,
                            )

                        # Layer 3: In-memory generated set vs flushed snapshot/order minutes
                        missing = []
                        with self._state.lock:
                            generated = set(self._state._generated_tickfile_minutes)
                        flushed_snaps = set(self._state.flushed_snapshot_minutes)
                        assert hasattr(self._state, 'flushed_order_minutes'), (
                            "SharedState.flushed_order_minutes required for Fix-C Layer 3 "
                            "(see Implementation Plan step 3 and step 4b)"
                        )
                        flushed_orders = set(self._state.flushed_order_minutes)
                        comparison_mins = flushed_snaps & flushed_orders
                        if comparison_mins:
                            missing = sorted(comparison_mins - generated)
                            extra = sorted(generated - comparison_mins)
                            if missing:
                                logger.warning(
                                    "Shutdown CHECK 3 FAIL: %d tickfile minutes MISSING (have snapshot+order, no tickfile): %s",
                                    len(missing), missing[:30],
                                )
                            if extra:
                                logger.warning(
                                    "Shutdown CHECK 3 WARN: %d tickfile minutes EXTRA (no snapshot+order): %s",
                                    len(extra), extra[:10],
                                )
                            if not missing and not extra:
                                logger.info(
                                    "Shutdown CHECK 3 PASS: tickfile complete (%d minutes match snapshot&order intersection)",
                                    len(comparison_mins),
                                )

                        # Summary line
                        logger.info(
                            "Tickfile shutdown summary: enqueue=%d, dequeue=%d, skip=%d, generated=%d, missing=%d",
                            getattr(self, '_tickfile_enqueue_count', 0),
                            getattr(self, '_tickfile_dequeue_count', 0),
                            getattr(self, '_tickfile_queue_skip_count', 0),
                            len(generated),
                            len(missing),
                        )

                    # Fix-E: Order late cap shutdown summary
                    if hasattr(self, '_total_late_order_dropped') and self._total_late_order_dropped > 0:
                        logger.warning(
                            "Order late cap summary: %d total records dropped due to late cap (cap=%d)",
                            self._total_late_order_dropped,
                            self._max_late_order_records,
                        )
                    logger.info("Engine stop: tickfile writer drained + finalized")
            else:
                # No writer or not started — still flush remaining
                try:
                    self._flusher.flush_all_remaining()
                except Exception as e:
                    logger.exception("Final flush failed")
                    flush_error = e

                # Fix-C also runs for non-tickfile mode if tickfile was enabled
                # (e.g., if writer died and was never started)
                # Fix-E: Order late cap shutdown summary (always runs)
                if hasattr(self, '_total_late_order_dropped') and self._total_late_order_dropped > 0:
                    logger.warning(
                        "Order late cap summary: %d total records dropped due to late cap (cap=%d)",
                        self._total_late_order_dropped,
                        self._max_late_order_records,
                    )

        except Exception as e:
            logger.exception("Engine stop failed unexpectedly")
            flush_error = e
        finally:
            # Cleanup
            self._tickfile_writer_alive = False
            self._flusher._engine_ref = None
            self._flusher._tickfile_queue = None
            self._tickfile_started = False

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

        if join_errors or flush_error:
            raise RuntimeError(
                f"Engine stop errors: join_timeout={join_errors}, flush={flush_error}"
            ) from flush_error

    def _restore_from_checkpoint(self) -> None:
        data = self._checkpoint.read()
        if data is None:
            logger.info("Starting fresh (no checkpoint)")
            return

        logger.info("Restoring from checkpoint: date=%s, last_seqno=%d", data.get("date"), data.get("last_seqno"))

        file_states = self._checkpoint.get_file_states(data)
        if "snapshot" in file_states:
            self._snapshot_tailer.state = file_states["snapshot"]
            self._file_states["snapshot"] = self._snapshot_tailer.state
        if "code" in file_states:
            self._code_table.set_state(
                file_states["code"].offset,
                file_states["code"].pending_line,
                file_states["code"].date,
            )
            self._file_states["code"] = file_states["code"]
        if "order" in file_states:
            self._order_tailer.state = file_states["order"]
            self._file_states["order"] = self._order_tailer.state
            self._committed_order_offset = file_states["order"].offset

        self._state.seqno = data.get("last_seqno", 0)
        self._state.current_minute = data.get("current_minute", "")
        self._state.output_minutes = set(data.get("output_minutes", []))
        self._state.last_output_minute = data.get("last_output_minute", "")
        self._state.last_output_date = data.get("last_output_date", "")
        self._state.first_data_received = data.get("first_data_received", False)
        self._state.last_totalvol_by_symbol = self._checkpoint.get_last_totalvol(data)
        self._state.last_totalamount_by_symbol = self._checkpoint.get_last_totalamount(data)

        snapshot_records = restore_latest_snapshot_from_file(
            self._config.output.output_dir,
            self._state.last_output_minute,
            self._config.input.file_encoding,
        )
        if snapshot_records:
            self._state.latest_snapshot = snapshot_records

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

    def _check_order_thread_error(self) -> None:
        if self._order_thread_error is not None:
            err = self._order_thread_error
            self._order_thread_error = None
            raise err

    def _get_target_date(self) -> str:
        return self._target_date or jst_now_yyyymmdd()

    def _data_loop(self) -> None:
        while self._running:
            try:
                self._check_order_thread_error()

                current_date = self._get_target_date()
                self._snapshot_tailer.set_date(current_date)
                self._code_table._tailer.set_date(current_date)

                data_read = False
                drain_count = 0
                while True:                                  # drain loop
                    lines = list(self._snapshot_tailer.read_lines())
                    if not lines:
                        break
                    data_read = True
                    drain_count += 1

                    for line in lines:
                        parsed = parse_snapshot_line(line, self._config.input.file_encoding)
                        if parsed:
                            record_date = str(parsed.time)[:8]
                            if record_date != current_date:
                                continue
                            self._state.process_snapshot(parsed)

                    self._maybe_refresh_code()

                    if drain_count >= 1000:                  # drain_count protection
                        logger.debug("Snapshot drain yield at %d chunks", drain_count)
                        break

                with self._checkpoint_lock:
                    self._file_states["snapshot"] = self._snapshot_tailer.state
                    self._file_states["code"] = self._code_table.get_state()

            except Exception as e:
                logger.error("Data thread error: %s", e, exc_info=True)
                self._running = False
                return

            if data_read:
                time.sleep(0.001)
            else:
                interval = get_poll_interval_ms(self._config) / 1000.0
                time.sleep(interval)

    def _clock_loop(self) -> None:
        while self._running:
            try:
                self._check_order_thread_error()
                self._flusher.tick()
            except SystemExit as e:
                logger.fatal("Clock thread fatal: exit code %d", e.code)
                self._running = False
                raise
            except Exception as e:
                logger.error("Clock thread error: %s", e, exc_info=True)
                self._running = False
                return

            time.sleep(1)

    def _maybe_refresh_code(self) -> None:
        now = time.monotonic()
        if now - self._last_code_refresh < self._config.recovery.code_refresh_sec:
            return
        self._last_code_refresh = now
        try:
            self._code_table.refresh()
        except Exception as e:
            logger.warning("Code table refresh failed: %s", e)

    def _process_parsed_record(
        self,
        record: OrderRecord,
        today_str: str,
        seqno: int,
        minute_key: str,
        buffers: Dict[str, _OrderMinuteBuffer],
        current_date: Optional[str],
        current_minute: Optional[str],
        pending_shared_orders: list,
        late_order_per_minute: Dict[str, int],
        late_dropped_per_minute: Dict[str, int],
        output_dir: str,
        total_late_dropped: int,
    ) -> tuple:
        """Shared per-record processing: cross-day, late-order, buffer, watermark.

        NOTE: pending_shared_orders is APPENDED to (line 767 equivalent) but
        the state-lock processing (lines 774-791) is NOT included here.
        The caller must process pending_shared_orders once per batch AFTER
        all records in the batch have been processed through this function.
        The LATE_CACHE_MARKER is captured from the caller's scope.

        Returns: (seqno, total_late_dropped, current_date, current_minute)
        """
        record_date = str(record.time)[:8]  # ALWAYS string for downstream comparisons

        # Cross-day flush + reset
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

            # Reset order progress for cross-day
            if self._config.output.enable_tickfile:
                with self._state.lock:
                    self._state.order_current_minute = ""
                self._drain_tickfile_triggers()
            # Clear _flushed_order_minutes to prevent stale entries
            self._flushed_order_minutes.clear()

        if current_date is None:
            current_date = record_date

        # Late order detection
        if minute_key in self._flushed_order_minutes:
            count = late_order_per_minute.get(minute_key, 0)
            if count >= self._max_late_order_records:
                late_dropped_per_minute[minute_key] = late_dropped_per_minute.get(minute_key, 0) + 1
                total_late_dropped += 1
                return (seqno, total_late_dropped, current_date, current_minute)
            late_order_per_minute[minute_key] = count + 1
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
            if self._config.output.enable_tickfile:
                pending_shared_orders.append(("__LATE__", record))
            return (seqno, total_late_dropped, current_date, current_minute)

        # Record-driven flush (only on forward progress)
        if current_minute is not None and minute_key > current_minute:
            self._flush_order_minute(
                buffers, current_minute, output_dir
            )

        # Buffer write
        buf = buffers.get(minute_key)
        if buf is None:
            buf = _OrderMinuteBuffer()
            buffers[minute_key] = buf
        buf.records.append(record)
        buf.line_end_offset = self._order_tailer.line_offset
        if self._config.output.enable_tickfile:
            pending_shared_orders.append((minute_key, record))

        # Monotonic watermark update
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
            current_date = record_date

        return (seqno, total_late_dropped, current_date, current_minute)

    def _write_late_orders_batch(
        self,
        decoded_late: List[OrderRecord],
        output_dir: str,
        pending_shared_orders: list,
    ) -> None:
        """Write Phase 21 late orders to disk, grouped by minute_key.

        Phase 21's Rust pipeline routes already-flushed-minute orders into
        ``late_order_buf``; this method decodes that buffer (done by the caller)
        and persists the records. Each minute gets ONE write call (batch append
        or create) rather than one ``open()`` per record — the per-record path
        deadlocks the order thread on file I/O during the 0900 open peak.

        Invariants (spec §6.1):
          - Same file target: each record still reaches its minute's order file.
          - Same intra-minute order: records keep ``decode_late_order_buf`` order.
          - ``_late_order_count`` += 1 per record (counted in the grouping pass).
          - ``_late_order_minutes`` accumulates every touched minute.
          - Tickfile routing: each record appended to ``pending_shared_orders``
            with the ``__LATE__`` marker when tickfile output is enabled.
          - append/create decision unchanged: file exists → append, else → create.
        """
        if not decoded_late:
            return

        enable_tickfile = self._config.output.enable_tickfile

        # Group by minute_key while preserving per-minute insertion order.
        late_by_minute: Dict[str, List[OrderRecord]] = {}
        for rec in decoded_late:
            mk = time_to_minute_key(rec.time)
            self._late_order_count += 1
            self._late_order_minutes.add(mk)
            late_by_minute.setdefault(mk, []).append(rec)
            if enable_tickfile:
                pending_shared_orders.append(("__LATE__", rec))

        # One write call per minute instead of one per record.
        for late_mk, batch in late_by_minute.items():
            path = get_order_file_path(output_dir, late_mk)
            if os.path.exists(path):
                append_order_records(path, batch)
            else:
                write_order_file(output_dir, late_mk, batch)

    def _order_loop(self) -> None:
        if not self._config.output.enable_order:
            return

        output_dir = self._config.output.output_dir
        output_delay_sec = self._config.recovery.output_delay_sec
        encoding = self._config.input.file_encoding
        stall_flush_sec = self._stall_flush_sec

        buffers: Dict[str, _OrderMinuteBuffer] = {}
        current_minute: Optional[str] = None
        current_date: Optional[str] = None
        seqno = 0
        prev_date: int = 0  # Phase 21: cross-day detection for process_order_batch
        late_order_per_minute: Dict[str, int] = {}
        late_dropped_per_minute: Dict[str, int] = {}
        total_late_dropped: int = 0
        prev_order_minute: Optional[str] = None
        last_order_advance_ts = time.monotonic()
        order_stall_warned = False

        try:
            while self._running:
                try:
                    today = self._get_target_date()
                    self._order_tailer.set_date(today)

                    data_read = False
                    drain_count = 0
                    pending_shared_orders: list = []
                    LATE_CACHE_MARKER = object()
                    while True:
                        lines = list(self._order_tailer.read_lines())
                        if not lines:
                            break
                        data_read = True
                        drain_count += 1

                        # ── Rust / Python parse branch ──
                        # Compute both str and int versions of today's date ONCE per batch.
                        # Guard runs BEFORE the Rust/Python branch to protect BOTH paths.
                        today_int = int(today)  # int — used by Rust date check only

                        # ── Phase 21: Full Rust pipeline (process_order_batch) ──
                        phase21_used = False  # Track whether Phase 21 processed this batch
                        if use_phase21_order_batch(self._config) and encoding.lower() in ("utf-8", "utf8"):
                            # Phase 21: parse + group + buffer build ALL in Rust
                            try:
                                (
                                    per_minute_buf, late_order_buf, latest_order_buf,
                                    late_minute_keys, skipped,
                                ) = _rust_process_order_batch(
                                    lines, encoding, today, today_int,
                                    prev_date, list(self._flushed_order_minutes),
                                )
                                logger.debug(
                                    "Phase 21 order batch: %d lines, %d skipped, %d late_keys",
                                    len(lines), skipped, len(late_minute_keys),
                                )
                            except Exception as e:
                                # Rust full-batch failed — call reset for panic recovery, fall through
                                logger.warning(
                                    "Phase 21 process_order_batch failed (%s: %s), falling back to Python for %d lines",
                                    type(e).__name__, e, len(lines),
                                )
                                try:
                                    _rust_reset_order_state()
                                except Exception:
                                    pass
                                per_minute_buf = None

                            if per_minute_buf is not None:
                                phase21_used = True
                                # Decode per_minute_buf and write to buffers
                                decoded_per_minute = decode_order_per_minute_buf(per_minute_buf)
                                for minute_key, records in decoded_per_minute.items():
                                    # Cross-day check for this minute
                                    if records:
                                        first_date = str(records[0].time)[:8]
                                        if current_date is not None and first_date != current_date:
                                            # Cross-day: flush all buffers
                                            self._flush_all_order_buffers(buffers, output_dir)
                                            current_date = first_date
                                            current_minute = None
                                        if current_date is None:
                                            current_date = first_date

                                    for rec in records:
                                        minute_key_for_record = time_to_minute_key(rec.time)
                                        buf = buffers.get(minute_key_for_record)
                                        if buf is None:
                                            buf = _OrderMinuteBuffer()
                                            buffers[minute_key_for_record] = buf
                                        buf.records.append(rec)
                                        buf.line_end_offset = self._order_tailer.line_offset
                                        if self._config.output.enable_tickfile:
                                            pending_shared_orders.append((minute_key_for_record, rec))
                                        # Watermark advance
                                        if current_minute is None or minute_key_for_record > current_minute:
                                            current_minute = minute_key_for_record
                                        # Late cache marker
                                        if minute_key_for_record in self._flushed_order_minutes:
                                            pending_shared_orders.append(("__LATE__", rec))

                                # Decode late_order_buf — write late orders to files
                                # (grouped per minute via _write_late_orders_batch to avoid
                                #  one open() per record; see spec 2026-06-15-late-order-batch-write-fix)
                                decoded_late = decode_late_order_buf(late_order_buf)
                                self._write_late_orders_batch(
                                    decoded_late, output_dir, pending_shared_orders,
                                )

                                # Decode latest_order_buf → atomically update SharedState
                                decoded_latest = decode_latest_order_buf(latest_order_buf)
                                if decoded_latest:
                                    with self._state.lock:
                                        for sym, rec in decoded_latest.items():
                                            existing = self._state.latest_order_by_symbol.get(sym)
                                            if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                                                self._state.latest_order_by_symbol[sym] = rec

                                # Update prev_date for next batch cross-day detection
                                prev_date = today_int

                        # ── Fallback: Python path (when Phase 21 not used or failed) ──
                        if not phase21_used:
                            if use_rust_accel(self._config) and encoding.lower() in ("utf-8", "utf8"):
                                # ── Rust accelerated path (tuple return — faster in release builds) ──
                                try:
                                    batch, skipped = _rust_parse_batch(lines, encoding)
                                except Exception as e:
                                    # Rust batch parse failed.
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
                                        # minute_key via shared round-up derivation
                                        minute_key = time_to_minute_key(record.time)
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
                                        minute_key = time_to_minute_key(record.time)
                                        # Feed to SAME shared processing function
                                        seqno, total_late_dropped, current_date, current_minute = \
                                            self._process_parsed_record(
                                                record, today, seqno, minute_key,
                                                buffers, current_date, current_minute,
                                                pending_shared_orders, late_order_per_minute,
                                                late_dropped_per_minute, output_dir,
                                                total_late_dropped)
                            else:
                                # ── Fallback: original Python path — no semantic changes ──
                                for line in lines:
                                    # Fast path: bytes → OrderRecord in one call
                                    record = parse_order_record(line, seqno + 1, encoding)
                                    if record is None:
                                        continue

                                    record_date = str(record.time)[:8]
                                    if record_date != today:
                                        continue

                                    seqno += 1
                                    minute_key = time_to_minute_key(record.time)
                                    # Feed to SAME shared processing function
                                    seqno, total_late_dropped, current_date, current_minute = \
                                        self._process_parsed_record(
                                            record, today, seqno, minute_key,
                                            buffers, current_date, current_minute,
                                            pending_shared_orders, late_order_per_minute,
                                            late_dropped_per_minute, output_dir,
                                            total_late_dropped)

                        # ── Batch-scoped: process pending_shared_orders under state lock ──
                        if pending_shared_orders:
                            with self._state.lock:
                                for mk, rec in pending_shared_orders:
                                    if mk == "__LATE__":
                                        existing = self._state.latest_order_by_symbol.get(rec.symbol)
                                        if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                                            self._state.latest_order_by_symbol[rec.symbol] = rec
                                    else:
                                        # Skip raw_order_buffers for already-flushed minutes
                                        # (flusher has passed these — writing would leak memory)
                                        if mk not in self._state.flushed_snapshot_minutes or mk in self._state._tickfile_pending:
                                            self._state.raw_order_buffers.setdefault(mk, []).append(rec)
                                        existing = self._state.latest_order_by_symbol.get(rec.symbol)
                                        if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                                            self._state.latest_order_by_symbol[rec.symbol] = rec
                            pending_shared_orders.clear()
                            if self._config.output.enable_tickfile:
                                self._drain_tickfile_triggers()

                        if drain_count >= 100:
                            self._flush_expired_order_minutes(
                                buffers, output_dir, output_delay_sec, current_minute or ""
                            )
                            self._enforce_max_pending(buffers, output_dir)
                            if late_dropped_per_minute:
                                total_dropped = sum(late_dropped_per_minute.values())
                                if total_dropped > 0:
                                    logger.warning(
                                        "Order late cap: dropped %d records across %d minutes (cap=%d)",
                                        total_dropped, len(late_dropped_per_minute), self._max_late_order_records,
                                    )
                                late_dropped_per_minute.clear()
                            if self._config.output.enable_tickfile:
                                self._drain_tickfile_triggers()
                            drain_count = 0

                    with self._checkpoint_lock:
                        self._file_states["order"] = FileState(
                            offset=self._committed_order_offset,
                            pending_line=b"",
                            date=today,
                        )

                    # Watermark-driven: flush expired minutes
                    self._flush_expired_order_minutes(
                        buffers, output_dir, output_delay_sec, current_minute or ""
                    )
                    if self._config.output.enable_tickfile:
                        self._drain_tickfile_triggers()

                    # Memory protection
                    self._enforce_max_pending(buffers, output_dir)

                    # Order watermark stall detection
                    if current_minute != prev_order_minute:
                        last_order_advance_ts = time.monotonic()
                        order_stall_warned = False
                        prev_order_minute = current_minute
                    else:
                        stalled_sec = time.monotonic() - last_order_advance_ts
                        if not order_stall_warned and stalled_sec >= stall_flush_sec:
                            logger.error(
                                "Order watermark stalled at %s for %.0f seconds",
                                current_minute, stalled_sec,
                            )
                            order_stall_warned = True
                            if buffers:
                                logger.info(
                                    "Stall-triggered order flush: %d remaining minutes",
                                    len(buffers),
                                )
                                self._flush_all_order_buffers(buffers, output_dir)

                except Exception as e:
                    logger.error("Order thread error: %s", e, exc_info=True)
                    self._order_thread_error = e
                    self._running = False
                    return

                if data_read:
                    time.sleep(0.001)
                else:
                    interval = get_poll_interval_ms(self._config) / 1000.0
                    time.sleep(interval)

        finally:
            try:
                self._flush_all_order_buffers(buffers, output_dir)
            except Exception:
                logger.exception("Order final flush failed")
            if late_dropped_per_minute:
                final_dropped = sum(late_dropped_per_minute.values())
                if final_dropped > 0:
                    logger.warning(
                        "Order late cap FINAL: dropped %d records across %d minutes in final batch (cap=%d)",
                        final_dropped, len(late_dropped_per_minute), self._max_late_order_records,
                    )
                late_dropped_per_minute.clear()
            # Store cumulative total for shutdown summary
            # Thread safety: safe because stop() joins order_thread before reading this field
            self._total_late_order_dropped = total_late_dropped
            try:
                self._order_tailer.close()
            except Exception:
                logger.exception("Failed to close order tailer")

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
        with self._state.lock:
            self._state.flushed_order_minutes.add(minute_key)
        logger.info(
            "Output order minute %s (%d records, committed_offset=%d)",
            minute_key, len(buf.records), buf.line_end_offset,
        )
        # NEW: Record minute for deferred tickfile trigger
        if self._config.output.enable_tickfile:
            # DO NOT update order_current_minute here — batch write hasn't happened yet
            # DO NOT trigger tickfile here — raw_order_buffers may not be complete yet
            self._tickfile_trigger_pending.append(minute_key)

    def _drain_tickfile_triggers(self) -> None:
        """Drain _tickfile_trigger_pending: update order_current_minute + enqueue tickfiles.
        Called after batch write and after _flush_expired_order_minutes.
        Tickfile generation is delegated to TickfileWriterThread.
        """
        if not self._tickfile_trigger_pending:
            return
        triggers = list(self._tickfile_trigger_pending)
        # N31: Do NOT clear yet — enqueue first, clear after success.

        if triggers:
            latest = max(triggers)
            with self._state.lock:
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

        for mk in triggers:
            self._tickfile_queue.put_nowait(mk)
        self._tickfile_enqueue_count += len(triggers)

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

        self._tickfile_trigger_pending.clear()

    def _tickfile_writer_loop(self) -> None:
        """Background thread: sole tickfile writer. Processes queue entries serially."""
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
                        "Tickfile writer received SystemExit for minute=%s [thread=tickfile-writer].",
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
                            "Tickfile writer: %d consecutive failures. Stopping writer thread.",
                            self._tickfile_writer_error_count,
                        )
                        self._tickfile_writer_running = False
                        break
        finally:
            self._tickfile_writer_alive = False
            self._tickfile_writer_running = False
            logger.info("Tickfile writer thread exiting [thread=tickfile-writer]")

    def _tickfile_writer_drain(self, timeout_sec: float = 30.0) -> int:
        """Drain all remaining entries from the tickfile queue.
        Called by Engine after writer thread is confirmed dead (via join).
        """
        import time as _time
        drained = 0
        abandoned = 0

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
                    "Tickfile drain skipped (writer thread still alive/zombie): %d entries abandoned",
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
                "Tickfile drain timed out after %.0fs: %d entries abandoned",
                timeout_sec, abandoned,
            )
        self._flusher._run_tickfile_recovery()
        return drained

    def _tickfile_writer_pause(self) -> None:
        """Synchronously pause the background tickfile writer for cross-day cleanup."""
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
                "Tickfile writer thread did not exit after 30s. Force-marking as dead."
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
                logger.critical("Zombie writer: %d queue entries abandoned", abandoned)
            self._tickfile_writer_alive = False
            return

        self._tickfile_writer_alive = False
        self._tickfile_writer_thread = None
        stale_drained = self._tickfile_writer_drain()
        self._flusher._run_tickfile_recovery()

        pause_duration_ms = (_time.monotonic() - pause_start) * 1000
        logger.info(
            "Cross-day writer pause completed [stale_drained=%d, duration_ms=%.0f]",
            stale_drained, pause_duration_ms,
        )

    def _tickfile_writer_resume(self) -> None:
        """Resume the background tickfile writer after cross-day cleanup."""
        if self._tickfile_writer_alive:
            logger.error("Tickfile writer resume called but writer_alive=True. Skipping.")
            return

        if self._tickfile_writer_thread is not None and self._tickfile_writer_thread.is_alive():
            logger.critical("Tickfile writer resume: old thread still alive!")
            return

        self._tickfile_writer_error_count = 0
        self._tickfile_writer_restart_count = 0

        stale_drained = self._tickfile_writer_drain(timeout_sec=10.0)
        if stale_drained > 0:
            logger.info("Resume drained %d stale entries", stale_drained)

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

    def _tickfile_writer_health_check(self) -> None:
        """Check writer thread health and attempt restart if dead."""
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
        self._flusher._run_tickfile_recovery()
        self._tickfile_writer_drain(timeout_sec=3.0)

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

    def _flush_expired_order_minutes(
        self,
        buffers: Dict[str, "_OrderMinuteBuffer"],
        output_dir: str,
        output_delay_sec: int,
        order_watermark: str = "",
    ) -> None:
        expired_keys = [
            k for k in buffers
            if is_data_driven_expired(k, order_watermark, self._data_flush_delay_minutes)
               or (self._enable_time_fallback and is_expired(k, output_delay_sec))
        ]
        for minute_key in sorted(expired_keys):
            self._flush_order_minute(buffers, minute_key, output_dir)

    def _flush_all_order_buffers(
        self,
        buffers: Dict[str, "_OrderMinuteBuffer"],
        output_dir: str,
    ) -> None:
        for minute_key in sorted(buffers):
            self._flush_order_minute(buffers, minute_key, output_dir)
        buffers.clear()

    def _enforce_max_pending(
        self,
        buffers: Dict[str, "_OrderMinuteBuffer"],
        output_dir: str,
    ) -> None:
        while len(buffers) > MAX_PENDING_ORDER_MINUTES:
            oldest = min(buffers)
            is_expired_minute = is_expired(
                oldest, self._config.recovery.output_delay_sec
            )
            logger.warning(
                "Force-flushing order minute %s (pending=%d, expired=%s) — exceeded max_pending_minutes=%d",
                oldest, len(buffers), is_expired_minute, MAX_PENDING_ORDER_MINUTES,
            )
            self._flush_order_minute(buffers, oldest, output_dir)


class _OrderMinuteBuffer:
    __slots__ = ("records", "line_end_offset")

    def __init__(self) -> None:
        self.records: List[OrderRecord] = []
        self.line_end_offset: int = 0
