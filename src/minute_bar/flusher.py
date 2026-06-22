from __future__ import annotations

import csv
import logging
import os
import queue as _queue_module
import threading
import time
from typing import Dict, Optional, Set

from minute_bar.aggregator import MAX_LATE_SNAPSHOT_QUEUE_SIZE, SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.clock import (
    extract_date_from_minute_key,
    is_data_driven_expired,
    is_expired,
    is_yesterday,
    jst_now_yyyymmdd,
    jst_now_yyyymmddhhmm,
    time_to_minute_key,
)
from minute_bar.code_table import CodeTable
from minute_bar.models import FileState, OrderRecord, SnapshotRecord
from minute_bar.writer import (
    append_snapshot_records,
    get_snapshot_file_path,
    write_kline_file,
    write_order_file,
    write_snapshot_file,
)

logger = logging.getLogger(__name__)

MAX_TICKFILE_PENDING_MINUTES = 10
_TICKFILE_QUEUE_WARNING_THRESHOLD = 500
_TICKFILE_QUEUE_CRITICAL_THRESHOLD = 800


class ClockWatermarkFlusher:
    def __init__(
        self,
        state: SharedState,
        code_table: CodeTable,
        checkpoint: CheckpointManager,
        output_dir: str,
        output_delay_sec: int,
        enable_full_snapshot: bool = True,
        enable_full_kline: bool = True,
        enable_kline: bool = True,
        enable_order: bool = True,
        file_states: Optional[Dict[str, FileState]] = None,
        checkpoint_lock: Optional[threading.Lock] = None,
        data_flush_delay_minutes: int = 1,
        enable_time_fallback: bool = True,
        stall_flush_sec: int = 300,
        enable_tickfile: bool = False,
        enable_tickfile_commit_marker: bool = True,
    ):
        self._state = state
        self._code_table = code_table
        self._checkpoint = checkpoint
        self._output_dir = output_dir
        self._output_delay_sec = output_delay_sec
        self._enable_full_snapshot = enable_full_snapshot
        self._enable_full_kline = enable_full_kline
        self._enable_kline = enable_kline
        self._enable_order = enable_order
        self._file_states = file_states or {}
        self._checkpoint_lock = checkpoint_lock
        self._data_flush_delay_minutes = data_flush_delay_minutes
        self._enable_time_fallback = enable_time_fallback
        self._stall_flush_sec = stall_flush_sec

        # Stall detection state (only accessed by clock-thread)
        self._last_watermark: Optional[str] = None
        self._last_watermark_advance_ts: Optional[float] = None
        self._stall_warned: bool = False

        # Tickfile state
        self._enable_tickfile = enable_tickfile
        # INV-CM-KILLSWITCH-CONSISTENCY: process-static flag, read once.
        self._enable_tickfile_commit_marker = enable_tickfile_commit_marker
        if enable_tickfile:
            if not self._enable_order:
                raise ValueError("enable_tickfile=True requires enable_order=True")
            # Recovery = single source of truth for seqno + skip-set (INV-CM-ORDER-1/2).
            # Runs eagerly in __init__, before any writer thread starts.
            from minute_bar.writer import _recover_tickfile_to_last_commit
            target_date = jst_now_yyyymmdd()
            try:
                committed_set, last_seqno, had_sidecar = _recover_tickfile_to_last_commit(
                    output_dir, target_date, enable_commit_marker=self._enable_tickfile_commit_marker)
            except Exception:
                logger.exception("Tickfile recovery failed at init; degrading to empty skip-set")
                committed_set, last_seqno, had_sidecar = set(), 0, False
            # INV-CM-SKIPSET-LIVE-FALLBACK: sidecar mode OR fallback row-scan both populate committed_set.
            self._state._generated_tickfile_minutes |= committed_set
            if (not had_sidecar) and committed_set:
                logger.warning("live_skipset_reconstructed_from_rows: %d minutes (had_sidecar=False)",
                               len(committed_set))
            # INV-CM-SEQNO-MONO-FILE: never regress.
            self._state._tickfile_seqno = max(self._state._tickfile_seqno, last_seqno)
            # INV-CM-RECONCILE-THREE-WAY: detection only (gap-injection deferred).
            self._reconcile_tickfile(committed_set)

        self._tickfile_queue: Optional[_queue_module.Queue] = None  # Set by Engine.start()
        self._engine_ref = None  # Set by Engine.__init__

        # Diagnostic set: per-minute-key WARNING tracking for tickfile skip events.
        # GIL-dependent, approximate. Acceptable for WARNING dedup.
        # Instance-level (not class-level) to avoid cross-instance pollution.
        self._tickfile_skip_warned_keys: set = set()

    def tick(self) -> None:
        self._step1_cross_day_check()
        if not self._step2_first_data_check():
            return
        self._step3_minute_output()
        # Tickfile pending overflow protection
        # CRITICAL: Only force-enqueue minutes where order data is available
        # (order_current_minute > minute_key). Enqueueing minutes without order
        # data produces carry-forward-only tickfile rows with 0 orders.
        if self._enable_tickfile:
            with self._state.lock:
                pending_keys = sorted(self._state._tickfile_pending.keys())
                order_watermark = self._state.order_current_minute
                total_pending = len(pending_keys)
                # Only consider minutes where order has passed
                eligible_keys = [mk for mk in pending_keys if order_watermark > mk]
                eligible_count = len(eligible_keys)
                if eligible_count > MAX_TICKFILE_PENDING_MINUTES:
                    force_count = eligible_count - MAX_TICKFILE_PENDING_MINUTES + 1
                    force_keys = eligible_keys[:force_count]
                else:
                    force_keys = []
                if total_pending > MAX_TICKFILE_PENDING_MINUTES:
                    logger.warning(
                        "Tickfile pending: %d total, %d eligible (order_watermark=%s)%s",
                        total_pending, eligible_count, order_watermark,
                        f", forcing {len(force_keys)} oldest eligible" if force_keys else "",
                    )
            if force_keys:
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
                            "Processing at most 1 minute/tick (~90ms).", mk,
                        )
                        try:
                            self._try_generate_tickfile(mk)
                            self._engine_ref._tickfile_overflow_direct_io_count += 1
                        except Exception:
                            logger.exception("Fallback tickfile generation failed for minute=%s", mk)
        self._step4_handle_late_records()
        self._step5_write_checkpoint()

        if self._engine_ref and self._enable_tickfile:
            self._engine_ref._tickfile_writer_health_check()

    def _step1_cross_day_check(self) -> None:
        with self._state.lock:
            if not self._state.first_data_received:
                return
            if not self._state.last_output_date:
                self._state.last_output_date = extract_date_from_minute_key(self._state.current_minute)
                return

            current_date = extract_date_from_minute_key(self._state.current_minute)
            if current_date == self._state.last_output_date:
                return

            pending = {
                k: self._state.ohlcv_buffers.pop(k)
                for k in list(self._state.ohlcv_buffers)
                if is_yesterday(k, current_date)
            }
            pending_raw = {
                k: self._state.raw_snapshot_buffers.pop(k)
                for k in list(self._state.raw_snapshot_buffers)
                if is_yesterday(k, current_date)
            }
            if not self._enable_tickfile:
                pending_orders = {
                    k: self._state.raw_order_buffers.pop(k)
                    for k in list(self._state.raw_order_buffers)
                    if is_yesterday(k, current_date)
                }
            else:
                pending_orders = {}
            # Pop per-minute snapshots for cross-day minutes
            pending_snapshots = {}
            for k in list(self._state._snapshot_at_minute_end):
                if is_yesterday(k, current_date):
                    pending_snapshots[k] = self._state._snapshot_at_minute_end.pop(k)
            snapshot_copy = dict(self._state.latest_snapshot)
            latest_order_copy = dict(self._state.latest_order_by_symbol)

        if pending:
            for minute_key in sorted(pending):
                data = pending[minute_key]
                raw = pending_raw.get(minute_key, {})
                orders = pending_orders.get(minute_key, [])
                minute_snapshot = pending_snapshots.get(minute_key, snapshot_copy)
                try:
                    self._write_minute_files(minute_key, minute_snapshot, data, raw, orders, latest_order_copy)
                except IOError as e:
                    logger.fatal("Output failed during cross-day flush for minute=%s: %s", minute_key, e)
                    raise SystemExit(1)

            self._write_checkpoint()

        # Step 1: Pause background writer for cross-day
        if self._engine_ref:
            self._engine_ref._tickfile_writer_pause()

        # Step 2: Force-generate remaining pending tickfiles BEFORE clearing.
        # Gate on order watermark: skip minutes order never reached (would be stale).
        # Spec 2026-06-16-tickfile-stale-fix-design §3.2.
        remaining_pending = []
        skipped_keys: list = []
        with self._state.lock:
            remaining_pending = sorted(self._state._tickfile_pending.keys())
            order_wm = self._state.order_current_minute
        if remaining_pending:
            generate_keys = [mk for mk in remaining_pending if order_wm and order_wm >= mk]
            skipped_keys = [mk for mk in remaining_pending if not (order_wm and order_wm >= mk)]
            if generate_keys:
                logger.warning(
                    "Cross-day: generating %d pending tickfiles before cleanup (order lagging)",
                    len(generate_keys),
                )
                for mk in generate_keys:
                    try:
                        self._try_generate_tickfile(mk)
                    except Exception:
                        logger.warning("Cross-day tickfile force-gen retry once for minute=%s", mk, exc_info=True)
                        try:
                            self._try_generate_tickfile(mk)
                        except Exception:
                            logger.critical(
                                "Cross-day tickfile force-gen FAILED twice minute=%s (data lost on clear)", mk,
                                exc_info=True)
            if skipped_keys:
                # Remove skipped from pending so the Fix-G check below does not log them
                # as failures; they are intentionally deferred to replay as 'missing'.
                with self._state.lock:
                    for mk in skipped_keys:
                        self._state._tickfile_pending.pop(mk, None)
                logger.warning(
                    "Cross-day: skipped %d tickfile minutes order hadn't reached "
                    "(no stale rows; fill via ReplayEngine --date=%s): %s",
                    len(skipped_keys), jst_now_yyyymmdd(), skipped_keys[:20],
                )

        # Fix-G: Log any minutes that failed to generate
        with self._state.lock:
            remaining = set(self._state._tickfile_pending.keys())
        if remaining:
            logger.critical(
                "Cross-day CHECK: %d tickfile minutes FAILED to generate (data will be lost on clear): %s",
                len(remaining), sorted(remaining)[:20],
            )

        # INV-CM-CROSSDAY-FLUSH-BARRIER: recover the OLD date's tickfile (truncate any partial tail
        # the writer left) before clearing state. The T7 pause's _run_tickfile_recovery() resolved its
        # date via jst_now_yyyymmdd() = the NEW date, so the OLD-date partial tail was never truncated.
        # Discard the returned set (INV-CM-CROSSDAY-COMMITTED-DISCARD: old-date minutes must NOT enter
        # the live skip-set; we only want the truncate + audit side-effect). Best-effort — never block.
        old_date = self._state.last_output_date
        if old_date and old_date != current_date:
            from minute_bar.writer import _recover_tickfile_to_last_commit
            try:
                _recover_tickfile_to_last_commit(
                    self._output_dir, old_date,
                    enable_commit_marker=self._enable_tickfile_commit_marker,
                )
            except Exception:
                logger.exception("Cross-day old-date tickfile recovery failed for date=%s", old_date)

        cleared_pending = 0
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
            self._state._snapshot_at_minute_end.clear()
            self._state.latest_order_by_symbol = {
                sym: rec for sym, rec in self._state.latest_order_by_symbol.items()
                if str(rec.time)[:8] == current_date
            }
            # Conditional watermark reset: only set if watermark < new day zero base
            new_day_base = current_date + "0000"
            if not self._state.current_minute or self._state.current_minute < new_day_base:
                self._state.current_minute = new_day_base
            # Tickfile sync cleanup
            cleared_pending = len(self._state._tickfile_pending)
            self._state._tickfile_pending.clear()
            orphaned_keys = [k for k in list(self._state.raw_order_buffers)
                             if k[:8] != current_date]
            for k in orphaned_keys:
                self._state.raw_order_buffers.pop(k, None)
            self._state._tickfile_seqno = 0
            self._state.order_current_minute = ""
            self._state._generated_tickfile_minutes.clear()
            self._tickfile_skip_warned_keys.clear()

        if cleared_pending > 0:
            logger.warning(
                "Cross-day cleared %d tickfile pending minutes that failed to generate",
                cleared_pending,
            )
        logger.info("Cross-day reset completed. New date: %s", current_date)

        # Step 3.5: Prune stale _write_locks for old dates (N30)
        from minute_bar import writer as _writer_mod
        _writer_mod._prune_write_locks(current_date)

        # Step 4: Resume writer thread for new day
        if self._engine_ref:
            self._engine_ref._tickfile_writer_resume()

    def _step2_first_data_check(self) -> bool:
        with self._state.lock:
            return self._state.first_data_received

    def _step3_minute_output(self) -> None:
        try:
            with self._state.lock:
                data_watermark = self._state.current_minute
                expired_keys = sorted(
                    k for k in self._state.ohlcv_buffers
                    if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
                       or (self._enable_time_fallback and is_expired(k, self._output_delay_sec))
                )
                data_driven_keys = [
                    k for k in expired_keys
                    if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
                ]
                fallback_keys = [k for k in expired_keys if k not in data_driven_keys]
                already_flushed = {k for k in expired_keys
                                   if k in self._state.flushed_snapshot_minutes}
        except ValueError:
            logger.exception("Invalid minute_key or watermark format, skipping tick (data_watermark=%s)", locals().get('data_watermark'))
            return

        # Stall detection (wall-clock based)
        now = time.monotonic()
        if data_watermark == self._last_watermark:
            if self._last_watermark_advance_ts is not None:
                stalled_sec = now - self._last_watermark_advance_ts
                if not self._stall_warned and stalled_sec >= self._stall_flush_sec:
                    logger.error(
                        "Watermark stalled at %s for %.0f seconds — data thread may be dead or data source stopped",
                        data_watermark, stalled_sec,
                    )
                    self._stall_warned = True

                    # Flush all remaining ohlcv buffers — data source has stopped
                    with self._state.lock:
                        stall_keys = sorted(self._state.ohlcv_buffers.keys())
                    if stall_keys:
                        logger.info("Stall-triggered flush: %d remaining minutes", len(stall_keys))
                        self._flush_minutes_internal(stall_keys, is_final=False)
        else:
            if self._last_watermark is not None and self._stall_warned:
                logger.info(
                    "Watermark recovered: %s -> %s (was stalled for %.0f seconds)",
                    self._last_watermark, data_watermark,
                    now - self._last_watermark_advance_ts if self._last_watermark_advance_ts else 0,
                )
            self._last_watermark = data_watermark
            self._last_watermark_advance_ts = now
            self._stall_warned = False

        if expired_keys:
            normal_keys = [k for k in expired_keys if k not in already_flushed]
            reflush_keys = [k for k in expired_keys if k in already_flushed]

            if normal_keys:
                normal_data_driven = [k for k in normal_keys if k in data_driven_keys]
                normal_fallback = [k for k in normal_keys if k in fallback_keys]
                if normal_data_driven:
                    logger.info("Data-driven flush: %d minutes (watermark=%s)", len(normal_data_driven), data_watermark)
                if normal_fallback:
                    logger.warning("Time-fallback flush: %d minutes (data progress lagging)", len(normal_fallback))
                self._flush_minutes_internal(normal_keys, is_final=False)

            if reflush_keys:
                logger.warning(
                    "Re-routing %d already-flushed minutes to late queue (race window detected)",
                    len(reflush_keys),
                )
                self._reroute_buffer_to_late_queue(reflush_keys)

    def _flush_minutes_internal(
        self, minute_keys: list, *, is_final: bool = False
    ) -> None:
        """Flush specified minutes from buffers to files.

        Responsible for:
        1. Pop data from ohlcv/raw_snapshot/raw_order buffers under lock
        2. Pop per-minute snapshots under lock (fallback to current latest_snapshot)
        3. Write files outside lock
        4. Update state under lock after each successful write
        """
        with self._state.lock:
            ohlcv_data = {}
            for k in minute_keys:
                v = self._state.ohlcv_buffers.pop(k, None)
                if v is not None:
                    ohlcv_data[k] = v
                else:
                    logger.debug("minute_key %s not found in ohlcv_buffers during flush, skipping", k)
            raw_data = {}
            for k in minute_keys:
                v = self._state.raw_snapshot_buffers.pop(k, None)
                if v is not None:
                    raw_data[k] = v
            order_data = {}
            if not self._enable_tickfile:
                for k in minute_keys:
                    v = self._state.raw_order_buffers.pop(k, None)
                    if v is not None:
                        order_data[k] = v
            # Pop per-minute snapshots for carry-forward correctness.
            # These snapshots were captured at the moment current_minute advanced
            # past each minute, before any future data updated latest_snapshot.
            snapshots = {}
            for k in minute_keys:
                snap = self._state._snapshot_at_minute_end.pop(k, None)
                if snap is not None:
                    snapshots[k] = snap
            # Fallback for minutes without a per-minute snapshot
            # (first minute, last minute, or after restart).
            snapshot_copy = dict(self._state.latest_snapshot)
            latest_order_copy = dict(self._state.latest_order_by_symbol)

        errors = []
        for minute_key in minute_keys:
            data = ohlcv_data.get(minute_key)
            if data is None:
                continue
            raw = raw_data.get(minute_key, {})
            orders = order_data.get(minute_key, [])
            # Use per-minute snapshot if available, otherwise fallback to current latest_snapshot
            minute_snapshot = snapshots.get(minute_key, snapshot_copy)
            try:
                self._write_minute_files(minute_key, minute_snapshot, data, raw, orders, latest_order_copy)
            except Exception:
                if is_final:
                    logger.exception("Final flush failed for minute=%s", minute_key)
                    errors.append(minute_key)
                    continue
                else:
                    logger.fatal("Output failed for minute=%s", minute_key)
                    raise SystemExit(1)

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
            if is_final:
                logger.info("Final flush: minute=%s (%d symbols)", minute_key, len(data))

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

        if is_final:
            total = len(ohlcv_data)
            if errors:
                logger.error("Final flush summary: %d/%d minutes failed: %s", len(errors), total, errors)
                raise RuntimeError(
                    f"Final flush failed for {len(errors)}/{total} minutes: {errors}"
                )
            else:
                logger.info("Final flush summary: %d minutes flushed successfully", total)

    def flush_all_remaining(self, skip_tickfile: bool = False) -> None:
        """Flush all remaining buffers on shutdown.

        PRECONDITION: All worker threads must be joined before calling.
        NOT thread-safe. No watermark or time checks — unconditional flush.
        """
        with self._state.lock:
            remaining_keys = sorted(self._state.ohlcv_buffers.keys())

        flush_error = None
        if remaining_keys:
            try:
                self._flush_minutes_internal(remaining_keys, is_final=True)
            except Exception as e:
                flush_error = e

        # Flush any late records that were re-routed but not yet processed by step 4.
        # After all threads are joined, no new late records will be added.
        self._step4_handle_late_records()

        # Generate tickfile for remaining pending minutes (EOF fallback).
        # Gate on order watermark: skip minutes order never reached (would be stale
        # carry-forward). Spec 2026-06-16-tickfile-stale-fix-design §3.1.
        if not skip_tickfile and self._enable_tickfile:
            tickfile_errors = 0
            skipped_keys: list = []
            with self._state.lock:
                remaining_pending = sorted(self._state._tickfile_pending.keys())
                order_wm = self._state.order_current_minute
            for mk in remaining_pending:
                if order_wm and order_wm >= mk:
                    try:
                        self._try_generate_tickfile(mk)
                    except Exception:
                        tickfile_errors += 1
                        logger.exception("EOF tickfile generation failed for minute=%s", mk)
                else:
                    skipped_keys.append(mk)
            if skipped_keys:
                # Remove skipped from pending so the engine's post-flush CHECK does not
                # flag them as failures; they are intentionally deferred to replay as
                # 'missing' (mirrors the cross-day path). Spec §3.1.
                with self._state.lock:
                    for mk in skipped_keys:
                        self._state._tickfile_pending.pop(mk, None)
                logger.warning(
                    "Shutdown skipped %d tickfile minutes order hadn't reached "
                    "(no stale rows written; fill via ReplayEngine --date=%s): %s",
                    len(skipped_keys), jst_now_yyyymmdd(), skipped_keys[:20],
                )
            if remaining_pending:
                logger.info("EOF tickfile summary: %d generated, %d skipped, %d failed",
                            len(remaining_pending) - len(skipped_keys) - tickfile_errors,
                            len(skipped_keys), tickfile_errors)
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

        try:
            self._write_checkpoint()
        except Exception:
            logger.exception("Failed to write checkpoint after final flush")
            if flush_error:
                logger.error("Also had flush errors (checkpoint error takes priority): %s", flush_error)
            raise
        if flush_error:
            raise flush_error

    def _write_minute_files(
        self,
        minute_key: str,
        snapshot_copy: Dict[str, SnapshotRecord],
        ohlcv_data: Dict[str, OHLCVAggregate],
        raw_records: Dict[str, list] = None,
        order_records: list = None,
        latest_order_copy: Optional[Dict] = None,
    ) -> None:
        write_snapshot_file(
            self._output_dir, minute_key, snapshot_copy, ohlcv_data,
            self._code_table, full=self._enable_full_snapshot,
            raw_records=raw_records,
        )
        if self._enable_kline:
            write_kline_file(
                self._output_dir, minute_key, snapshot_copy, ohlcv_data,
                self._code_table, full=self._enable_full_kline,
            )
        if self._enable_order and order_records:
            write_order_file(self._output_dir, minute_key, order_records)

    def _run_tickfile_recovery(self) -> None:
        """Run sidecar recovery + sync skip-set/seqno. Called by engine health-check/drain/pause
        (INV-CM-ORDER-RESTART) and is idempotent. No-op when tickfile disabled."""
        if not self._enable_tickfile:
            return
        from minute_bar.writer import _recover_tickfile_to_last_commit
        target_date = jst_now_yyyymmdd()
        try:
            committed_set, last_seqno, had_sidecar = _recover_tickfile_to_last_commit(
                self._output_dir, target_date,
                enable_commit_marker=self._enable_tickfile_commit_marker)
        except Exception:
            logger.exception("Tickfile recovery (runtime) failed for date=%s", target_date)
            return
        if had_sidecar or committed_set:
            with self._state.lock:
                self._state._generated_tickfile_minutes |= committed_set
                self._state._tickfile_seqno = max(self._state._tickfile_seqno, last_seqno)
        # INV-CM-RECONCILE-THREE-WAY: detection only (gap-injection deferred).
        # Run regardless of had_sidecar/committed_set so an empty tickfile vs.
        # non-empty snapshot/order is also caught.
        self._reconcile_tickfile(committed_set)

    def _reconcile_tickfile(self, committed_set: set) -> None:
        """INV-CM-RECONCILE-THREE-WAY (detection only; gap-injection deferred to follow-up).

        Compares tickfile committed minutes (from sidecar/row-scan recovery) against
        snapshot/order minute-sets and CRITICAL-logs discrepancies. Best-effort — never
        raises, since recovery must not be blocked by reconcile.

        Three legs compared:
          - tickfile  : committed_set (minutes physically present in tickfile sidecar)
          - snapshot  : self._state.flushed_snapshot_minutes  (runtime) ∪
                        self._state.output_minutes            (checkpointed snapshot output)
          - order     : self._state.flushed_order_minutes     (runtime order-file set)

        Discrepancy meanings:
          - tickfile_missing = a minute present in snapshot/order but absent from tickfile.
            Recovery signal: that minute will need gap-injection regeneration (deferred).
          - tickfile_only    = a minute present in tickfile but absent from snapshot/order.
            Checkpoint/order-bookkeeping bug signal.

        NOTE: At init-time recovery, flushed_snapshot_minutes/flushed_order_minutes may not
        yet be populated (they are restored by Engine._restore_from_checkpoint after the
        flusher is constructed). In that case reference is empty and reconcile is a no-op;
        the snapshot/order legs become meaningful on the runtime recovery path
        (_run_tickfile_recovery) and during live operation. Gap-injection (actually
        regenerating the missing tickfile minutes) is explicitly DEFERRED per the
        commit-marker plan.
        """
        if not self._enable_tickfile:
            return
        try:
            with self._state.lock:
                snapshot_minutes = set(getattr(self._state, "flushed_snapshot_minutes", None) or set())
                output_minutes = set(getattr(self._state, "output_minutes", None) or set())
                order_minutes = set(getattr(self._state, "flushed_order_minutes", None) or set())
            reference = snapshot_minutes | output_minutes | order_minutes
            if not reference:
                # Nothing to compare against (fresh run / no checkpoint restored yet).
                return
            committed = set(committed_set or set())
            tickfile_missing = reference - committed
            tickfile_only = committed - reference
            if tickfile_missing:
                logger.critical(
                    "tickfile_reconcile: %d minutes present in snapshot/order but MISSING from tickfile "
                    "(will be regenerated as gaps): %s",
                    len(tickfile_missing), sorted(tickfile_missing)[:20],
                )
            if tickfile_only:
                logger.critical(
                    "tickfile_reconcile: %d minutes present in tickfile but NOT in snapshot/order "
                    "(checkpoint/order bug signal): %s",
                    len(tickfile_only), sorted(tickfile_only)[:20],
                )
        except Exception:
            logger.debug("tickfile reconcile failed (best-effort)", exc_info=True)

    def _try_generate_tickfile(self, minute_key: str) -> None:
        """Generate tickfile for a minute. Thread-safe. Callable from any thread.

        Returns silently if no pending data found (already generated by other thread).
        On IO failure, re-inserts ALL popped data for flush_all_remaining retry.
        """
        import threading as _threading

        # Step 1: Dedup check + pop pending + seqno — all in ONE lock block
        with self._state.lock:
            if minute_key in self._state._generated_tickfile_minutes:
                logger.debug("Tickfile dedup: minute=%s already generated, skipping", minute_key)
                return
            pending = self._state._tickfile_pending.pop(minute_key, None)
            if pending is None:
                # Diagnostic counter: approximate, GIL-dependent. Acceptable for monitoring.
                if self._engine_ref is not None:
                    self._engine_ref._tickfile_queue_skip_count += 1
                if minute_key not in self._tickfile_skip_warned_keys:
                    self._tickfile_skip_warned_keys.add(minute_key)
                    logger.warning(
                        "Tickfile skipped: no pending data for minute=%s [thread=%s, queue_depth=%d]",
                        minute_key,
                        threading.current_thread().name,
                        self._tickfile_queue.qsize() if self._tickfile_queue else 0,
                    )
                else:
                    logger.debug(
                        "Tickfile skipped (already warned): minute=%s",
                        minute_key,
                    )
                return
            # Pop order records (same lock block — Implementation Note #2)
            order_records = self._state.raw_order_buffers.pop(minute_key, [])
            latest_order_copy = dict(self._state.latest_order_by_symbol)
            # Increment seqno after guard passes (Implementation Note #2)
            self._state._tickfile_seqno += 1
            current_seqno = self._state._tickfile_seqno

        # Step 2: Select + Write (outside lock)
        start_ts = time.monotonic()
        try:
            from minute_bar.tickfile import select_tickfile_records
            from minute_bar.writer import get_tickfile_path, write_tickfile_rows

            raw_records = pending['raw_records']
            snapshot_copy = pending['snapshot_copy']
            code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
            selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)

            if not selected:
                logger.warning("Tickfile: no records selected for minute=%s", minute_key)
                # Data has been popped but no records to write — mark as processed
                # (Implementation Note #6: prevents double-enqueue second attempt)
                with self._state.lock:
                    self._state._generated_tickfile_minutes.add(minute_key)
                return

            write_tickfile_rows(self._output_dir, minute_key, selected, current_seqno,
                                code_table_getter=code_getter,
                                enable_commit_marker=self._enable_tickfile_commit_marker)

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

            # Step 3: Mark as generated AFTER successful write (Implementation Note #1)
            with self._state.lock:
                self._state._generated_tickfile_minutes.add(minute_key)

        except Exception:
            # Re-insert ALL popped data so flush_all_remaining can retry.
            # Only re-insert if not already generated (partial write protection — Fix-F)
            with self._state.lock:
                if minute_key not in self._state._generated_tickfile_minutes:
                    self._state._tickfile_pending[minute_key] = pending
                    if order_records:
                        self._state.raw_order_buffers[minute_key] = order_records
                else:
                    logger.warning(
                        "Tickfile IO error: partial write for minute=%s, skipping re-insert "
                        "(partial data may exist on disk)",
                        minute_key,
                    )
            logger.warning("Tickfile IO failed for minute=%s, re-inserted for retry [thread=%s]",
                           minute_key, _threading.current_thread().name)
            raise

    def _enqueue_tickfile(self, minute_key: str) -> None:
        """Enqueue a tickfile generation request. Thread-safe. Non-blocking."""
        if self._tickfile_queue is None:
            return
        self._tickfile_queue.put_nowait(minute_key)
        if self._engine_ref is not None:
            self._engine_ref._tickfile_enqueue_count += 1
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

    def _write_checkpoint(self) -> None:
        files_snapshot: Dict[str, FileState]
        if self._checkpoint_lock is not None:
            with self._checkpoint_lock:
                files_snapshot = dict(self._file_states)
        else:
            files_snapshot = dict(self._file_states)

        self._checkpoint.write(
            date=self._state.last_output_date,
            last_seqno=self._state.seqno,
            output_minutes=self._state.output_minutes,
            last_output_minute=self._state.last_output_minute,
            current_minute=self._state.current_minute,
            last_output_date=self._state.last_output_date,
            first_data_received=self._state.first_data_received,
            files=files_snapshot,
            last_totalvol_by_symbol=self._state.last_totalvol_by_symbol,
            last_totalamount_by_symbol=self._state.last_totalamount_by_symbol,
        )

    def _step4_handle_late_records(self) -> None:
        with self._state.lock:
            late_records = self._state.pop_late_snapshot_records()

        if not late_records:
            return

        grouped: dict[str, list[SnapshotRecord]] = {}
        for minute_key, record in late_records:
            grouped.setdefault(minute_key, []).append(record)

        for minute_key in sorted(grouped):
            path = get_snapshot_file_path(self._output_dir, minute_key)
            if os.path.exists(path):
                append_snapshot_records(path, grouped[minute_key], self._code_table)
            else:
                logger.warning("Late snapshot minute %s file missing", minute_key)

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

    def _step5_write_checkpoint(self) -> None:
        with self._state.lock:
            if self._state._late_snapshot_records:
                logger.debug(
                    "Skipping checkpoint: %d late records pending",
                    len(self._state._late_snapshot_records),
                )
                return
        self._write_checkpoint()

    def _reroute_buffer_to_late_queue(self, minute_keys: list) -> None:
        """Move buffer data for already-flushed minutes to the late queue.

        Happens when data arrives during the race window between buffer pop
        and flushed_snapshot_minutes.add() in _flush_minutes_internal.
        """
        rerouted = 0
        dropped = 0
        ohlcv_dropped_symbols = 0
        with self._state.lock:
            for k in minute_keys:
                ohlcv = self._state.ohlcv_buffers.pop(k, None)
                if ohlcv:
                    ohlcv_dropped_symbols += len(ohlcv)

                raw = self._state.raw_snapshot_buffers.pop(k, None)
                if raw:
                    for symbol, records in raw.items():
                        for record in records:
                            if len(self._state._late_snapshot_records) < MAX_LATE_SNAPSHOT_QUEUE_SIZE:
                                self._state._late_snapshot_records.append((k, record))
                                rerouted += 1
                            else:
                                dropped += 1
                self._state._snapshot_at_minute_end.pop(k, None)
                if self._enable_tickfile:
                    order_buf = self._state.raw_order_buffers.pop(k, None)
                    # DO NOT pop _tickfile_pending — tickfile generation must proceed
                    # even when snapshot data is re-routed to late queue.
                    # See: INV-TF1 — only _try_generate_tickfile and _step1_cross_day_check
                    # may remove entries from _tickfile_pending.
                    if order_buf:
                        logger.debug("Reroute: popped %d order records for minute=%s (no tickfile)",
                                     len(order_buf), k)
        if rerouted > 0 or dropped > 0 or ohlcv_dropped_symbols > 0:
            logger.warning(
                "Re-routed %d snapshot records to late queue for %d already-flushed minutes "
                "(dropped %d ohlcv symbols, %d raw records hit queue limit)",
                rerouted, len(minute_keys), ohlcv_dropped_symbols, dropped,
            )
