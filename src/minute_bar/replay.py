from __future__ import annotations

import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set

from minute_bar.aggregator import SharedState, build_order_record, build_snapshot_record
from minute_bar.clock import time_to_minute_key
from minute_bar.code_table import CodeTable
from minute_bar.config import AppConfig
from minute_bar.csv_parser import parse_order_line, parse_snapshot_line
from minute_bar.file_tailer import FileTailer
from minute_bar.writer import (
    append_order_records,
    append_snapshot_records,
    get_order_file_path,
    get_snapshot_file_path,
    write_kline_file,
    write_order_file,
    write_snapshot_file,
)

logger = logging.getLogger(__name__)

DELAYED_FLUSH_ROUNDS = 3


class ReplayEngine:
    def __init__(self, config: AppConfig, date: str):
        self._config = config
        self._date = date
        self._code_table = CodeTable(
            config.input.csv_dir,
            encoding=config.input.file_encoding,
            chunk_size=config.input.chunk_size_bytes,
        )
        self._enable_tickfile = config.output.enable_tickfile
        self._tickfile_seqno: int = 0
        # Part 2 (stale-fix): minutes already present in the output tickfile;
        # populated by startup scan, used to skip already-generated minutes (fill only gaps).
        self._generated_tickfile_minutes: set = set()

    def _scan_generated_tickfile_minutes(self, output_dir: str) -> set:
        """Scan the day's tickfile; return the set of minute_keys already present.

        tickfile is ONE file per day (tickfile_{date}.csv); the UpdateTime column
        (index 16) is minute_key-derived (tickfile.py), so each row's minute is
        recoverable. Returns empty set if no tickfile exists (pure replay unaffected).
        Spec 2026-06-16-tickfile-stale-fix-design §4.1.
        """
        from minute_bar.writer import get_tickfile_path
        sample_mk = f"{self._date}0000"   # any minute of the date -> same per-day path
        path = get_tickfile_path(output_dir, sample_mk)
        if not os.path.exists(path):
            return set()
        return self._extract_minutes_from_tickfile(path)

    @staticmethod
    def _extract_minutes_from_tickfile(path: str) -> set:
        from minute_bar.writer import extract_minutes_from_tickfile
        return extract_minutes_from_tickfile(path)

    def run(self) -> None:
        logger.info("Replay mode: loading data for date %s", self._date)

        self._code_table.load(self._date)
        logger.info("Code table: %d symbols", len(self._code_table.table))

        enable_order = self._config.output.enable_order
        stop_event = threading.Event()

        snapshot_range = {"first": None, "last": None, "count": 0}
        order_range = {"first": None, "last": None, "count": 0}

        self._state = SharedState(
            first_seen_volume_base=self._config.aggregation.first_seen_volume_base
        )

        # Part 2 (stale-fix) + commit-marker (T6): learn which minutes are already
        # COMMITTED in the output tickfile so we skip them (fill only gaps). When a
        # sidecar commit file exists it is authoritative and the tickfile is truncated
        # to the last committed offset (drops partial mid-append tails + un-sidecared
        # stale rows). Falls back to a row scan when there is no sidecar or recovery
        # raises. No-op when no tickfile exists (pure replay).
        if self._enable_tickfile:
            from minute_bar.writer import _recover_tickfile_to_last_commit
            try:
                committed_set, last_seqno, had_sidecar = _recover_tickfile_to_last_commit(
                    self._config.output.output_dir, self._date,
                    enable_commit_marker=self._config.recovery.enable_tickfile_commit_marker)
            except Exception:
                logger.exception("Replay tickfile recovery failed; falling back to row scan")
                committed_set, last_seqno, had_sidecar = self._scan_generated_tickfile_minutes(
                    self._config.output.output_dir), 0, False
            if had_sidecar or committed_set:
                # sidecar authoritative OR fallback row-scan already populated committed_set
                self._generated_tickfile_minutes = committed_set
            else:
                # legacy fallback (no sidecar, empty) -> scan
                self._generated_tickfile_minutes = self._scan_generated_tickfile_minutes(
                    self._config.output.output_dir)
            self._tickfile_seqno = max(self._tickfile_seqno, last_seqno)  # INV-CM-SEQNO-MONO-FILE
            if self._generated_tickfile_minutes:
                logger.info("Replay: %d tickfile minutes committed (had_sidecar=%s) — fill only gaps",
                            len(self._generated_tickfile_minutes), had_sidecar)

        write_executor = ThreadPoolExecutor(max_workers=2)

        try:
            if enable_order:
                order_future = write_executor.submit(
                    self._stream_orders, stop_event, order_range
                )
            else:
                order_future = None

            self._stream_snapshots(stop_event, write_executor, snapshot_range)

            if order_future is not None:
                order_future.result()

        except Exception:
            stop_event.set()
            raise
        finally:
            write_executor.shutdown(wait=True)

        self._write_summary(snapshot_range, order_range, enable_order)

    def _stream_snapshots(
        self,
        stop_event: threading.Event,
        write_executor: ThreadPoolExecutor,
        minute_range: dict,
    ) -> None:
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

                self._state.process_snapshot(parsed)
                current_round_minutes.add(minute_key)

                if minute_range["first"] is None:
                    minute_range["first"] = minute_key
                minute_range["last"] = minute_key

            # Flush late records collected this round
            if late_records_by_minute:
                self._flush_late_snapshots(
                    late_records_by_minute, output_dir, code_table
                )
                late_records_by_minute.clear()

            # Delayed flush: flush minutes not seen in recent rounds
            active_history.append(current_round_minutes)
            if len(active_history) > DELAYED_FLUSH_ROUNDS:
                stale_round = active_history.pop(0)
                to_flush = stale_round - current_round_minutes
                for mk in sorted(to_flush):
                    if mk not in flushed_minutes and mk in self._state.ohlcv_buffers:
                        self._flush_snapshot_minute(
                            mk, output_dir, code_table,
                            full_snapshot, full_kline, enable_kline, write_executor,
                        )
                        flushed_minutes.add(mk)

        # EOF final flush: flush all remaining buffered minutes
        for mk in sorted(self._state.ohlcv_buffers):
            if mk not in flushed_minutes:
                self._flush_snapshot_minute(
                    mk, output_dir, code_table,
                    full_snapshot, full_kline, enable_kline, write_executor,
                )
                flushed_minutes.add(mk)
            else:
                # Residual data in buffer for already-flushed minute
                logger.warning("EOF: minute %s already flushed but buffer not empty — late append", mk)
                raw = self._state.raw_snapshot_buffers.get(mk, {})
                if raw:
                    path = get_snapshot_file_path(output_dir, mk)
                    for sym, recs in raw.items():
                        append_snapshot_records(path, recs, code_table)
                    with self._state.lock:
                        for recs in raw.values():
                            for rec in recs:
                                self._state.maybe_update_latest_unlocked(rec)
                        late_snapshot_count += sum(len(r) for r in raw.values())
                        late_snapshot_minutes.add(mk)
                self._state.ohlcv_buffers.pop(mk, None)
                self._state.raw_snapshot_buffers.pop(mk, None)

        tailer.close()

        if self._enable_tickfile:
            with self._state.lock:
                orphaned_keys = list(self._state.raw_order_buffers.keys())
                orphaned_count = sum(len(self._state.raw_order_buffers[k]) for k in orphaned_keys)
            if orphaned_keys:
                logger.info(
                    "Tickfile orphaned order buffers: %d keys, %d total records (replay final flush)",
                    len(orphaned_keys), orphaned_count,
                )

        minute_range["count"] = len(flushed_minutes)
        minute_range["late_snapshot_count"] = late_snapshot_count
        minute_range["late_snapshot_minutes"] = sorted(late_snapshot_minutes)

        logger.info(
            "Snapshot stream complete: %d minutes across %s..%s (%d late records)",
            len(flushed_minutes),
            minute_range["first"], minute_range["last"],
            late_snapshot_count,
        )

    def _flush_snapshot_minute(
        self,
        minute_key: str,
        output_dir: str,
        code_table: CodeTable,
        full_snapshot: bool,
        full_kline: bool,
        enable_kline: bool,
        write_executor: ThreadPoolExecutor,
    ) -> None:
        with self._state.lock:
            ohlcv_data = dict(self._state.ohlcv_buffers.get(minute_key, {}))
            raw_records = dict(self._state.raw_snapshot_buffers.get(minute_key, {}))
            # Use per-minute snapshot if available, fallback to current latest_snapshot
            snapshot_copy = self._state._snapshot_at_minute_end.pop(minute_key, None)
            if snapshot_copy is None:
                snapshot_copy = dict(self._state.latest_snapshot)

        futures = []
        futures.append(write_executor.submit(
            write_snapshot_file, output_dir, minute_key, snapshot_copy,
            ohlcv_data, code_table, full=full_snapshot, raw_records=raw_records,
        ))
        if enable_kline:
            futures.append(write_executor.submit(
                write_kline_file, output_dir, minute_key, snapshot_copy,
                ohlcv_data, code_table, full=full_kline,
            ))

        for f in futures:
            f.result()

        with self._state.lock:
            for sym, agg in ohlcv_data.items():
                self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
                self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
            self._state.ohlcv_buffers.pop(minute_key, None)
            self._state.raw_snapshot_buffers.pop(minute_key, None)

        if self._enable_tickfile:
            # Part 2 (stale-fix) + commit-marker (T6): skip minutes already COMMITTED
            # in the output tickfile (fill only gaps; do not duplicate/corrupt rows).
            # Check BEFORE seqno increment so skipped minutes do not burn seqno numbers.
            # Seqno is now seeded once at run() start from sidecar recovery (no lazy
            # per-minute file scan); INV-CM-SEQNO-MONO-FILE.
            if minute_key in self._generated_tickfile_minutes:
                logger.debug("Replay skip already-generated tickfile minute=%s", minute_key)
            else:
                from minute_bar.writer import write_tickfile_rows
                from minute_bar.tickfile import select_tickfile_records
                self._tickfile_seqno += 1
                current_seqno = self._tickfile_seqno
                with self._state.lock:
                    order_records = self._state.raw_order_buffers.pop(minute_key, [])
                    latest_order_copy = dict(self._state.latest_order_by_symbol)
                code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
                selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)
                write_tickfile_rows(output_dir, minute_key, selected, current_seqno,
                                    code_table_getter=code_getter, skip_fsync=False,
                                    enable_commit_marker=self._config.recovery.enable_tickfile_commit_marker)
                self._generated_tickfile_minutes.add(minute_key)

        logger.info(
            "Output snapshot minute %s (%d updated, %d total)",
            minute_key, len(ohlcv_data), len(snapshot_copy),
        )

    def _flush_late_snapshots(
        self,
        late_records_by_minute: dict[str, list],
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
                    with self._state.lock:
                        for rec in records:
                            self._state.maybe_update_latest_unlocked(rec)
            else:
                logger.warning("Late snapshot minute %s file missing", minute_key)

        logger.info(
            "Flushed %d late snapshot records across %d minutes",
            sum(len(v) for v in late_records_by_minute.values()),
            len(late_records_by_minute),
        )

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
        pending_state_orders: list = []

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
                if self._enable_tickfile:
                    pending_state_orders.append((minute_key, record))
                current_round_minutes.add(minute_key)

                if minute_range["first"] is None:
                    minute_range["first"] = minute_key
                minute_range["last"] = minute_key

            # Batch write to SharedState for tickfile generation
            if self._enable_tickfile and pending_state_orders:
                with self._state.lock:
                    for mk, rec in pending_state_orders:
                        self._state.raw_order_buffers.setdefault(mk, []).append(rec)
                        existing = self._state.latest_order_by_symbol.get(rec.symbol)
                        if existing is None or (rec.time, rec.rcvtime) >= (existing.time, existing.rcvtime):
                            self._state.latest_order_by_symbol[rec.symbol] = rec
                pending_state_orders.clear()

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
