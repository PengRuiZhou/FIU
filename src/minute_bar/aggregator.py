from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional

from minute_bar.clock import time_to_minute_key
from minute_bar.csv_parser import ParsedOrder, ParsedSnapshot
from minute_bar.models import OHLCVAggregate, OrderRecord, SnapshotRecord
from minute_bar.validator import validate_snapshot

logger = logging.getLogger(__name__)

MAX_LATE_SNAPSHOT_QUEUE_SIZE = 50000


def build_snapshot_record(parsed: ParsedSnapshot, seqno: int) -> Optional[SnapshotRecord]:
    return SnapshotRecord(
        symbol=parsed.symbol,
        seqno=seqno,
        time=parsed.time,
        rcvtime=parsed.rcvtime,
        preclose=float(parsed.preclose),
        lastprice=float(parsed.lastprice),
        open=float(parsed.open),
        high=float(parsed.high),
        low=float(parsed.low),
        close=float(parsed.close),
        lasttradeprice=float(parsed.lasttradeprice),
        lasttradeqty=parsed.lasttradeqty,
        totalvol=parsed.totalvol,
        totalamount=float(parsed.totalamount),
        sessionid=parsed.sessionid,
        tradetype=parsed.tradetype,
        status=parsed.status,
        direction=parsed.direction,
        pflag=parsed.pflag,
        decimal=parsed.decimal,
        vwap=float(parsed.vwap),
        shortsellflag=parsed.shortsellflag,
    )


def build_order_record(parsed: ParsedOrder, seqno: int) -> OrderRecord:
    return OrderRecord(
        symbol=parsed.symbol,
        seqno=seqno,
        time=parsed.time,
        bidprice=parsed.bidprice,
        bidsize=parsed.bidsize,
        askprice=parsed.askprice,
        asksize=parsed.asksize,
        decimal=parsed.decimal,
        rcvtime=parsed.rcvtime,
    )


class SharedState:
    def __init__(self, first_seen_volume_base: str = "start_totalvol"):
        self.lock = threading.RLock()
        self.ohlcv_buffers: Dict[str, Dict[str, OHLCVAggregate]] = {}
        self.raw_snapshot_buffers: Dict[str, Dict[str, List[SnapshotRecord]]] = {}
        self.raw_order_buffers: Dict[str, List[OrderRecord]] = {}
        self.latest_snapshot: Dict[str, SnapshotRecord] = {}
        self.last_totalvol_by_symbol: Dict[str, int] = {}
        self.last_totalamount_by_symbol: Dict[str, float] = {}
        self.seqno: int = 0
        self.current_minute: str = ""
        self.first_data_received: bool = False
        self.last_output_date: str = ""
        self.first_seen_volume_base = first_seen_volume_base

        self.last_output_minute: str = ""
        self.output_minutes: set[str] = set()

        # Late record tracking
        self.flushed_snapshot_minutes: set[str] = set()
        self._late_snapshot_records: list[tuple[str, SnapshotRecord]] = []
        self.late_snapshot_count: int = 0
        self.late_snapshot_minutes: set[str] = set()

        # Per-minute snapshot of latest_snapshot, captured when current_minute advances.
        # Ensures carry-forward rows never contain data from future minutes.
        self._snapshot_at_minute_end: Dict[str, Dict[str, SnapshotRecord]] = {}

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
        # Minutes that have been successfully generated as tickfile (dedup guard).
        # Cleared on cross-day reset. Memory: ~330 entries × 20 bytes ≈ 7KB.
        self._generated_tickfile_minutes: set = set()
        # Minutes that have been flushed to order output files.
        # Used by Fix-C Layer 3 for tickfile completeness check.
        self.flushed_order_minutes: set = set()

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
                if len(self._late_snapshot_records) >= MAX_LATE_SNAPSHOT_QUEUE_SIZE:
                    logger.warning(
                        "Late snapshot queue full (%d), dropping record for minute=%s symbol=%s",
                        MAX_LATE_SNAPSHOT_QUEUE_SIZE, minute_key, record.symbol,
                    )
                    return False
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

            # Advance current_minute BEFORE updating latest_snapshot.
            # Snapshot latest_snapshot at the moment of minute boundary to prevent
            # carry-forward rows from containing future data.
            if not self.current_minute or minute_key > self.current_minute:
                if self.current_minute:
                    self._snapshot_at_minute_end[self.current_minute] = dict(self.latest_snapshot)
                self.current_minute = minute_key

            self.latest_snapshot[symbol] = record

            if not self.first_data_received:
                self.first_data_received = True
                logger.info("First data received at minute=%s", minute_key)

            return False

    def process_order(self, parsed: ParsedOrder) -> None:
        """TEST ONLY: do not call from live mode. Calling from live _order_loop would cause seqno duplication."""
        with self.lock:
            self.seqno += 1
            record = build_order_record(parsed, seqno=self.seqno)
            minute_key = time_to_minute_key(record.time)
            if minute_key not in self.raw_order_buffers:
                self.raw_order_buffers[minute_key] = []
            self.raw_order_buffers[minute_key].append(record)
            # Update latest_order_by_symbol cache
            existing = self.latest_order_by_symbol.get(record.symbol)
            if existing is None or (record.time, record.rcvtime) >= (existing.time, existing.rcvtime):
                self.latest_order_by_symbol[record.symbol] = record

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
