"""Phase 21 late-order batch write tests.

Spec: docs/superpowers/specs/2026-06-15-phase21-late-order-batch-write-fix.md

The Phase 21 Rust pipeline decodes `late_order_buf` into a flat list of late
OrderRecords. The order thread must write these to disk grouped by minute_key
(one `append_order_records`/`write_order_file` call per minute) rather than
calling `open()` once per record — the per-record path deadlocks the order
thread on file I/O during the 0900 open peak (watermark stuck at 0907).
"""
from unittest.mock import patch

from minute_bar.clock import time_to_minute_key
from minute_bar.config import AppConfig, InputConfig, OutputConfig
from minute_bar.engine import Engine
from minute_bar.models import OrderRecord


def make_config(enable_tickfile=False, **overrides):
    defaults = dict(
        input=InputConfig(csv_dir="/tmp/input"),
        output=OutputConfig(output_dir="/tmp/output", enable_tickfile=enable_tickfile),
    )
    defaults.update(overrides)
    return AppConfig(**defaults)


def make_engine(config=None, enable_tickfile=False):
    with patch("minute_bar.engine.ClockWatermarkFlusher"), \
         patch("minute_bar.engine.CodeTable"), \
         patch("minute_bar.engine.FileTailer"), \
         patch("minute_bar.engine.CheckpointManager"):
        if config is None:
            config = make_config(enable_tickfile=enable_tickfile)
        return Engine(config)


def _rec(symbol: str, time: int, seqno: int = 0) -> OrderRecord:
    return OrderRecord(
        symbol=symbol, seqno=seqno, time=time,
        bidprice=100, bidsize=10, askprice=101, asksize=20,
        decimal=2, rcvtime=time,
    )


def _records_spanning_minutes():
    """5 records across 2 minutes:
       - minute '202605280900': 3 records (in insertion order)
       - minute '202605280901': 2 records
    """
    return [
        _rec("7203", 20260528090000100),
        _rec("7203", 20260528090000200),
        _rec("1301", 20260528090000300),
        _rec("7203", 20260528090100100),
        _rec("1301", 20260528090100200),
    ]


class TestWriteLateOrdersBatchAppendPath:
    """When late-minute files already exist → one append_order_records per minute."""

    def test_one_append_call_per_minute_not_per_record(self):
        engine = make_engine()
        records = _records_spanning_minutes()

        with patch("minute_bar.engine.os.path.exists", return_value=True) as mock_exists, \
             patch("minute_bar.engine.append_order_records") as mock_append, \
             patch("minute_bar.engine.write_order_file") as mock_write:
            engine._write_late_orders_batch(records, "/tmp/output", [])

        # One append per unique minute — NOT one per record (the bug)
        assert mock_append.call_count == 2
        # Create path never taken when files exist
        assert mock_write.call_count == 0
        # os.path.exists checked once per minute (2), not once per record (5)
        assert mock_exists.call_count == 2

    def test_append_receives_correct_records_grouped_by_minute_in_order(self):
        engine = make_engine()
        records = _records_spanning_minutes()

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records") as mock_append, \
             patch("minute_bar.engine.write_order_file"):
            engine._write_late_orders_batch(records, "/tmp/output", [])

        # Build minute_key -> records passed to append
        by_minute = {}
        for call in mock_append.call_args_list:
            path, recs = call.args
            minute_key = path  # path encodes minute; we map via records' minute instead
            for r in recs:
                by_minute.setdefault(time_to_minute_key(r.time), []).append(r)

        # minute 0901 got exactly its 3 records (was 0900 input → floor+1 → 0901)
        assert len(by_minute["202605280901"]) == 3
        assert [r.time for r in by_minute["202605280901"]] == \
            [20260528090000100, 20260528090000200, 20260528090000300]
        # minute 0902 got exactly its 2 records (was 0901 input → floor+1 → 0902)
        assert len(by_minute["202605280902"]) == 2
        assert [r.time for r in by_minute["202605280902"]] == \
            [20260528090100100, 20260528090100200]


class TestWriteLateOrdersBatchCreatePath:
    """When a late-minute file does NOT exist → write_order_file (create)."""

    def test_missing_file_uses_write_order_file_per_minute(self):
        engine = make_engine()
        records = _records_spanning_minutes()

        with patch("minute_bar.engine.os.path.exists", return_value=False), \
             patch("minute_bar.engine.append_order_records") as mock_append, \
             patch("minute_bar.engine.write_order_file") as mock_write:
            engine._write_late_orders_batch(records, "/tmp/output", [])

        assert mock_write.call_count == 2
        assert mock_append.call_count == 0


class TestWriteLateOrdersBatchInvariants:
    """Counting / set / tickfile-routing invariants (spec §6.1)."""

    def test_late_order_count_equals_total_records(self):
        engine = make_engine()
        records = _records_spanning_minutes()

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records"), \
             patch("minute_bar.engine.write_order_file"):
            engine._write_late_orders_batch(records, "/tmp/output", [])

        assert engine._late_order_count == 5

    def test_late_order_minutes_set_captures_all_unique_minutes(self):
        engine = make_engine()
        records = _records_spanning_minutes()

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records"), \
             patch("minute_bar.engine.write_order_file"):
            engine._write_late_orders_batch(records, "/tmp/output", [])

        assert engine._late_order_minutes == {"202605280901", "202605280902"}

    def test_tickfile_enabled_routes_every_record_as_late(self):
        engine = make_engine(enable_tickfile=True)
        records = _records_spanning_minutes()
        pending: list = []

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records"), \
             patch("minute_bar.engine.write_order_file"):
            engine._write_late_orders_batch(records, "/tmp/output", pending)

        # Every late record routed to pending with __LATE__ marker, in order
        late_entries = [e for e in pending if e[0] == "__LATE__"]
        assert len(late_entries) == 5
        assert [e[1].time for e in late_entries] == [r.time for r in records]

    def test_tickfile_disabled_does_not_touch_pending(self):
        engine = make_engine(enable_tickfile=False)
        records = _records_spanning_minutes()
        pending: list = []

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records"), \
             patch("minute_bar.engine.write_order_file"):
            engine._write_late_orders_batch(records, "/tmp/output", pending)

        assert pending == []

    def test_empty_input_is_noop(self):
        engine = make_engine()

        with patch("minute_bar.engine.os.path.exists", return_value=True), \
             patch("minute_bar.engine.append_order_records") as mock_append, \
             patch("minute_bar.engine.write_order_file") as mock_write:
            engine._write_late_orders_batch([], "/tmp/output", [])

        assert mock_append.call_count == 0
        assert mock_write.call_count == 0
        assert engine._late_order_count == 0
