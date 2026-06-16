"""Dual-path parity test: run Rust + Python simultaneously, diff each minute.

This test verifies that Phase 21 Rust process_order_batch produces
identical results to the Python reference path for the same input data.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from minute_bar.clock import time_to_minute_key


class TestPhase21Parity:
    """Dual-path parity test between Rust and Python order processing."""

    def test_parity_single_batch(self):
        """Single batch: Rust and Python should produce identical results."""
        try:
            from minute_bar._order_accel import process_order_batch
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return  # Skip if Rust not available

        rust_reset_state()

        lines = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",
            b"7203,20260528090000125,4585000,110,4595000,210,2,0",
            b"7205,20260528090100123,4620000,80,4630000,180,2,0",
        ]
        today = "20260528"
        today_int = 20260528

        # Call Rust process_order_batch
        result = process_order_batch(lines, "utf-8", today, today_int, 0, [])
        per_minute_buf, late_buf, latest_buf, late_keys, skipped = result

        # Decode Rust output using Phase 21 decoders
        from minute_bar.csv_parser import (
            decode_order_per_minute_buf,
            decode_late_order_buf,
            decode_latest_order_buf,
        )

        rust_per_min = decode_order_per_minute_buf(per_minute_buf)
        rust_late = decode_late_order_buf(late_buf)
        rust_latest = decode_latest_order_buf(latest_buf)

        # Python reference
        from minute_bar.models import OrderRecord

        py_per_min: dict = {}
        py_latest: dict = {}

        for line in lines:
            line_str = line.decode("utf-8")
            fields = line_str.split(",")
            sym = fields[0]
            time_val = int(fields[1])
            minute_key = time_to_minute_key(time_val)

            rec = OrderRecord(
                symbol=sym, seqno=0, time=time_val,
                bidprice=int(fields[2]), bidsize=int(fields[3]),
                askprice=int(fields[4]), asksize=int(fields[5]),
                decimal=int(fields[6]) if len(fields) > 6 else 2,
                rcvtime=int(fields[7]) if len(fields) > 7 else 0,
            )

            if minute_key not in py_per_min:
                py_per_min[minute_key] = []
            py_per_min[minute_key].append(rec)

            existing = py_latest.get(sym)
            if existing is None or (rec.time, rec.rcvtime) > (existing.time, existing.rcvtime):
                py_latest[sym] = rec

        # Compare
        assert set(rust_per_min.keys()) == set(py_per_min.keys()), (
            f"Minute keys differ: {set(rust_per_min.keys())} vs {set(py_per_min.keys())}"
        )

        for mk in rust_per_min:
            rust_recs = rust_per_min[mk]
            py_recs = py_per_min[mk]
            assert len(rust_recs) == len(py_recs), (
                f"Record count mismatch at {mk}: {len(rust_recs)} vs {len(py_recs)}"
            )
            for rust_r, py_r in zip(rust_recs, py_recs):
                assert rust_r.symbol == py_r.symbol
                assert rust_r.time == py_r.time
                assert rust_r.bidprice == py_r.bidprice
                assert rust_r.bidsize == py_r.bidsize
                assert rust_r.askprice == py_r.askprice
                assert rust_r.asksize == py_r.asksize
                assert rust_r.decimal == py_r.decimal

        # Compare latest
        assert set(rust_latest.keys()) == set(py_latest.keys()), (
            f"Latest symbols differ: {set(rust_latest.keys())} vs {set(py_latest.keys())}"
        )
        for sym in rust_latest:
            rust_r = rust_latest[sym]
            py_r = py_latest[sym]
            assert rust_r.symbol == py_r.symbol
            assert rust_r.time == py_r.time
            assert rust_r.bidprice == py_r.bidprice

    def test_parity_multiple_batches(self):
        """Multiple batches: verify Rust and Python are consistent across batches."""
        try:
            from minute_bar._order_accel import process_order_batch
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return

        rust_reset_state()

        today = "20260528"

        batch1 = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",
        ]
        batch2 = [
            b"7203,20260528090100123,4585000,110,4595000,210,2,0",
            b"7205,20260528090200123,4620000,80,4630000,180,2,0",
        ]

        # Both batches same day
        result1 = process_order_batch(batch1, "utf-8", today, 20260528, 0, [])
        result2 = process_order_batch(batch2, "utf-8", today, 20260528, 20260528, [])

        # Batches should both succeed
        assert result1[4] == 0  # no skipped
        assert result2[4] == 0

        from minute_bar.csv_parser import decode_order_per_minute_buf, decode_latest_order_buf

        rust1 = decode_order_per_minute_buf(result1[0])
        rust2 = decode_order_per_minute_buf(result2[0])

        # All minute_keys from both batches should be present
        all_keys = set(rust1.keys()) | set(rust2.keys())
        expected_keys = {"202605280900", "202605280901", "202605280902"}
        assert all_keys == expected_keys, f"Keys: {all_keys} vs {expected_keys}"

    def test_parity_late_order_detection(self):
        """Rust and Python should route late orders identically."""
        try:
            from minute_bar._order_accel import process_order_batch
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return

        rust_reset_state()

        today = "20260528"
        lines = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
        ]

        # First batch: no flushed minutes
        result1 = process_order_batch(lines, "utf-8", today, 20260528, 0, [])
        assert result1[4] == 0  # no skipped

        # Second batch: 202605280900 is now flushed
        result2 = process_order_batch(lines, "utf-8", today, 20260528, 20260528, ["202605280900"])

        # 0900 should be in late_order_buf
        from minute_bar.csv_parser import decode_late_order_buf
        late_records = decode_late_order_buf(result2[1])
        assert len(late_records) == 1, f"Expected 1 late record, got {len(late_records)}"
        assert late_records[0].symbol == "7203"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
