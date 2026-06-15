"""Phase 21 warmup test.

Tests that all Phase 21 Rust functions warmup successfully at startup.
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestPhase21Warmup:
    """Test Phase 21 Rust function warmup."""

    def test_process_order_batch_warmup(self):
        """Test process_order_batch warmup with empty batch."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return  # Skip if Rust not available

        result = process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])

        # Must return 5-tuple
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 5, f"Expected 5 elements, got {len(result)}"

        per_minute_buf, late_order_buf, latest_order_buf, late_minute_keys, skipped = result

        # Validate magic bytes in each buffer
        assert per_minute_buf[0:4] == b'\xaa\xbb\xcc\x01', \
            f"per_minute magic invalid: {per_minute_buf[0:4].hex()}"
        assert late_order_buf[0:4] == b'\xaa\xbb\xcc\x02', \
            f"late_order magic invalid: {late_order_buf[0:4].hex()}"
        assert latest_order_buf[0:4] == b'\xaa\xbb\xcc\x03', \
            f"latest_order magic invalid: {latest_order_buf[0:4].hex()}"

        # Skipped should be 0 for empty input
        assert skipped == 0, f"Expected 0 skipped, got {skipped}"

        # late_minute_keys should be empty
        assert len(late_minute_keys) == 0

    def test_parse_snapshot_batch_warmup(self):
        """Test parse_snapshot_batch warmup with empty batch."""
        try:
            from minute_bar._order_accel import parse_snapshot_batch
        except ImportError:
            return

        result = parse_snapshot_batch([], 'utf-8')

        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 2, f"Expected 2 elements, got {len(result)}"

        records, skipped = result
        assert isinstance(records, list), f"Expected list, got {type(records)}"
        assert len(records) == 0, f"Expected 0 records, got {len(records)}"
        assert skipped == 0

    def test_aggregate_snapshot_batch_warmup(self):
        """Test aggregate_snapshot_batch warmup with empty inputs."""
        try:
            from minute_bar._order_accel import aggregate_snapshot_batch
        except ImportError:
            return

        result = aggregate_snapshot_batch([], "202605280900", [], [], [])

        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 5, f"Expected 5 elements, got {len(result)}"

        ohlcv_buf, new_minute, base_vol, base_amt, late_keys = result

        # Magic should be valid
        assert ohlcv_buf[0:4] == b'\xaa\xbb\xcc\x04', \
            f"ohlcv magic invalid: {ohlcv_buf[0:4].hex()}"

        # new_minute should be preserved input
        assert new_minute == "202605280900"

        assert isinstance(base_vol, list)
        assert isinstance(base_amt, list)
        assert len(late_keys) == 0

    def test_tickfile_generate_warmup(self):
        """Test tickfile_generate warmup with empty buffers."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        result = tickfile_generate(b'', b'', b'', "202605280900", [], 0)

        assert isinstance(result, str), f"Expected str, got {type(result)}"
        # Should return header only
        assert "InstrumentID" in result
        assert "TradingDay" in result

    def test_rust_reset_state(self):
        """Test rust_reset_state succeeds without panic."""
        try:
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return

        # Should not raise
        rust_reset_state()

    def test_rust_reset_snapshot_state(self):
        """Test rust_reset_snapshot_state succeeds without panic."""
        try:
            from minute_bar._order_accel import rust_reset_snapshot_state
        except ImportError:
            return

        rust_reset_snapshot_state()

    def test_is_available(self):
        """Test is_available returns True when Rust is functional."""
        try:
            from minute_bar._order_accel import is_available
        except ImportError:
            return

        assert is_available() is True

    def test_all_magic_bytes_valid_in_warmup(self):
        """Assert magic bytes valid in all returned buffers from all Phase 21 functions."""
        try:
            from minute_bar._order_accel import (
                process_order_batch,
                parse_snapshot_batch,
                aggregate_snapshot_batch,
                tickfile_generate,
            )
        except ImportError:
            return

        # process_order_batch
        result = process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])
        assert result[0][0:4] == b'\xaa\xbb\xcc\x01'
        assert result[1][0:4] == b'\xaa\xbb\xcc\x02'
        assert result[2][0:4] == b'\xaa\xbb\xcc\x03'

        # parse_snapshot_batch (tuple return)
        parsed = parse_snapshot_batch([], 'utf-8')
        assert isinstance(parsed[0], list)

        # aggregate_snapshot_batch
        agg_result = aggregate_snapshot_batch([], "202605280900", [], [], [])
        assert agg_result[0][0:4] == b'\xaa\xbb\xcc\x04'

        # tickfile_generate
        csv = tickfile_generate(b'', b'', b'', "202605280900", [], 0)
        assert "InstrumentID" in csv


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
