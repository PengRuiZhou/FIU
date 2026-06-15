"""Panic isolation tests for Phase 21 Rust functions.

Feed malformed input to each #[pyfunction] and assert the Python process survives.
"""
from __future__ import annotations

import sys
import os
import struct

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestRustPanicIsolation:
    """Assert Python process survives malformed input to Phase 21 functions."""

    def test_process_order_batch_malformed_lines(self):
        """process_order_batch should not panic on malformed lines."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return

        # Various malformed inputs
        test_cases = [
            # Empty lines list — should return empty result
            [],
            # Single invalid line
            [b"not_valid_csv_at_all"],
            # Binary garbage
            [b"\xff\xfe\xfd\xfc"],
            # Very long line (should not OOM)
            [b"7203," + b"x" * 1_000_000],
            # Missing fields
            [b"7203,20260528090000123"],
            # Extra fields
            [b"7203,20260528090000123,4580000,100,4590000,200,2,0,extra,fields,here"],
            # Non-UTF-8 with rust accel (encoding check)
            [b"\xff\xfe"],
            # Header line
            [b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime"],
            # Wrong date format
            [b"7203,20260528,4580000,100,4590000,200,2,0"],
        ]

        for lines in test_cases:
            try:
                result = process_order_batch(lines, 'utf-8', '20260528', 20260528, 0, [])
                # Should return 5-tuple on success
                assert isinstance(result, tuple)
                assert len(result) == 5
            except Exception as e:
                # Should not panic (raise RuntimeError from catch_unwind is OK)
                # but process should survive
                assert "panic" not in str(e).lower(), f"Panic raised for {lines[:50]}: {e}"

    def test_process_order_batch_corrupted_buffer(self):
        """process_order_batch should handle internal state corruption gracefully."""
        try:
            from minute_bar._order_accel import process_order_batch, rust_reset_state
        except ImportError:
            return

        # Reset to clean state
        rust_reset_state()

        # Valid batch first
        result1 = process_order_batch(
            [b"7203,20260528090000123,4580000,100,4590000,200,2,0"],
            'utf-8', '20260528', 20260528, 0, []
        )
        assert isinstance(result1, tuple)

        # Now call with empty — state should be clean
        result2 = process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])
        assert isinstance(result2, tuple)

    def test_parse_snapshot_batch_malformed_lines(self):
        """parse_snapshot_batch should not panic on malformed lines."""
        try:
            from minute_bar._order_accel import parse_snapshot_batch
        except ImportError:
            return

        test_cases = [
            [],
            [b"not_valid"],
            [b"\xff\xfe\xfd"],
            [b"symbol,time,preclose,lastprice,open,high,low,close,lasttradeprice,lasttradeqty,totalvol,totalamount,sessionid,tradetype,status,direction,pflag,decimal,vwap,shortsellflag,rcvtime"],
            [b",20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N"],
            [b"7203,not_a_time,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0"],
            [b"7203," + b"x" * 100000],
        ]

        for lines in test_cases:
            try:
                result = parse_snapshot_batch(lines, 'utf-8')
                assert isinstance(result, tuple)
                assert len(result) == 2
            except Exception as e:
                assert "panic" not in str(e).lower(), f"Panic for {lines[:30]}: {e}"

    def test_aggregate_snapshot_batch_malformed_data(self):
        """aggregate_snapshot_batch should not panic on empty/malformed inputs."""
        try:
            from minute_bar._order_accel import aggregate_snapshot_batch
        except ImportError:
            return

        test_cases = [
            # Empty batch
            ([], "202605280900", [], [], []),
            # Valid record then call with empty (after state was populated)
        ]

        for args in test_cases:
            try:
                result = aggregate_snapshot_batch(*args)
                assert isinstance(result, tuple)
                assert len(result) == 5
            except Exception as e:
                assert "panic" not in str(e).lower(), f"Panic for {args}: {e}"

    def test_tickfile_generate_malformed_buffers(self):
        """tickfile_generate should not panic on malformed buffers."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        test_cases = [
            # All empty
            (b'', b'', b'', "202605280900", ["7203"], 1),
            # Corrupted order buffer (wrong magic)
            (b'\x01\x02\x03\x04' + b'garbage', b'', b'', "202605280900", ["7203"], 1),
            # Truncated buffers
            (b'\xaa\xbb\xcc\x03\x01\x00', b'', b'', "202605280900", ["7203"], 1),
            # Corrupted snapshot buffer
            (b'', b'\xaa\xbb\xcc\x05\x01\x00' + b'garbage', b'', "202605280900", ["7203"], 1),
        ]

        for args in test_cases:
            try:
                result = tickfile_generate(*args)
                assert isinstance(result, str)
            except Exception as e:
                # Rust returns Err on magic mismatch (not panic)
                # Both are acceptable — process must survive
                assert "panic" not in str(e).lower(), f"Panic for {args}: {e}"

    def test_rust_reset_state_recovery(self):
        """rust_reset_state should recover from any internal state."""
        try:
            from minute_bar._order_accel import (
                process_order_batch, rust_reset_state, is_available
            )
        except ImportError:
            return

        # Process some data
        process_order_batch(
            [b"7203,20260528090000123,4580000,100,4590000,200,2,0"],
            'utf-8', '20260528', 20260528, 0, []
        )

        # Reset should not panic
        rust_reset_state()

        # Should still be available after reset
        assert is_available() is True

        # Should work normally after reset
        result = process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])
        assert isinstance(result, tuple)

    def test_rust_reset_snapshot_state_recovery(self):
        """rust_reset_snapshot_state should recover from any internal state."""
        try:
            from minute_bar._order_accel import (
                parse_snapshot_batch, aggregate_snapshot_batch,
                rust_reset_snapshot_state, is_available
            )
        except ImportError:
            return

        # Process some data
        parsed, _ = parse_snapshot_batch(
            [b"7203,20260528090000123,4580000,4590000,4600000,4610000,4570000,4585000,4590000,100,1000,1000000,12345,T,S,1,N,2,5000,0"],
            'utf-8'
        )
        aggregate_snapshot_batch(parsed, "202605280900", [], [], [])

        # Reset should not panic
        rust_reset_snapshot_state()

        # Should still be available after reset
        assert is_available() is True

        # Should work normally after reset
        result = aggregate_snapshot_batch([], "202605280900", [], [], [])
        assert isinstance(result, tuple)

    def test_large_batch_does_not_panic(self):
        """Large batch should return error, not panic."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return

        # Create a very large batch (exceeds MAX_BATCH_SIZE = 1_000_000)
        # This should return a ValueError, not panic
        large_batch = [b"7203,20260528090000123,4580000,100,4590000,200,2,0"] * 2_000_000

        try:
            result = process_order_batch(large_batch, 'utf-8', '20260528', 20260528, 0, [])
            # If it doesn't raise, something is wrong but not a panic
            assert False, "Expected ValueError for oversized batch"
        except ValueError as e:
            # Expected: batch too large
            assert "too large" in str(e).lower() or "max" in str(e).lower()
        except Exception as e:
            # RuntimeError from panic catch is also acceptable
            assert "panic" in str(e).lower() or "too large" in str(e).lower()

    def test_process_after_panic_state_is_clean(self):
        """State should be recoverable after any panic scenario."""
        try:
            from minute_bar._order_accel import (
                process_order_batch, rust_reset_state, is_available
            )
        except ImportError:
            return

        # Verify clean state works
        rust_reset_state()
        assert is_available() is True

        result = process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])
        assert isinstance(result, tuple)
        assert result[4] == 0  # no skipped


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
