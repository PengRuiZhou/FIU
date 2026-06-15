"""RSS monitoring test for Phase 21 Rust functions.

Asserts that peak RSS remains below threshold from baseline after loading
Phase 21 Rust extension.
"""
from __future__ import annotations

import os
import sys
import gc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def get_rss_kb() -> int:
    """Get current process RSS in KB (cross-platform)."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:
        pass

    # Windows
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss // 1024
    except ImportError:
        pass

    return 0


class TestPhase21RSSProfile:
    """RSS monitoring assertions for Phase 21 functions."""

    def test_rss_within_threshold(self):
        """Assert peak RSS < 400MB from baseline after Phase 21 load."""
        try:
            import psutil
        except ImportError:
            return  # Skip if psutil not available

        import gc
        gc.collect()

        process = psutil.Process()
        baseline_rss = process.memory_info().rss // 1024  # KB

        # Force Phase 21 import and warmup
        from minute_bar._order_accel import (
            process_order_batch,
            parse_snapshot_batch,
            aggregate_snapshot_batch,
            tickfile_generate,
            rust_reset_state,
            is_available,
        )

        assert is_available(), "Phase 21 Rust should be available"

        # Warmup calls
        process_order_batch([], 'utf-8', '20260528', 20260528, 0, [])
        parse_snapshot_batch([], 'utf-8')
        aggregate_snapshot_batch([], "202605280900", [], [], [])
        tickfile_generate(b'', b'', b'', "202605280900", [], 0)
        rust_reset_state()

        gc.collect()
        peak_rss = process.memory_info().rss // 1024  # KB

        THRESHOLD_KB = 400 * 1024  # 400 MB

        assert peak_rss < THRESHOLD_KB, (
            f"Peak RSS {peak_rss / 1024:.1f} MB exceeds threshold {THRESHOLD_KB / 1024:.0f} MB. "
            f"Baseline was {baseline_rss / 1024:.1f} MB."
        )

    def test_rss_after_batch_processing(self):
        """Assert RSS doesn't grow excessively after processing many batches."""
        try:
            import psutil
        except ImportError:
            return

        gc.collect()
        process = psutil.Process()
        initial_rss = process.memory_info().rss // 1024  # KB

        from minute_bar._order_accel import process_order_batch, rust_reset_state

        rust_reset_state()

        # Process many batches
        lines = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",
            b"7203,20260528090000125,4585000,110,4595000,210,2,0",
            b"7205,20260528090100123,4620000,80,4630000,180,2,0",
        ] * 100  # 400 lines total

        for _ in range(100):
            try:
                process_order_batch(lines, 'utf-8', '20260528', 20260528, 0, [])
            except Exception:
                pass

        gc.collect()
        final_rss = process.memory_info().rss // 1024  # KB

        growth_kb = final_rss - initial_rss
        GROWTH_THRESHOLD_KB = 50 * 1024  # 50 MB

        assert growth_kb < GROWTH_THRESHOLD_KB, (
            f"RSS grew by {growth_kb / 1024:.1f} MB after batch processing. "
            f"Threshold: {GROWTH_THRESHOLD_KB / 1024:.0f} MB. "
            f"Initial: {initial_rss / 1024:.1f} MB, Final: {final_rss / 1024:.1f} MB."
        )

    def test_rss_after_tickfile_generation(self):
        """Assert RSS stable after tickfile_generate calls."""
        try:
            import psutil
        except ImportError:
            return

        gc.collect()
        process = psutil.Process()
        initial_rss = process.memory_info().rss // 1024  # KB

        from minute_bar._order_accel import tickfile_generate

        # Generate many tickfiles
        for _ in range(100):
            try:
                tickfile_generate(b'', b'', b'', "202605280900", ["7203", "7204"], 0)
            except Exception:
                pass

        gc.collect()
        final_rss = process.memory_info().rss // 1024  # KB

        growth_kb = final_rss - initial_rss
        GROWTH_THRESHOLD_KB = 20 * 1024  # 20 MB

        assert growth_kb < GROWTH_THRESHOLD_KB, (
            f"RSS grew by {growth_kb / 1024:.1f} MB after tickfile generation. "
            f"Threshold: {GROWTH_THRESHOLD_KB / 1024:.0f} MB."
        )


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
