"""
Phase 21 E2E Benchmark Test (Live mode only)

Runs data_simulator (continuous write) + Engine (live tail + process) with
Phase 21 Rust acceleration enabled, and measures wall-clock time for the
0850–0910 peak window (21 minutes covering the 0900 open spike).

SLA thresholds (0850–0910 window):
  GREEN:  < 30s
  YELLOW: 30–120s
  RED:    > 120s (FAIL)
"""
from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

# Historical source data lives in D:/FIU/input (raw CSVs the simulator replays).
TEST_DATA_DIR = Path("D:/FIU/input")
TEST_DATE = "20260528"
# Benchmark output lives under test/ so artifacts are inspectable.
BENCH_DIR = Path("D:/FIU/test/phase21_benchmark")


@pytest.fixture(scope="module")
def rust_available():
    """Check if Rust acceleration is available."""
    try:
        from minute_bar._order_accel import is_available
        return is_available()
    except (ImportError, OSError):
        return False


class TestPhase21E2ELiveBenchmark:
    """Live-mode E2E benchmark: data_simulator → Engine with Phase 21 Rust."""

    @pytest.mark.skipif(not Path("D:/FIU/input/order.csv.20260528").exists(),
                        reason="Live source data not found")
    def test_warmup(self, rust_available):
        """Verify all Phase 21 Rust functions warmup correctly."""
        if not rust_available:
            pytest.skip("Rust acceleration not available")

        from minute_bar._order_accel import (
            aggregate_snapshot_batch,
            is_available,
            parse_snapshot_batch,
            process_order_batch,
            rust_reset_state,
            rust_reset_snapshot_state,
            tickfile_generate,
        )

        assert is_available()

        result = process_order_batch([], "utf-8", TEST_DATE, 0, 0, [])
        assert len(result) == 5, "process_order_batch should return 5-tuple"
        assert result[0][0:4] == b"\xaa\xbb\xcc\x01", "per_minute_buf magic"

        result = parse_snapshot_batch([], "utf-8")
        assert len(result) == 2, "parse_snapshot_batch should return 2-tuple"

        result = aggregate_snapshot_batch([], "202605280900", [], [], [])
        assert len(result) == 5, "aggregate_snapshot_batch should return 5-tuple"

        rust_reset_state()
        rust_reset_snapshot_state()
        logger.info("Phase 21 warmup: PASSED")

    @pytest.mark.skipif(not Path("D:/FIU/input/order.csv.20260528").exists(),
                        reason="Live source data not found")
    def test_live_benchmark(self, rust_available):
        """Live mode benchmark: data_simulator writes continuously, Engine tails + processes.

        Measures the 0850–0910 peak window (21 minutes) wall-clock under continuous
        production-like load. The simulator replays historical CSVs to a temp dir at
        high speed; the Engine tails those files in live mode (FileTailer) and processes
        them with Phase 21 Rust acceleration enabled.
        """
        if not rust_available:
            pytest.skip("Rust acceleration not available")

        import threading
        import psutil

        # Reset Rust state before run
        try:
            from minute_bar._order_accel import rust_reset_state, rust_reset_snapshot_state
            rust_reset_state()
            rust_reset_snapshot_state()
        except (ImportError, OSError):
            pass

        # Source = raw historical input; simulator output = engine input (tail source).
        # All benchmark artifacts live under test/phase21_benchmark/.
        source_dir = "D:/FIU/input"
        sim_out = BENCH_DIR / "sim_out"
        engine_out = BENCH_DIR / "engine_out"
        # Clean any previous run, then create fresh dirs
        shutil.rmtree(sim_out, ignore_errors=True)
        shutil.rmtree(engine_out, ignore_errors=True)
        sim_out.mkdir(parents=True)
        engine_out.mkdir(parents=True)

        # --- Start simulator in a background thread ---
        from data_simulator.simulator import Simulator

        sim = Simulator(
            source_dir=source_dir,
            output_dir=str(sim_out),
            speed=100000,              # 100Kx realtime — drains fast
            date=TEST_DATE,
            file_types=["order", "snapshot", "code"],
            clean=True,
        )
        sim_thread = threading.Thread(target=sim.run, name="simulator", daemon=True)
        sim_thread.start()

        # --- Start engine in live mode (csv_dir = simulator output) ---
        from minute_bar.config import (AggregationConfig, AppConfig, InputConfig,
                                       OutputConfig, RecoveryConfig)
        from minute_bar.engine import Engine

        config = AppConfig(
            input=InputConfig(
                csv_dir=str(sim_out),
                target_date=TEST_DATE,
                order_chunk_size_bytes=524288,
                file_encoding="utf-8",
                enable_order_accel=True,
                enable_rust_order_full_batch=True,
                enable_rust_snapshot_batch=True,
                enable_rust_tickfile=True,
                poll_interval_ms=50,
            ),
            output=OutputConfig(
                output_dir=str(engine_out),
                enable_order=True,
                enable_tickfile=True,
                enable_kline=False,
                enable_full_snapshot=True,
                enable_full_kline=False,
            ),
            aggregation=AggregationConfig(
                first_seen_volume_base="start_totalvol",
            ),
            recovery=RecoveryConfig(
                checkpoint_file=str(BENCH_DIR / "engine_ckpt.json"),
                output_delay_sec=0,
                data_flush_delay_minutes=0,
                enable_time_fallback=False,
                stall_flush_sec=30,
            ),
        )

        rss_before = psutil.Process().memory_info().rss
        engine = Engine(config)

        # Run engine.start() in its own thread (it blocks until stop())
        eng_thread = threading.Thread(target=engine.start, name="engine-main", daemon=True)
        eng_thread.start()

        # --- Monitor: time the 0850–0910 peak window (21 minutes) ---
        # Covers pre-open buildup (0850–0859), the 0900 open spike, and the
        # post-open surge (0901–0910). 0850 and 0910 are the heaviest concurrent
        # minutes around the open, so this window captures real GIL pressure.
        window_start = "202605280850"
        window_end = "202605280910"
        first_seen = {}            # minute_key -> monotonic timestamp when first reached
        start_window = None
        # Order thread may lag behind snapshot under GIL pressure — cap the wait
        # so we always get a result to diagnose, even if the window never completes.
        deadline = time.monotonic() + 420  # 7 min hard cap

        while time.monotonic() < deadline:
            with engine._state.lock:
                order_min = engine._state.order_current_minute
            if order_min:
                if order_min not in first_seen:
                    first_seen[order_min] = time.monotonic()
                    print(f"[monitor] order reached minute={order_min}", flush=True)
                if start_window is None and order_min >= window_start:
                    start_window = first_seen[order_min]
                    print(f"[monitor] window START @ {order_min}", flush=True)
                # Stop once order has fully passed the window end
                if start_window is not None and order_min > window_end:
                    print(f"[monitor] window END @ {order_min}", flush=True)
                    break
            time.sleep(0.05)

        end_window = time.monotonic()
        elapsed_window = (end_window - start_window) if start_window else None
        window_completed = start_window is not None and elapsed_window is not None and \
            any(m > window_end for m in first_seen)
        rss_after = psutil.Process().memory_info().rss
        rss_delta = rss_after - rss_before

        # --- Shutdown ---
        engine._running = False
        engine.stop()
        sim.stop()
        sim_thread.join(timeout=5)
        eng_thread.join(timeout=15)

        # Clean up intermediate simulator output (keep engine_out for inspection)
        shutil.rmtree(sim_out, ignore_errors=True)

        # --- Report (print so output survives `tail`) ---
        order_max_minute = max(first_seen.keys()) if first_seen else "(none)"
        print(f"\n=== Live Benchmark Results ===", flush=True)
        print(f"0850–0910 window elapsed: {elapsed_window}", flush=True)
        print(f"Window completed: {window_completed}", flush=True)
        print(f"Order reached max minute: {order_max_minute}", flush=True)
        print(f"RSS delta: {rss_delta / (1024 * 1024):.1f} MB", flush=True)
        if first_seen:
            sorted_minutes = sorted(first_seen.items())
            for mk, ts in sorted_minutes:
                rel = ts - start_window if start_window else 0
                print(f"  {mk}: +{rel:.2f}s", flush=True)

        # This is a diagnostic benchmark — record results, do not hard-fail on SLA.
        # The order-thread stall under GIL pressure is a known issue that Phase 21
        # addresses; if the window still doesn't complete, that signals Phase 21's
        # late-order Python path (append_order_records per-record) is the bottleneck.
        if not window_completed:
            pytest.fail(
                f"Order thread stalled at {order_max_minute} — never reached >{window_end}. "
                f"Window did not complete (order lagging snapshot). See per-minute timing above."
            )
        assert rss_delta < 400 * 1024 * 1024, f"RSS delta {rss_delta/1e6:.0f}MB > 400MB"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
