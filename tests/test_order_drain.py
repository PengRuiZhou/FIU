"""Tests for order drain loop in engine.py."""
import threading
import time
from unittest.mock import MagicMock, patch, call

from minute_bar.models import OrderRecord

import pytest

from minute_bar.config import AppConfig, InputConfig, OutputConfig
from minute_bar.engine import Engine, _OrderMinuteBuffer
from minute_bar.models import OrderRecord


def make_config(**overrides):
    defaults = dict(
        input=InputConfig(csv_dir="/tmp/input"),
        output=OutputConfig(output_dir="/tmp/output"),
    )
    defaults.update(overrides)
    return AppConfig(**defaults)


def make_engine(config=None):
    with patch("minute_bar.engine.ClockWatermarkFlusher"), \
         patch("minute_bar.engine.CodeTable"), \
         patch("minute_bar.engine.FileTailer"), \
         patch("minute_bar.engine.CheckpointManager"):
        if config is None:
            config = make_config()
        engine = Engine(config)
        return engine


class TestDrainLoopSleepBehavior:
    """Verify drain loop uses config interval when no data, short sleep when data available."""

    def test_no_data_uses_config_interval(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None
        engine._flushed_order_minutes = set()
        engine._late_order_count = 0
        engine._late_order_minutes = set()

        stop_after = [0]

        original_sleep = time.sleep
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter([]),
        ]

        with patch.object(engine._order_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine._order_tailer, "line_offset", 0), \
             patch.object(engine, "_get_target_date", return_value="20260521"), \
             patch.object(engine, "_flush_expired_order_minutes"), \
             patch.object(engine, "_enforce_max_pending"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.time.monotonic", return_value=100.0):
            engine._order_loop()

        assert len(sleep_calls) >= 1
        assert sleep_calls[0] == 0.2

    def test_data_available_uses_short_sleep(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None
        engine._flushed_order_minutes = set()
        engine._late_order_count = 0
        engine._late_order_minutes = set()

        stop_after = [0]

        original_sleep = time.sleep
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["1301,20260521093000123,4500.0,100.0,4510.0,200.0,2,20260521083000123"]),
            iter([]),
        ]

        with patch.object(engine._order_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine._order_tailer, "line_offset", 100), \
             patch.object(engine, "_get_target_date", return_value="20260521"), \
             patch.object(engine, "_flush_expired_order_minutes"), \
             patch.object(engine, "_enforce_max_pending"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.time.monotonic", return_value=100.0), \
             patch("minute_bar.engine.parse_order_record") as mock_parse:
            mock_record = OrderRecord(symbol="7203", seqno=1, time=20260521093000123,
                                      bidprice=100, bidsize=10, askprice=101, asksize=20,
                                      decimal=2, rcvtime=20260521093000100)
            mock_parse.return_value = mock_record

            engine._order_loop()

        assert len(sleep_calls) >= 1
        assert sleep_calls[0] == 0.001


class TestDrainLoopContinuousRead:
    """Verify drain loop reads continuously while data is available."""

    def test_drains_multiple_chunks_before_sleep(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None
        engine._flushed_order_minutes = set()
        engine._late_order_count = 0
        engine._late_order_minutes = set()

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["1301,20260521093000123,4500.0,100.0,4510.0,200.0,2,20260521083000123"]),
            iter(["1301,20260521093000456,4500.0,100.0,4510.0,200.0,2,20260521083000456"]),
            iter([]),
        ]

        with patch.object(engine._order_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine._order_tailer, "line_offset", 100), \
             patch.object(engine, "_get_target_date", return_value="20260521"), \
             patch.object(engine, "_flush_expired_order_minutes"), \
             patch.object(engine, "_enforce_max_pending"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.time.monotonic", return_value=100.0), \
             patch("minute_bar.engine.parse_order_record") as mock_parse:
            mock_record = OrderRecord(symbol="7203", seqno=1, time=20260521093000123,
                                      bidprice=100, bidsize=10, askprice=101, asksize=20,
                                      decimal=2, rcvtime=20260521093000100)
            mock_parse.return_value = mock_record

            engine._order_loop()

        assert mock_parse.call_count == 2


class TestDrainLoopPeriodicSafetyCheck:
    """Verify drain loop calls safety checks every 100 iterations."""

    def test_flush_called_at_100th_iteration(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None
        engine._flushed_order_minutes = set()
        engine._late_order_count = 0
        engine._late_order_minutes = set()

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        single_line = "1301,20260521093000123,4500.0,100.0,4510.0,200.0,2,20260521083000123"
        read_lines_results = [iter([single_line])] * 101 + [iter([])]

        flush_expired_calls = [0]

        def mock_flush_expired(*args, **kwargs):
            flush_expired_calls[0] += 1

        with patch.object(engine._order_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine._order_tailer, "line_offset", 100), \
             patch.object(engine, "_get_target_date", return_value="20260521"), \
             patch.object(engine, "_flush_expired_order_minutes", side_effect=mock_flush_expired), \
             patch.object(engine, "_enforce_max_pending") as mock_enforce, \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.time.monotonic", return_value=100.0), \
             patch("minute_bar.engine.parse_order_record") as mock_parse:
            mock_record = OrderRecord(symbol="7203", seqno=1, time=20260521093000123,
                                      bidprice=100, bidsize=10, askprice=101, asksize=20,
                                      decimal=2, rcvtime=20260521093000100)
            mock_parse.return_value = mock_record

            engine._order_loop()

        assert flush_expired_calls[0] >= 1


class TestDrainLoopRecordDrivenFlush:
    """Verify record-driven flush triggers on minute boundary changes within drain loop."""

    def test_minute_advance_triggers_flush(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None
        engine._flushed_order_minutes = set()
        engine._late_order_count = 0
        engine._late_order_minutes = set()

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        line_0930 = "1301,20260521093000123,4500.0,100.0,4510.0,200.0,2,20260521083000123"
        line_0931 = "1301,20260521093100123,4500.0,100.0,4510.0,200.0,2,20260521083000123"
        read_lines_results = [
            iter([line_0930]),
            iter([line_0931]),
            iter([]),
        ]

        with patch.object(engine._order_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine._order_tailer, "line_offset", 100), \
             patch.object(engine, "_get_target_date", return_value="20260521"), \
             patch.object(engine, "_flush_expired_order_minutes"), \
             patch.object(engine, "_enforce_max_pending"), \
             patch.object(engine, "_flush_order_minute") as mock_flush_minute, \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.time.monotonic", return_value=100.0), \
             patch("minute_bar.engine.parse_order_record") as mock_parse:
            times = [20260521093000123, 20260521093100123]
            mock_parse.side_effect = [
                OrderRecord(symbol="7203", seqno=i+1, time=t,
                            bidprice=100, bidsize=10, askprice=101, asksize=20,
                            decimal=2, rcvtime=t)
                for i, t in enumerate(times)
            ]

            engine._order_loop()

        assert mock_flush_minute.call_count >= 1
        minute_keys = [c[0][1] for c in mock_flush_minute.call_args_list]
        assert "202605210930" in minute_keys
