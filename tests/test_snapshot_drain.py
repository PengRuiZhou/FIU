"""Tests for snapshot _data_loop drain loop in engine.py."""
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from minute_bar.config import AppConfig, InputConfig, OutputConfig
from minute_bar.engine import Engine


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

        stop_after = [0]
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [iter([])]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep):
            engine._data_loop()

        assert len(sleep_calls) >= 1
        assert sleep_calls[0] > 0.001  # config interval, not short sleep

    def test_data_available_uses_short_sleep(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["some_snapshot_line"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.return_value = None
            engine._data_loop()

        assert len(sleep_calls) >= 1
        assert sleep_calls[0] == 0.001


class TestDrainLoopContinuousRead:
    """Verify drain loop reads continuously while data is available."""

    def test_drains_multiple_chunks_before_sleep(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["line1"]),
            iter(["line2"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.return_value = None
            engine._data_loop()

        assert mock_parse.call_count == 2


class TestDrainLoopWatermarkUpdate:
    """Verify drain loop correctly updates SharedState via process_snapshot."""

    def test_process_snapshot_called_with_valid_records(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        parsed = MagicMock()
        parsed.time = 20260525113000123

        read_lines_results = [
            iter(["snapshot_line_1"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.return_value = parsed

            with patch.object(engine._state, "process_snapshot") as mock_process:
                engine._data_loop()

            mock_process.assert_called_once_with(parsed)


class TestDrainCountBreak:
    """Verify drain loop breaks at drain_count >= 1000."""

    def test_breaks_at_1000_chunks(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        iterations = [0]

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        single_line_results = [iter(["line"])] * 1001 + [iter([])]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=single_line_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line", return_value=None):
            engine._data_loop()


class TestDrainLoopCodeRefresh:
    """Verify _maybe_refresh_code is called inside drain loop."""

    def test_refresh_called_per_chunk(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["line1"]),
            iter(["line2"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code") as mock_refresh, \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line", return_value=None):
            engine._data_loop()

        assert mock_refresh.call_count == 2


class TestDrainLoopStopResponse:
    """Verify drain loop exits promptly when _running is set to False."""

    def test_exits_on_running_false(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        call_count = [0]

        def mock_sleep(seconds):
            call_count[0] += 1
            engine._running = False

        read_lines_results = [iter([])]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep):
            engine._data_loop()

        assert call_count[0] == 1


class TestDrainLoopLateRecordRouting:
    """Verify late records (after flush) are routed through process_snapshot."""

    def test_late_record_still_goes_to_process_snapshot(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        parsed = MagicMock()
        parsed.time = 20260525112800123

        read_lines_results = [
            iter(["late_snapshot_line"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.return_value = parsed

            with patch.object(engine._state, "process_snapshot") as mock_process:
                engine._data_loop()

            mock_process.assert_called_once_with(parsed)


class TestDrainLoopCrossDateFilter:
    """Verify records from a different date are filtered out."""

    def test_cross_date_record_skipped(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        parsed = MagicMock()
        parsed.time = 20260526112800123  # different date (26th vs 25th)

        read_lines_results = [
            iter(["cross_date_line"]),
            iter([]),
        ]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.return_value = parsed

            with patch.object(engine._state, "process_snapshot") as mock_process:
                engine._data_loop()

            mock_process.assert_not_called()


class TestDrainLoopParseNone:
    """Verify parse_snapshot_line returning None doesn't interrupt drain."""

    def test_none_parse_continues_drain(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 1:
                engine._running = False

        read_lines_results = [
            iter(["bad_line1"]),
            iter(["good_line"]),
            iter([]),
        ]

        parsed = MagicMock()
        parsed.time = 20260525113000123

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line") as mock_parse:
            mock_parse.side_effect = [None, parsed]

            with patch.object(engine._state, "process_snapshot") as mock_process:
                engine._data_loop()

            assert mock_parse.call_count == 2
            mock_process.assert_called_once_with(parsed)


class TestDrainLoopResumesAfterBreak:
    """Verify drain loop correctly resumes after drain_count break."""

    def test_resumes_reading_after_drain_yield(self):
        engine = make_engine()
        engine._running = True
        engine._order_thread_error = None

        outer_iterations = [0]
        stop_after = [0]

        def mock_sleep(seconds):
            stop_after[0] += 1
            if stop_after[0] >= 2:
                engine._running = False

        chunk_results = [iter(["line"])] * 1001
        read_lines_results = chunk_results + [iter([]), iter(["resumed_line"]), iter([])]

        with patch.object(engine._snapshot_tailer, "read_lines", side_effect=read_lines_results), \
             patch.object(engine, "_get_target_date", return_value="20260525"), \
             patch.object(engine, "_maybe_refresh_code"), \
             patch("minute_bar.engine.time.sleep", side_effect=mock_sleep), \
             patch("minute_bar.engine.parse_snapshot_line", return_value=None):
            engine._data_loop()

        assert stop_after[0] == 2
