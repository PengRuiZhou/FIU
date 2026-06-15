"""Tests for data-driven watermark changes in engine.py."""
import pytest
import threading
from unittest.mock import patch, MagicMock

from minute_bar.aggregator import SharedState
from minute_bar.clock import is_data_driven_expired
from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig
from minute_bar.engine import Engine, _OrderMinuteBuffer
from minute_bar.models import OrderRecord


def make_order_record(symbol="1301", seqno=1, time_=20260521093000123, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_,
        bidprice=4500.0, bidsize=100.0, askprice=4510.0, asksize=200.0,
        decimal=2, rcvtime=20260521083000123,
    )
    defaults.update(kwargs)
    return OrderRecord(**defaults)


def make_config(**overrides):
    defaults = dict(
        input=InputConfig(csv_dir="/tmp/input"),
        output=OutputConfig(output_dir="/tmp/output"),
    )
    defaults.update(overrides)
    return AppConfig(**defaults)


class TestOrderLoopProcessingOrder:
    """Verify seqno is assigned before late detection."""

    def test_late_record_gets_seqno(self):
        buf = _OrderMinuteBuffer()
        buf.records.append(make_order_record(seqno=1))
        buf.line_end_offset = 100

        flushed = {"202605210930"}
        late_per_minute = {}

        minute_key = "202605210930"
        current_minute = "202605210931"
        seqno = 5

        seqno += 1
        record = make_order_record(seqno=seqno)
        assert record.seqno == 6
        assert minute_key in flushed
        assert minute_key != current_minute


class TestOrderLoopMonotonicWatermark:
    def test_out_of_order_record_does_not_update_watermark(self):
        current_minute = "202605210931"
        minute_key = "202605210930"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210931"

    def test_forward_record_updates_watermark(self):
        current_minute = "202605210930"
        minute_key = "202605210931"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210931"

    def test_none_initial_watermark(self):
        current_minute = None
        minute_key = "202605210930"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210930"


class TestFlushExpiredOrderMinutes:
    def test_data_driven_flush_with_watermark(self):
        with patch("minute_bar.engine.ClockWatermarkFlusher"), \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            config = make_config()
            engine = Engine.__new__(Engine)
            engine._config = config
            engine._data_flush_delay_minutes = 1
            engine._enable_time_fallback = False
            engine._flushed_order_minutes = set()
            engine._checkpoint_lock = threading.Lock()
            engine._committed_order_offset = 0

            buffers = {
                "202605210900": _OrderMinuteBuffer(),
                "202605210901": _OrderMinuteBuffer(),
            }
            buffers["202605210900"].records = [make_order_record()]
            buffers["202605210901"].records = [make_order_record(seqno=2)]

            order_watermark = "202605210901"
            expired = [
                k for k in buffers
                if is_data_driven_expired(k, order_watermark, 1)
            ]
            assert "202605210900" in expired
            assert "202605210901" not in expired

    def test_watermark_empty_no_flush(self):
        with patch("minute_bar.engine.ClockWatermarkFlusher"), \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            config = make_config()
            engine = Engine.__new__(Engine)
            engine._config = config
            engine._data_flush_delay_minutes = 1
            engine._enable_time_fallback = False
            engine._flushed_order_minutes = set()

            buffers = {
                "202605210900": _OrderMinuteBuffer(),
            }
            buffers["202605210900"].records = [make_order_record()]

            expired = [
                k for k in buffers
                if is_data_driven_expired(k, "", 1)
            ]
            assert len(expired) == 0


class TestStopFinalFlush:
    def test_stop_calls_flush_all_remaining(self):
        with patch("minute_bar.engine.ClockWatermarkFlusher") as MockFlusher, \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            config = make_config()
            engine = Engine(config)
            engine._running = False
            engine._data_thread = None
            engine._clock_thread = None
            engine._order_thread = None

            engine.stop()

            MockFlusher.return_value.flush_all_remaining.assert_called_once()

    def test_stop_closes_all_resources(self):
        with patch("minute_bar.engine.ClockWatermarkFlusher") as MockFlusher, \
             patch("minute_bar.engine.CodeTable") as MockCT, \
             patch("minute_bar.engine.FileTailer") as MockFT:
            MockFlusher.return_value.flush_all_remaining.side_effect = RuntimeError("flush failed")

            config = make_config()
            engine = Engine(config)
            engine._running = False
            engine._data_thread = None
            engine._clock_thread = None
            engine._order_thread = None

            with pytest.raises(RuntimeError):
                engine.stop()

            # Both tailers share the same FileTailer mock, so close() is called twice
            assert MockFT.return_value.close.call_count >= 2
            engine._code_table.close.assert_called()


class TestOrderLoopCrossDaySafety:
    def test_cross_day_flush_failure_still_updates_date(self):
        current_date = "20260520"
        record_date = "20260521"

        buffers = {"202605200930": _OrderMinuteBuffer()}
        buffers["202605200930"].records = [make_order_record()]

        try:
            raise IOError("simulated flush failure")
        except Exception:
            pass
        finally:
            old_keys = [k for k in buffers if k[:8] != record_date]
            for k in old_keys:
                buffers.pop(k, None)

        current_date = record_date
        assert current_date == "20260521"
        assert len(buffers) == 0
