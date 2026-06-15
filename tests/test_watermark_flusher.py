"""Tests for data-driven watermark flush logic in flusher.py."""
import pytest
from unittest.mock import patch

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.code_table import CodeTable
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import OHLCVAggregate, SnapshotRecord


def make_snapshot(symbol="1301", seqno=1, lastprice=4500.0, time_=20260520093000999, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_, rcvtime=20260520083000999,
        preclose=4435.0, lastprice=lastprice, open=4400.0, high=4510.0, low=4435.0,
        close=4500.0, lasttradeprice=4500.0, lasttradeqty=100, totalvol=78300,
        totalamount=3502510.0, sessionid=1, tradetype="", status="T",
        direction=0, pflag="Y", decimal=2, vwap=4450.0, shortsellflag=0,
    )
    defaults.update(kwargs)
    return SnapshotRecord(**defaults)


def make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=True, output_delay_sec=60, stall_flush_sec=300):
    code_table = CodeTable.__new__(CodeTable)
    code_table._table = {}
    checkpoint = CheckpointManager(str(tmp_path / "checkpoint.json"), str(tmp_path))
    return ClockWatermarkFlusher(
        state=state,
        code_table=code_table,
        checkpoint=checkpoint,
        output_dir=str(tmp_path),
        output_delay_sec=output_delay_sec,
        enable_full_snapshot=True,
        enable_full_kline=True,
        enable_kline=True,
        enable_order=True,
        file_states={},
        checkpoint_lock=None,
        data_flush_delay_minutes=data_flush_delay_minutes,
        enable_time_fallback=enable_time_fallback,
        stall_flush_sec=stall_flush_sec,
    )


class TestStep3DataDrivenFlush:
    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_data_driven_flush_triggers(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210901"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        assert "202605210900" in state.flushed_snapshot_minutes
        assert "202605210900" not in state.ohlcv_buffers

    @patch("minute_bar.flusher.is_expired", return_value=True)
    def test_fallback_flush_when_watermark_stalled(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=True)
        flusher._step3_minute_output()

        assert "202605210900" in state.flushed_snapshot_minutes

    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_no_flush_when_watermark_behind(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        assert "202605210900" not in state.flushed_snapshot_minutes

    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_no_fallback_when_disabled(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        mock_expired.assert_not_called()


class TestFlushMinutesInternal:
    def test_defensive_pop_missing_key(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        flusher = make_flusher(state, tmp_path)
        flusher._flush_minutes_internal(["202605210999"], is_final=False)

    def test_is_final_continue_on_failure(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.ohlcv_buffers["202605210901"] = {"1302": OHLCVAggregate(symbol="1302")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.raw_snapshot_buffers["202605210901"] = {}
        state.latest_snapshot["1301"] = make_snapshot()
        state.latest_snapshot["1302"] = make_snapshot(symbol="1302")

        flusher = make_flusher(state, tmp_path)
        call_count = 0

        def failing_write(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise IOError("simulated failure")

        flusher._write_minute_files = failing_write
        with pytest.raises(RuntimeError, match="Final flush failed"):
            flusher._flush_minutes_internal(["202605210900", "202605210901"], is_final=True)
        assert call_count == 2


class TestFlushAllRemaining:
    def test_flushes_all_remaining_buffers(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.ohlcv_buffers["202605210901"] = {"1302": OHLCVAggregate(symbol="1302")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.raw_snapshot_buffers["202605210901"] = {}
        state.latest_snapshot["1301"] = make_snapshot()
        state.latest_snapshot["1302"] = make_snapshot(symbol="1302")

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()

        assert len(state.ohlcv_buffers) == 0
        assert "202605210900" in state.flushed_snapshot_minutes
        assert "202605210901" in state.flushed_snapshot_minutes

    def test_writes_checkpoint_after_flush(self, tmp_path):
        import os
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()

        assert os.path.exists(str(tmp_path / "checkpoint.json"))

    def test_empty_buffers_is_noop(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()


class TestCrossDayWatermarkReset:
    def test_cross_day_keeps_zero_base_watermark(self, tmp_path):
        """Watermark at exactly zero base stays after cross-day."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605200000"

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        # Zero base is not < zero base, so no overwrite needed, value stays
        assert state.current_minute == "202605200000"

    def test_cross_day_does_not_overwrite_advanced_watermark(self, tmp_path):
        """If data-thread already pushed watermark past zero, don't overwrite."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605220900"

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        assert state.current_minute == "202605220900"


class TestStallDetection:
    def test_stall_warning_triggered(self, tmp_path):
        import time as _time
        from minute_bar import flusher as flusher_mod

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}

        flusher = make_flusher(state, tmp_path, enable_time_fallback=False, stall_flush_sec=0.1)
        flusher._step3_minute_output()
        _time.sleep(0.15)

        with patch.object(flusher_mod.logger, "error") as mock_error:
            flusher._step3_minute_output()
            stall_calls = [c for c in mock_error.call_args_list if "stalled" in str(c).lower()]
            assert len(stall_calls) > 0

    def test_stall_counter_resets_on_progress(self, tmp_path):
        import time as _time
        from minute_bar import flusher as flusher_mod

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        flusher = make_flusher(state, tmp_path, enable_time_fallback=False, stall_flush_sec=1.0)
        flusher._step3_minute_output()

        state.current_minute = "202605210901"
        flusher._step3_minute_output()
        assert not flusher._stall_warned

    def test_stall_triggers_ohlcv_flush(self, tmp_path):
        """Stall-triggered flush flushes all remaining ohlcv buffers."""
        import time as _time

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211530"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211530"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, enable_time_fallback=False, stall_flush_sec=0.1)
        flusher._step3_minute_output()

        assert "202605211530" in state.ohlcv_buffers  # not expired yet
        _time.sleep(0.15)
        flusher._step3_minute_output()

        assert "202605211530" not in state.ohlcv_buffers  # stall flush cleared it
        assert "202605211530" in state.flushed_snapshot_minutes

    def test_stall_not_triggered_below_threshold(self, tmp_path):
        """Stall flush does NOT trigger when stalled < stall_flush_sec."""
        import time as _time

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211530"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211530"] = {}

        flusher = make_flusher(state, tmp_path, enable_time_fallback=False, stall_flush_sec=10.0)
        flusher._step3_minute_output()
        _time.sleep(0.1)
        flusher._step3_minute_output()

        assert "202605211530" in state.ohlcv_buffers  # still there

    def test_stall_flush_recovery(self, tmp_path):
        """After stall flush, new data is processed normally."""
        import time as _time

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211530"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211530"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, enable_time_fallback=False, stall_flush_sec=0.1)
        flusher._step3_minute_output()
        _time.sleep(0.15)
        flusher._step3_minute_output()
        assert "202605211530" not in state.ohlcv_buffers

        # New data arrives — watermark advances
        state.current_minute = "202605211531"
        state.ohlcv_buffers["202605211531"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211531"] = {}
        flusher._step3_minute_output()
        assert not flusher._stall_warned
        # 1531 can be flushed by data-driven since watermark is now 1531 + delay
        # The stall flag resets on watermark advance


class TestRerouteBufferToLateQueue:
    def test_reroutes_raw_snapshot_to_late_queue(self, tmp_path):
        """Raw snapshot records are moved to _late_snapshot_records."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        rec = make_snapshot(symbol="1301")
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [rec]}
        state._snapshot_at_minute_end["202605211524"] = {"1301": rec}

        flusher = make_flusher(state, tmp_path)
        flusher._reroute_buffer_to_late_queue(["202605211524"])

        assert "202605211524" not in state.ohlcv_buffers
        assert "202605211524" not in state.raw_snapshot_buffers
        assert "202605211524" not in state._snapshot_at_minute_end
        assert len(state._late_snapshot_records) == 1
        assert state._late_snapshot_records[0] == ("202605211524", rec)

    def test_drops_ohlcv_data(self, tmp_path):
        """OHLCV aggregated data is dropped (cannot reconstruct individual records)."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}

        flusher = make_flusher(state, tmp_path)
        flusher._reroute_buffer_to_late_queue(["202605211524"])

        assert "202605211524" not in state.ohlcv_buffers
        assert len(state._late_snapshot_records) == 0

    def test_drops_excess_when_queue_full(self, tmp_path):
        """Records beyond MAX_LATE_SNAPSHOT_QUEUE_SIZE are dropped."""
        from minute_bar.aggregator import MAX_LATE_SNAPSHOT_QUEUE_SIZE

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        # Fill late queue to capacity
        for i in range(MAX_LATE_SNAPSHOT_QUEUE_SIZE):
            state._late_snapshot_records.append(
                ("202605210900", make_snapshot(symbol=f"S{i:05d}"))
            )

        # Add buffer data for an already-flushed minute
        extra_records = [
            make_snapshot(symbol="1301"),
            make_snapshot(symbol="1302"),
        ]
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {
            "1301": [extra_records[0]],
            "1302": [extra_records[1]],
        }

        flusher = make_flusher(state, tmp_path)
        flusher._reroute_buffer_to_late_queue(["202605211524"])

        # Queue should not exceed max size
        assert len(state._late_snapshot_records) == MAX_LATE_SNAPSHOT_QUEUE_SIZE


class TestStep3AlreadyFlushedSplit:
    def test_already_flushed_not_flushed_again(self, tmp_path):
        """Already-flushed minutes are NOT passed to _flush_minutes_internal."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {}
        state.flushed_snapshot_minutes.add("202605211524")
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        call_log = []
        flusher._flush_minutes_internal = lambda keys, **kw: call_log.append(keys)

        flusher._step3_minute_output()

        # 1524 was already flushed, should NOT be in _flush_minutes_internal call
        assert all("202605211524" not in call for call in call_log)

    def test_already_flushed_rerouted_to_late_queue(self, tmp_path):
        """Already-flushed minutes are rerouted via _reroute_buffer_to_late_queue."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [make_snapshot()]}
        state.flushed_snapshot_minutes.add("202605211524")
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        rerouted_keys = []
        flusher._reroute_buffer_to_late_queue = lambda keys: rerouted_keys.extend(keys)
        flusher._flush_minutes_internal = lambda keys, **kw: None

        flusher._step3_minute_output()

        assert "202605211524" in rerouted_keys

    def test_normal_keys_still_flushed(self, tmp_path):
        """Unflushed expired minutes are still flushed normally."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)

        flusher._step3_minute_output()

        assert "202605211524" in state.flushed_snapshot_minutes


class TestRerouteStep4Integration:
    def test_rerouted_records_appended_by_step4(self, tmp_path):
        """Re-routed records are appended to file by step 4."""
        import csv

        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"

        rec = make_snapshot(symbol="1301")
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [rec]}
        state.flushed_snapshot_minutes.add("202605211524")
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)

        # First: write initial file so step4 can append to it
        flusher._flush_minutes_internal(["202605211524"], is_final=False)
        # Now simulate race: buffer gets new data during write window
        rec2 = make_snapshot(symbol="1301", seqno=999)
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [rec2]}

        # Step 3: re-route to late queue
        flusher._step3_minute_output()
        assert len(state._late_snapshot_records) >= 1

        # Step 4: process late records
        flusher._step4_handle_late_records()
        assert len(state._late_snapshot_records) == 0

    def test_reroute_then_checkpoint_excludes_buffer(self, tmp_path):
        """After re-route, checkpoint does not contain the rerouted buffer data."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"

        rec = make_snapshot(symbol="1301")
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [rec]}
        state.flushed_snapshot_minutes.add("202605211524")
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._flush_minutes_internal = lambda keys, **kw: None

        flusher._step3_minute_output()

        assert "202605211524" not in state.ohlcv_buffers
        assert "202605211524" not in state.raw_snapshot_buffers


class TestConcurrentRerouteAndLateQueue:
    def test_reroute_plus_existing_late_records(self, tmp_path):
        """Re-routed records + pre-existing late records are both handled by step 4."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605211530"

        # Pre-existing late records (different symbol)
        existing_late = make_snapshot(symbol="9999", time_=20260521152400999)
        state._late_snapshot_records.append(("202605211524", existing_late))

        # Race window data to be re-routed
        race_rec = make_snapshot(symbol="1301", time_=20260521152400888)
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [race_rec]}
        state.flushed_snapshot_minutes.add("202605211524")
        state.latest_snapshot["1301"] = make_snapshot()
        state.latest_snapshot["9999"] = existing_late

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)

        # Write initial file
        flusher._flush_minutes_internal(["202605211524"], is_final=False)

        # Re-populate buffer (simulating race window)
        race_rec2 = make_snapshot(symbol="1301", time_=20260521152400888)
        state.ohlcv_buffers["202605211524"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605211524"] = {"1301": [race_rec2]}

        # Step 3: re-route (1 record)
        flusher._step3_minute_output()

        # Verify late queue has both: pre-existing + re-routed
        assert len(state._late_snapshot_records) == 2

        # Step 4: process all
        flusher._step4_handle_late_records()
        assert len(state._late_snapshot_records) == 0
        assert state.late_snapshot_count == 2


class TestFourWaySplit:
    def test_data_driven_and_fallback_split_with_already_flushed(self, tmp_path):
        """Four-way split: data-driven×normal, data-driven×reflush, fallback×normal, fallback×reflush."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210903"

        # data-driven expired (watermark=0903, delay=1, so <=0901 is expired)
        state.ohlcv_buffers["202605210900"] = {"S1": OHLCVAggregate(symbol="S1")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.ohlcv_buffers["202605210901"] = {"S2": OHLCVAggregate(symbol="S2")}
        state.raw_snapshot_buffers["202605210901"] = {}

        # data-driven expired AND already flushed
        state.flushed_snapshot_minutes.add("202605210901")

        state.latest_snapshot["S1"] = make_snapshot(symbol="S1")
        state.latest_snapshot["S2"] = make_snapshot(symbol="S2")

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)

        flushed_keys = []
        rerouted_keys = []
        flusher._flush_minutes_internal = lambda keys, **kw: flushed_keys.extend(keys)
        flusher._reroute_buffer_to_late_queue = lambda keys: rerouted_keys.extend(keys)

        flusher._step3_minute_output()

        # 0900: normal, not already flushed → _flush_minutes_internal
        assert "202605210900" in flushed_keys
        # 0901: already flushed → _reroute_buffer_to_late_queue
        assert "202605210901" in rerouted_keys
        # 0900 should NOT be in rerouted
        assert "202605210900" not in rerouted_keys
        # 0901 should NOT be in flushed
        assert "202605210901" not in flushed_keys
