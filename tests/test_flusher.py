"""Tests for flusher late record handling."""
import csv
import os
import pytest
from unittest.mock import patch

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.code_table import CodeTable
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import FileState, OHLCVAggregate, SnapshotRecord
from minute_bar.writer import write_snapshot_file


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


def make_flusher(state, tmp_path):
    code_table = CodeTable.__new__(CodeTable)
    code_table._table = {}
    checkpoint = CheckpointManager(str(tmp_path / "checkpoint.json"), str(tmp_path))
    return ClockWatermarkFlusher(
        state=state,
        code_table=code_table,
        checkpoint=checkpoint,
        output_dir=str(tmp_path),
        output_delay_sec=60,
        file_states={},
        checkpoint_lock=None,
    )


class TestStep4LateRecords:
    def test_no_late_records_is_noop(self, tmp_path):
        state = SharedState()
        flusher = make_flusher(state, tmp_path)
        flusher._step4_handle_late_records()

    def test_late_records_appended_to_file(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1305", seqno=10, lastprice=4120.0)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        path = os.path.join(str(tmp_path), "snapshot", "2026", "20260520", "snapshot_minute_20260520_0930.csv")
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2

    def test_late_records_update_latest_snapshot(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1301", seqno=10, time_=20260520093100999, lastprice=9999.0)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        assert "1301" in state.latest_snapshot
        assert state.latest_snapshot["1301"].lastprice == 9999.0

    def test_late_records_update_count(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1305", seqno=10)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()

        assert state.late_snapshot_count == 1
        # late_snapshot_minutes re-derives the minute from record.time via
        # time_to_minute_key (round-up): 09:30:00.999 → bucket 0931.
        assert "202605200931" in state.late_snapshot_minutes

    def test_late_records_queue_cleared_after_processing(self, tmp_path):
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        flusher = make_flusher(state, tmp_path)

        rec = make_snapshot(symbol="1301")
        agg = OHLCVAggregate(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        late_rec = make_snapshot(symbol="1305", seqno=10)
        state._late_snapshot_records.append(("202605200930", late_rec))

        flusher._step4_handle_late_records()
        assert len(state._late_snapshot_records) == 0


class TestFlushedSnapshotMinutes:
    @patch("minute_bar.flusher.is_expired", return_value=True)
    def test_step3_records_flushed_minutes(self, mock_expired, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        flusher = make_flusher(state, tmp_path)

        state.ohlcv_buffers["202605200930"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605200930"] = {}
        rec = make_snapshot()
        state.latest_snapshot["1301"] = rec

        flusher._step3_minute_output()

        assert "202605200930" in state.flushed_snapshot_minutes


class TestCrossDayCleanup:
    def test_cross_day_clears_late_stats(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605200930"
        state.flushed_snapshot_minutes.add("202605190930")
        state.late_snapshot_count = 42
        state.late_snapshot_minutes.add("202605190930")

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        assert len(state.flushed_snapshot_minutes) == 0
        assert state.late_snapshot_count == 0
        assert len(state.late_snapshot_minutes) == 0


class TestStep5WriteCheckpoint:
    def test_checkpoint_skipped_with_pending_late_records(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.output_minutes.add("202605200930")
        state._late_snapshot_records.append(("202605200930", make_snapshot()))

        flusher = make_flusher(state, tmp_path)
        flusher._step5_write_checkpoint()

        assert not os.path.exists(str(tmp_path / "checkpoint.json"))

    def test_checkpoint_written_when_no_pending_late_records(self, tmp_path):
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.output_minutes.add("202605200930")
        state.last_output_minute = "202605200930"

        flusher = make_flusher(state, tmp_path)
        flusher._step5_write_checkpoint()

        assert os.path.exists(str(tmp_path / "checkpoint.json"))


def test_flusher_init_runs_recovery_and_populates_skipset(tmp_path):
    """INV-CM-ORDER-1: __init__ recovery replaces eager seqno; populates skip-set; truncates partial."""
    import os
    from minute_bar.aggregator import SharedState
    from minute_bar.tickfile import TICKFILE_HEADER
    from minute_bar.writer import get_tickfile_path
    from tests.test_tickfile_sync import _make_flusher

    state = SharedState()
    state.first_data_received = True
    date = "20260602"  # matches the jst_now patch inside _make_flusher
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ("a" * 60) + "\n"
    with open(tf, "wb") as f:
        f.write(committed.encode() + b"PARTIAL_TAIL")
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{len(committed.encode())},1,9\n")

    flusher = _make_flusher(state, tmp_path, enable_tickfile=True)
    assert os.path.getsize(tf) == len(committed.encode())  # truncated
    assert f"{date}0931" in state._generated_tickfile_minutes
    assert state._tickfile_seqno == 9
