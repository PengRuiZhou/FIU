"""Integration tests — end-to-end data flow simulation."""
import csv
import json
import os
import time
import threading
import pytest
from unittest.mock import patch

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.code_table import CodeTable
from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig, SessionConfig, LoggingConfig, AggregationConfig, TimezoneConfig
from minute_bar.csv_parser import parse_snapshot_line
from minute_bar.engine import restore_latest_snapshot_from_file
from minute_bar.file_tailer import FileTailer
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import FileState
from minute_bar.writer import write_snapshot_file, write_kline_file


def make_config(tmp_path):
    return AppConfig(
        input=InputConfig(csv_dir=str(tmp_path / "input"), output_dir=str(tmp_path / "output")),
        output=OutputConfig(output_dir=str(tmp_path / "output")),
        recovery=RecoveryConfig(checkpoint_file=str(tmp_path / "checkpoint.json")),
    )


class TestEndToEndPipeline:
    def test_parse_aggregate_output(self, tmp_path):
        """Test the core pipeline: parse CSV → aggregate → write output files."""
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        # Simulate receiving snapshot data
        lines = [
            b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,4450000,0,20260520083000999",
            b"1301,20260520093001200,443500,452000,440000,451000,443500,452000,452000,200,150,67800000,1,,T,0,Y,2,4450000,0,20260520083001200",
            b"1305,20260520093000500,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,4110000,0,20260520083000500",
        ]

        for line in lines:
            parsed = parse_snapshot_line(line)
            assert parsed is not None
            state.process_snapshot(parsed)

        assert state.seqno == 3
        assert len(state.latest_snapshot) == 2

        # Write output files. process_snapshot stored OHLCV under the round-up
        # minute_key: clock-minute 0930 timestamps → bucket 0931.
        minute_key = "202605200931"
        snapshot_copy = dict(state.latest_snapshot)
        ohlcv_data = state.ohlcv_buffers.get(minute_key, {})

        output_dir = str(tmp_path / "output")
        write_snapshot_file(output_dir, minute_key, snapshot_copy, ohlcv_data, code_table, full=True)
        write_kline_file(output_dir, minute_key, snapshot_copy, ohlcv_data, code_table, full=True)

        # Verify snapshot file
        snap_path = os.path.join(output_dir, "snapshot", "2026", "20260520", "snapshot_minute_20260520_0931.csv")
        assert os.path.exists(snap_path)
        with open(snap_path) as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
            assert len(rows) == 2  # 2 symbols

        # Verify kline file
        kline_path = os.path.join(output_dir, "kline", "2026", "20260520", "kline_minute_20260520_0931.csv")
        assert os.path.exists(kline_path)
        with open(kline_path) as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
            assert len(rows) == 2

            # 1301 has OHLCV data
            row_1301 = [r for r in rows if r[1] == "1301"][0]
            assert row_1301[-1] == "Y"  # update_flag
            assert float(row_1301[3]) == 4500.0  # open (first price)
            assert float(row_1301[4]) == 4520.0  # high
            assert float(row_1301[5]) == 4500.0  # low
            assert float(row_1301[6]) == 4520.0  # close (last price)

    def test_carry_forward_symbols(self, tmp_path):
        """Symbols not updated in current minute should still appear with update_flag=N."""
        state = SharedState()
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        # Minute 1: both symbols updated
        state.process_snapshot(parse_snapshot_line(
            b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999"
        ))
        state.process_snapshot(parse_snapshot_line(
            b"1305,20260520093000999,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,0,0,20260520083000999"
        ))

        # Minute 2: only 1301 updated
        state.process_snapshot(parse_snapshot_line(
            b"1301,20260520093100999,443500,451000,440000,451000,443500,451000,451000,50,120,54000000,1,,T,0,Y,2,0,0,20260520083100999"
        ))

        output_dir = str(tmp_path / "output")

        # Output minute 2 with carry-forward for 1305. Round-up: clock-minute
        # 0931 timestamp (093100999) → bucket 0932.
        minute_key = "202605200932"
        ohlcv_data = state.ohlcv_buffers.get(minute_key, {})
        snapshot_copy = dict(state.latest_snapshot)

        write_kline_file(output_dir, minute_key, snapshot_copy, ohlcv_data, code_table, full=True)

        kline_path = os.path.join(output_dir, "kline", "2026", "20260520", "kline_minute_20260520_0932.csv")
        with open(kline_path) as f:
            reader = csv.reader(f)
            next(reader)
            rows = {r[1]: r for r in reader}

            # 1301 was updated
            assert rows["1301"][-1] == "Y"
            assert int(rows["1301"][9]) > 0  # count > 0

            # 1305 was not updated (carry-forward)
            assert rows["1305"][-1] == "N"
            assert rows["1305"][7] == "0"  # volume=0
            assert rows["1305"][9] == "0"  # count=0

    def test_restore_snapshot_from_file(self, tmp_path):
        """Test restoring latest_snapshot from a snapshot_minute file."""
        output_dir = str(tmp_path / "output" / "snapshot" / "2026" / "20260520")
        os.makedirs(output_dir)

        # Write a snapshot file manually (new 24-column format)
        snap_path = os.path.join(output_dir, "snapshot_minute_20260520_0930.csv")
        with open(snap_path, "w", encoding="utf-8") as f:
            f.write("seqno,symbol,name,time,preclose,lastprice,open,high,low,close,lasttradeprice,lasttradeqty,totalvol,totalamount,sessionid,tradetype,status,direction,pflag,decimal,vwap,shortsellflag,rcvtime,update_flag\n")
            f.write("1,1301,,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,,T,0,Y,2,4450000,0,20260520083000999,Y\n")
            f.write("2,1305,,20260520093000999,412000,412300,410000,413000,410000,412300,412300,50,74450,308198040,1,,T,0,Y,2,4110000,0,20260520083000999,Y\n")

        records = restore_latest_snapshot_from_file(str(tmp_path / "output"), "202605200930")
        assert len(records) == 2
        assert "1301" in records
        assert records["1301"].totalvol == 78300
        assert records["1305"].lastprice == 412300.0

    def test_checkpoint_round_trip(self, tmp_path):
        """Test checkpoint write → read → restore state."""
        path = str(tmp_path / "checkpoint.json")
        mgr = CheckpointManager(path, str(tmp_path))

        files = {
            "snapshot": FileState(offset=5000, pending_line=b"", date="20260520"),
            "code": FileState(offset=200, pending_line=b"partial", date="20260520"),
        }

        mgr.write(
            date="20260520", last_seqno=42,
            output_minutes={"202605200930"},
            last_output_minute="202605200930",
            current_minute="202605200931",
            last_output_date="20260520",
            first_data_received=True,
            files=files,
            last_totalvol_by_symbol={"1301": 78300},
            last_totalamount_by_symbol={"1301": 3502510.0},
        )

        data = mgr.read()
        assert data is not None
        assert data["last_seqno"] == 42

        states = mgr.get_file_states(data)
        assert states["snapshot"].offset == 5000
        assert states["code"].pending_line == b"partial"

        assert mgr.get_last_totalvol(data) == {"1301": 78300}
        assert mgr.get_last_totalamount(data) == {"1301": 3502510.0}


class TestFileTailerIntegration:
    def test_incremental_read_with_csv_parser(self, tmp_path):
        """Simulate FIU writing CSV data incrementally."""
        csv_file = tmp_path / "snapshot.csv.20260520"
        csv_file.write_bytes(b"")

        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")

        # First write: 2 complete lines
        with open(csv_file, "ab") as f:
            f.write(b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999\n")
            f.write(b"1305,20260520093000500,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,0,0,20260520083000500\n")

        lines = list(tailer.read_lines())
        assert len(lines) == 2

        # Parse both lines
        for line in lines:
            parsed = parse_snapshot_line(line)
            assert parsed is not None

        # Second write: 1 more line + incomplete
        with open(csv_file, "ab") as f:
            f.write(b"1301,20260520093100999,443500,451000,440000,451000,443500,451000,451000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083100999\n")
            f.write(b"1301,20260520093200")

        lines = list(tailer.read_lines())
        assert len(lines) == 1  # only 1 complete line

        # Complete the partial line
        with open(csv_file, "ab") as f:
            f.write(b"999,443500,452000,440000,452000,443500,452000,452000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083200999\n")

        lines = list(tailer.read_lines())
        assert len(lines) == 1
        parsed = parse_snapshot_line(lines[0])
        assert parsed is not None
        assert parsed.time == 20260520093200999

        tailer.close()
