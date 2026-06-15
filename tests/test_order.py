"""Tests for order processing."""
import csv
import os
import pytest
from minute_bar.config import AppConfig, InputConfig, OutputConfig, AggregationConfig
from minute_bar.csv_parser import parse_order_line, ParsedOrder
from minute_bar.models import OrderRecord
from minute_bar.replay import ReplayEngine
from minute_bar.writer import write_order_file


class TestParseOrderLine:
    def test_minimal_6_cols(self):
        line = b"1301,20260520093000999,450000,100,451000,200"
        result = parse_order_line(line)
        assert result is not None
        assert result.symbol == "1301"
        assert result.time == 20260520093000999
        assert result.bidprice == 450000
        assert result.bidsize == 100
        assert result.askprice == 451000
        assert result.asksize == 200
        assert result.decimal == 2  # default
        assert result.rcvtime == 0  # default

    def test_full_8_cols(self):
        line = b"1301,20260520093000999,450000,100,451000,200,2,20260520083000999"
        result = parse_order_line(line)
        assert result is not None
        assert result.decimal == 2
        assert result.rcvtime == 20260520083000999

    def test_header_skip(self):
        line = b"symbol,time,bidprice,bidsize,askprice,asksize"
        result = parse_order_line(line)
        assert result is None

    def test_too_few_cols(self):
        line = b"1301,20260520093000999,450000"
        result = parse_order_line(line)
        assert result is None

    def test_too_many_cols(self):
        line = b"1301,20260520093000999,450000,100,451000,200,2,20260520083000999,extra"
        result = parse_order_line(line)
        assert result is None

    def test_empty_symbol(self):
        line = b",20260520093000999,450000,100,451000,200"
        result = parse_order_line(line)
        assert result is None


class TestWriteOrderFile:
    def test_write(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
            OrderRecord(symbol="1301", seqno=2, time=20260520093001500,
                        bidprice=450500.0, bidsize=150.0, askprice=450800.0, asksize=180.0,
                        decimal=2, rcvtime=20260520083001500),
        ]
        write_order_file(str(tmp_path), "202605200930", records)

        path = tmp_path / "order" / "2026" / "20260520" / "order_minute_20260520_0930.csv"
        assert path.exists()
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "bidprice" in header
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0][1] == "1301"
            assert rows[0][2] == "20260520093000999"
            assert rows[1][2] == "20260520093001500"

    def test_empty_records(self, tmp_path):
        write_order_file(str(tmp_path), "202605200930", [])
        assert not (tmp_path / "order" / "2026" / "20260520").exists()


class TestReplayWithOrder:
    def test_replay_snapshot_and_order(self, tmp_path):
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        # Write code.csv
        with open(csv_dir / "code.csv.20260520", "wb") as f:
            f.write(b"1301,1,TSE,TestStock,equity,01,0111,0050,JP3257200000,100,514000,373500,2,20260520054625009,20260520,443500\n")

        # Write snapshot.csv
        with open(csv_dir / "snapshot.csv.20260520", "wb") as f:
            f.write(b"symbol,time,preclose,lastprice,open,high,low,close,lasttradeprice,lasttradeqty,totalvol,totalamount,sessionid,tradetype,status,direction,pflag,rcvtime\n")
            f.write(b"1301,20260520093000999,443500,450000,445500,450000,445500,450000,450000,100,3200,1425800000,1,,T,1,Y,2,44556250,48,20260520083000999\n")

        # Write order.csv
        with open(csv_dir / "order.csv.20260520", "wb") as f:
            f.write(b"symbol,time,bidprice,bidsize,askprice,asksize\n")
            f.write(b"1301,20260520093000999,449500,200,450500,300\n")
            f.write(b"1301,20260520093001500,449800,150,450300,250\n")

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        snap_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"
        order_dir = tmp_path / "output" / "order" / "2026" / "20260520"
        assert (snap_dir / "snapshot_minute_20260520_0930.csv").exists()
        assert (order_dir / "order_minute_20260520_0930.csv").exists()

        with open(order_dir / "order_minute_20260520_0930.csv", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0][1] == "1301"
            assert rows[1][1] == "1301"

    def test_replay_no_order_file(self, tmp_path):
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        with open(csv_dir / "code.csv.20260520", "wb") as f:
            f.write(b"1301,1,TSE,TestStock,equity,01,0111,0050,JP3257200000,100,514000,373500,2,20260520054625009,20260520,443500\n")
        with open(csv_dir / "snapshot.csv.20260520", "wb") as f:
            f.write(b"1301,20260520093000999,443500,450000,445500,450000,445500,450000,450000,100,3200,1425800000,1,,T,1,Y,2,44556250,48,20260520083000999\n")

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        snap_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"
        order_dir = tmp_path / "output" / "order" / "2026" / "20260520"
        assert (snap_dir / "snapshot_minute_20260520_0930.csv").exists()
        # No order file when no order.csv exists
        assert not (order_dir / "order_minute_20260520_0930.csv").exists()
