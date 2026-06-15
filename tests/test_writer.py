"""Tests for writer module."""
import csv
import os
import pytest
import threading
from minute_bar.writer import (
    append_order_records,
    append_snapshot_records,
    atomic_write,
    compute_trade_flag,
    get_order_file_path,
    get_snapshot_file_path,
    get_tickfile_path,
    _get_write_lock,
    _minute_end_threshold,
    write_snapshot_file,
    write_kline_file,
    write_order_file,
    write_tickfile_rows,
    recover_tickfile_seqno,
)
from minute_bar.tickfile import TICKFILE_HEADER
from minute_bar.models import OHLCVAggregate, OrderRecord, SnapshotRecord
from minute_bar.code_table import CodeTable


def make_snapshot(symbol="1301", seqno=1, lastprice=4500.0, decimal=2, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=20260520093000999, rcvtime=20260520083000999,
        preclose=4435.0, lastprice=lastprice, open=4400.0, high=4510.0, low=4435.0,
        close=4500.0, lasttradeprice=4500.0, lasttradeqty=100, totalvol=78300,
        totalamount=3502510.0, sessionid=1, tradetype="", status="T",
        direction=0, pflag="Y", decimal=decimal, vwap=4450.0, shortsellflag=0,
    )
    defaults.update(kwargs)
    return SnapshotRecord(**defaults)


def make_agg(symbol="1301", **kwargs):
    defaults = dict(
        symbol=symbol, open=4500.0, high=4520.0, low=4480.0, close=4510.0,
        volume=500, amount=2250000.0, count=10,
        start_totalvol=77800, start_totalamount=3500000.0,
        end_totalvol=78300, end_totalamount=3502510.0,
        any_lasttradeqty_positive=True, seqno=1, decimal=2,
    )
    defaults.update(kwargs)
    return OHLCVAggregate(**defaults)


class TestAtomicWrite:
    def test_write_and_read(self, tmp_path):
        path = str(tmp_path / "test.csv")
        atomic_write(path, "hello\nworld\n")
        with open(path) as f:
            assert f.read() == "hello\nworld\n"

    def test_overwrites_existing(self, tmp_path):
        path = str(tmp_path / "test.csv")
        atomic_write(path, "first")
        atomic_write(path, "second")
        with open(path) as f:
            assert f.read() == "second"

    def test_creates_parent_dirs(self, tmp_path):
        path = str(tmp_path / "sub" / "dir" / "test.csv")
        atomic_write(path, "deep")
        assert os.path.exists(path)

    def test_no_tmp_file_left(self, tmp_path):
        path = str(tmp_path / "test.csv")
        atomic_write(path, "content")
        assert not os.path.exists(path + ".tmp")


class TestTradeFlag:
    def test_with_volume(self):
        agg = make_agg(volume=100)
        assert compute_trade_flag(agg) == "Y"

    def test_with_lasttradeqty(self):
        agg = make_agg(volume=0, any_lasttradeqty_positive=True)
        assert compute_trade_flag(agg) == "Y"

    def test_no_trade(self):
        agg = make_agg(volume=0, any_lasttradeqty_positive=False)
        assert compute_trade_flag(agg) == "N"


class TestWriteSnapshotFile:
    def test_full_snapshot(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        path = tmp_path / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        assert path.exists()
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "update_flag" in header
            row = next(reader)
            assert row[1] == "1301"
            assert row[-1] == "Y"  # update_flag

    def test_carry_forward(self, tmp_path):
        rec = make_snapshot(symbol="1305")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1305": rec}, {}, code_table, full=True,
        )

        path = tmp_path / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            row = next(reader)
            assert row[1] == "1305"
            assert row[-1] == "N"  # update_flag=N


class TestWriteKlineFile:
    def test_with_ohlcv(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_kline_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        path = tmp_path / "kline" / "2026" / "20260520" / "kline_minute_20260520_0930.csv"
        assert path.exists()
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "trade_flag" in header
            row = next(reader)
            assert row[1] == "1301"
            assert row[-1] == "Y"  # update_flag
            assert row[-2] == "Y"  # trade_flag

    def test_carry_forward_kline(self, tmp_path):
        rec = make_snapshot(symbol="1305", lastprice=4123.0)
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_kline_file(
            str(tmp_path), "202605200930",
            {"1305": rec}, {}, code_table, full=True,
        )

        path = tmp_path / "kline" / "2026" / "20260520" / "kline_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            next(reader)
            row = next(reader)
            # open=high=low=close=lastprice for carry-forward
            assert row[3] == row[4] == row[5] == row[6] == "4123.00"
            assert row[7] == "0"  # volume
            assert row[9] == "0"  # count
            assert row[-1] == "N"  # update_flag
            assert row[-2] == "N"  # trade_flag


class TestFilePathHelpers:
    def test_snapshot_file_path(self):
        path = get_snapshot_file_path("/output", "202605200930")
        assert path == os.path.join("/output", "snapshot", "2026", "20260520", "snapshot_minute_20260520_0930.csv")

    def test_order_file_path(self):
        path = get_order_file_path("/output", "202605200930")
        assert path == os.path.join("/output", "order", "2026", "20260520", "order_minute_20260520_0930.csv")


class TestAppendOrderRecords:
    def test_append_to_existing_file(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path), "202605200930", records)

        late_records = [
            OrderRecord(symbol="1305", seqno=2, time=20260520093001500,
                        bidprice=410000.0, bidsize=50.0, askprice=412000.0, asksize=80.0,
                        decimal=2, rcvtime=20260520083001500),
        ]
        path = get_order_file_path(str(tmp_path), "202605200930")
        append_order_records(path, late_records)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "bidprice" in header
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0][1] == "1301"
            assert rows[1][1] == "1305"

    def test_append_multiple_records(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path), "202605200930", records)

        late_records = [
            OrderRecord(symbol="1305", seqno=2, time=20260520093001500,
                        bidprice=410000.0, bidsize=50.0, askprice=412000.0, asksize=80.0,
                        decimal=2, rcvtime=20260520083001500),
            OrderRecord(symbol="1306", seqno=3, time=20260520093001600,
                        bidprice=420000.0, bidsize=60.0, askprice=422000.0, asksize=90.0,
                        decimal=2, rcvtime=20260520083001600),
        ]
        path = get_order_file_path(str(tmp_path), "202605200930")
        append_order_records(path, late_records)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 3


class TestAppendSnapshotRecords:
    def test_append_with_update_flag_y(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        late_recs = [
            make_snapshot(symbol="1305", seqno=10, lastprice=4120.0),
        ]
        path = get_snapshot_file_path(str(tmp_path), "202605200930")
        append_snapshot_records(path, late_recs, code_table)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2
            appended = [r for r in rows if r[1] == "1305"][0]
            assert appended[-1] == "Y"

    def test_append_preserves_existing_rows(self, tmp_path):
        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": rec}, {"1301": agg}, code_table, full=True,
        )

        late_recs = [make_snapshot(symbol="1301", seqno=20, lastprice=9999.0)]
        path = get_snapshot_file_path(str(tmp_path), "202605200930")
        append_snapshot_records(path, late_recs, code_table)

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0][1] == "1301"
            assert rows[1][1] == "1301"


class TestWriteLock:
    def test_same_path_returns_same_lock(self):
        lock1 = _get_write_lock("/tmp/test.csv")
        lock2 = _get_write_lock("/tmp/test.csv")
        assert lock1 is lock2

    def test_different_paths_return_different_locks(self):
        lock1 = _get_write_lock("/tmp/test1.csv")
        lock2 = _get_write_lock("/tmp/test2.csv")
        assert lock1 is not lock2

    def test_concurrent_append_and_atomic_write(self, tmp_path):
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}

        path = get_snapshot_file_path(str(tmp_path), "202605200930")

        rec = make_snapshot(symbol="1301")
        agg = make_agg(symbol="1301")
        write_snapshot_file(str(tmp_path), "202605200930", {"1301": rec}, {"1301": agg}, code_table, full=True)

        errors = []

        def do_append():
            try:
                for _ in range(20):
                    late = make_snapshot(symbol="1305", seqno=100)
                    append_snapshot_records(path, [late], code_table)
            except Exception as e:
                errors.append(e)

        def do_atomic_write():
            try:
                for _ in range(20):
                    rec2 = make_snapshot(symbol="1301", seqno=200)
                    agg2 = make_agg(symbol="1301")
                    write_snapshot_file(
                        str(tmp_path), "202605200930",
                        {"1301": rec2}, {"1301": agg2}, code_table, full=True,
                    )
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=do_append)
        t2 = threading.Thread(target=do_atomic_write)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert "update_flag" in header
            rows = list(reader)
            assert len(rows) >= 1


class TestMinuteEndThreshold:
    def test_basic_minute(self):
        assert _minute_end_threshold("202605200930") == 20260520093100000

    def test_hour_boundary(self):
        assert _minute_end_threshold("202605200859") == 20260520090000000

    def test_day_boundary(self):
        assert _minute_end_threshold("202605202359") == 20260521000000000


class TestCarryForwardTimeFilter:
    def test_snapshot_skips_future_carry_forward(self, tmp_path):
        """Carry-forward with time >= minute_end is skipped."""
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        # time=202605210900... is >= minute_end for minute 202605200930 (093100000)
        future_rec = make_snapshot(symbol="FUTURE", time=20260521090000999)
        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"FUTURE": future_rec}, {}, code_table, full=True,
        )
        path = tmp_path / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
            # The future carry-forward should be skipped — only header
            assert len(rows) == 0

    def test_snapshot_keeps_valid_carry_forward(self, tmp_path):
        """Carry-forward with time < minute_end is kept."""
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        # time=202605200929... is < minute_end for minute 202605200930 (093100000)
        valid_rec = make_snapshot(symbol="VALID", time=20260520092900999)
        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"VALID": valid_rec}, {}, code_table, full=True,
        )
        path = tmp_path / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0][1] == "VALID"
            assert rows[0][-1] == "N"

    def test_kline_skips_future_carry_forward(self, tmp_path):
        """Kline carry-forward with time >= minute_end is skipped."""
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        future_rec = make_snapshot(symbol="FUTURE", time=20260521090000999)
        write_kline_file(
            str(tmp_path), "202605200930",
            {"FUTURE": future_rec}, {}, code_table, full=True,
        )
        path = tmp_path / "kline" / "2026" / "20260520" / "kline_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 0

    def test_active_symbol_not_filtered(self, tmp_path):
        """Symbols with ohlcv_data are NOT filtered even if snapshot has future time."""
        code_table = CodeTable.__new__(CodeTable)
        code_table._table = {}
        # snapshot_copy has future time, but symbol is in ohlcv_data
        future_rec = make_snapshot(symbol="1301", time=20260521090000999)
        agg = make_agg(symbol="1301")
        write_snapshot_file(
            str(tmp_path), "202605200930",
            {"1301": future_rec}, {"1301": agg}, code_table, full=True,
        )
        path = tmp_path / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0930.csv"
        with open(path) as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            # Active symbol should still appear (update_flag=Y)
            assert len(rows) == 1
            assert rows[0][-1] == "Y"


class TestStreamingWriteOrderFile:
    """Verify streaming write produces byte-identical output and uses write lock."""

    def test_streaming_write_output_matches(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
            OrderRecord(symbol="1305", seqno=2, time=20260520093001500,
                        bidprice=410000.0, bidsize=50.0, askprice=412000.0, asksize=80.0,
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
            assert rows[1][1] == "1305"

    def test_streaming_write_acquires_lock(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        path = get_order_file_path(str(tmp_path), "202605200930")
        lock = _get_write_lock(path)
        assert lock is not None
        write_order_file(str(tmp_path), "202605200930", records)
        assert (tmp_path / "order" / "2026" / "20260520" / "order_minute_20260520_0930.csv").exists()

    def test_streaming_write_no_tmp_left(self, tmp_path):
        records = [
            OrderRecord(symbol="1301", seqno=1, time=20260520093000999,
                        bidprice=450000.0, bidsize=100.0, askprice=451000.0, asksize=200.0,
                        decimal=2, rcvtime=20260520083000999),
        ]
        write_order_file(str(tmp_path), "202605200930", records)
        path = get_order_file_path(str(tmp_path), "202605200930")
        assert not os.path.exists(path + ".tmp")


class TestGetTickfilePath:
    def test_path_format(self):
        path = get_tickfile_path("output", "202606020900")
        expected = os.path.join("output", "tickfile", "2026", "20260602", "tickfile_20260602.csv")
        assert path == expected

    def test_same_day_returns_same_path(self):
        p1 = get_tickfile_path("out", "202606020900")
        p2 = get_tickfile_path("out", "202606021530")
        assert p1 == p2


def _make_tick_snap(symbol="7203", **kw):
    from tests.test_tickfile import _make_snapshot
    return _make_snapshot(symbol=symbol, **kw)


def _make_tick_order(symbol="7203", **kw):
    from tests.test_tickfile import _make_order
    return _make_order(symbol=symbol, **kw)


class TestWriteTickfileRows:
    def test_first_write_creates_header_and_data(self, tmp_path):
        snap = _make_tick_snap(symbol="7203")
        order = _make_tick_order(symbol="7203")
        selected = [("7203", snap, order)]
        output_dir = str(tmp_path)
        write_tickfile_rows(output_dir, "202606020900", selected, 1)
        path = get_tickfile_path(output_dir, "202606020900")
        assert os.path.exists(path)
        with open(path, encoding="utf-8", newline="") as f:
            lines = f.readlines()
        assert lines[0].strip() == TICKFILE_HEADER
        assert len(lines) == 2
        assert len(lines[1].strip().split(',')) == 65

    def test_append_adds_rows_without_header(self, tmp_path):
        snap1 = _make_tick_snap(symbol="7203")
        order1 = _make_tick_order(symbol="7203")
        write_tickfile_rows(str(tmp_path), "202606020900", [("7203", snap1, order1)], 1)
        snap2 = _make_tick_snap(symbol="6758")
        order2 = _make_tick_order(symbol="6758")
        write_tickfile_rows(str(tmp_path), "202606020901", [("6758", snap2, order2)], 2)
        path = get_tickfile_path(str(tmp_path), "202606020900")
        with open(path, encoding="utf-8", newline="") as f:
            lines = f.readlines()
        assert len(lines) == 3

    def test_zero_symbols_writes_nothing(self, tmp_path):
        write_tickfile_rows(str(tmp_path), "202606020900", [], 1)
        path = get_tickfile_path(str(tmp_path), "202606020900")
        assert not os.path.exists(path)

    def test_corrupted_header_raises_ioerror(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("WRONG,HEADER\n")
        snap = _make_tick_snap(symbol="7203")
        with pytest.raises(IOError, match="corrupt"):
            write_tickfile_rows(str(tmp_path), "202606020900", [("7203", snap, None)], 1)

    def test_truncated_last_line_repaired(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(TICKFILE_HEADER + "\n")
            f.write("7203,20260602")
        snap = _make_tick_snap(symbol="7203")
        write_tickfile_rows(str(tmp_path), "202606020900", [("7203", snap, None)], 1)
        with open(path, encoding="utf-8", newline="") as f:
            lines = f.readlines()
        assert len(lines) == 3
        assert lines[2].strip().split(',')[0] == "7203"

    def test_empty_file_overwritten(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").close()
        snap = _make_tick_snap(symbol="7203")
        write_tickfile_rows(str(tmp_path), "202606020900", [("7203", snap, None)], 1)
        with open(path, encoding="utf-8", newline="") as f:
            lines = f.readlines()
        assert lines[0].strip() == TICKFILE_HEADER


class TestRecoverTickfileSeqno:
    def test_nonexistent_file_returns_zero(self, tmp_path):
        result = recover_tickfile_seqno(str(tmp_path), "202606020900")
        assert result == 0

    def test_empty_file_returns_zero(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").close()
        result = recover_tickfile_seqno(str(tmp_path), "202606020900")
        assert result == 0

    def test_header_only_returns_zero(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(TICKFILE_HEADER + "\n")
        result = recover_tickfile_seqno(str(tmp_path), "202606020900")
        assert result == 0

    def test_recovers_max_seqno(self, tmp_path):
        snap = _make_tick_snap(symbol="7203")
        order = _make_tick_order(symbol="7203")
        write_tickfile_rows(str(tmp_path), "202606020900", [("7203", snap, order)], 5)
        write_tickfile_rows(str(tmp_path), "202606020901", [("7203", snap, order)], 10)
        result = recover_tickfile_seqno(str(tmp_path), "202606020900")
        assert result == 10

    def test_skips_truncated_lines(self, tmp_path):
        path = get_tickfile_path(str(tmp_path), "202606020900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(TICKFILE_HEADER + "\n")
            fields = ["col"] * 59 + ["7203"] + ["col"] * 5
            f.write(",".join(fields) + "\n")
            f.write("truncated\n")
            fields2 = ["col"] * 59 + ["42"] + ["col"] * 5
            f.write(",".join(fields2) + "\n")
        result = recover_tickfile_seqno(str(tmp_path), "202606020900")
        assert result == 42


class TestTickfileMaxRowBytesAssert:
    """Spec N41: runtime assert TICKFILE_TAIL_READ_SIZE >= MAX_ROW_BYTES * 6."""

    def test_runtime_assert_holds(self):
        from minute_bar.writer import TICKFILE_MAX_ROW_BYTES, TICKFILE_TAIL_READ_SIZE
        assert TICKFILE_TAIL_READ_SIZE >= TICKFILE_MAX_ROW_BYTES * 6
