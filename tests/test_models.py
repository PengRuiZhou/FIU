"""Tests for data models."""
import pytest
from minute_bar.models import SnapshotRecord, OHLCVAggregate, FileState


def make_snapshot(symbol="1301", seqno=1, time_=20260520093000999, lastprice=450000.0,
                  totalvol=100, totalamount=45000000.0, decimal=2, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_, rcvtime=20260520083000999,
        preclose=443500.0, lastprice=lastprice, open=440000.0, high=451000.0, low=443500.0,
        close=450000.0, lasttradeprice=450000.0, lasttradeqty=100, totalvol=totalvol,
        totalamount=totalamount, sessionid=1, tradetype="", status="T",
        direction=0, pflag="Y", decimal=decimal, vwap=4450000.0, shortsellflag=0,
    )
    defaults.update(kwargs)
    return SnapshotRecord(**defaults)


class TestSnapshotRecord:
    def test_frozen(self):
        rec = make_snapshot()
        with pytest.raises(AttributeError):
            rec.symbol = "1305"

    def test_fields(self):
        rec = make_snapshot(symbol="1301", seqno=42)
        assert rec.symbol == "1301"
        assert rec.seqno == 42


class TestOHLCVAggregate:
    def test_single_update(self):
        agg = OHLCVAggregate(symbol="1301")
        rec = make_snapshot(lastprice=450000.0, totalvol=100, totalamount=45000000.0)
        agg.update(rec, base_vol=50, base_amt=20000000.0)
        assert agg.open == 4500.0
        assert agg.high == 4500.0
        assert agg.low == 4500.0
        assert agg.close == 4500.0
        assert agg.volume == 50
        assert agg.count == 1
        assert agg.end_totalvol == 100

    def test_multi_update(self):
        agg = OHLCVAggregate(symbol="1301")
        prices = [450000.0, 452000.0, 448000.0, 451000.0]
        for i, price in enumerate(prices):
            rec = make_snapshot(lastprice=price, totalvol=100 + i * 10, totalamount=45000000.0 + i * 100000)
            agg.update(rec, base_vol=100, base_amt=45000000.0)
        assert agg.open == 4500.0
        assert agg.high == 4520.0
        assert agg.low == 4480.0
        assert agg.close == 4510.0
        assert agg.count == 4

    def test_volume_negative_clamp(self):
        agg = OHLCVAggregate(symbol="1301")
        rec = make_snapshot(totalvol=50, totalamount=20000000.0)
        agg.update(rec, base_vol=100, base_amt=30000000.0)
        assert agg.volume == 0

    def test_any_lasttradeqty_positive(self):
        agg = OHLCVAggregate(symbol="1301")
        rec1 = make_snapshot(lasttradeqty=0)
        agg.update(rec1, base_vol=0, base_amt=0.0)
        assert not agg.any_lasttradeqty_positive

        rec2 = make_snapshot(lasttradeqty=100)
        agg.update(rec2, base_vol=0, base_amt=0.0)
        assert agg.any_lasttradeqty_positive


class TestFileState:
    def test_defaults(self):
        state = FileState()
        assert state.offset == 0
        assert state.pending_line == b""
        assert state.date == ""
