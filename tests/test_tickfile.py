import pytest

from minute_bar.tickfile import TICKFILE_HEADER, build_tickfile_row, select_tickfile_records, NA
from minute_bar.models import SnapshotRecord, OrderRecord
from minute_bar.csv_parser import ParsedCode


# --- Helpers ---

def _make_snapshot(**overrides) -> SnapshotRecord:
    defaults = dict(
        symbol="7203", seqno=1, time=20260602090013000, rcvtime=20260602080013000,
        preclose=443500.0, lastprice=444000.0, open=443800.0, high=444500.0,
        low=443000.0, close=444000.0, lasttradeprice=444000.0, lasttradeqty=100,
        totalvol=1000, totalamount=44400000.0, sessionid=1, tradetype="",
        status="T", direction=0, pflag="N", decimal=2, vwap=443500.0,
        shortsellflag=0,
    )
    defaults.update(overrides)
    return SnapshotRecord(**defaults)


def _make_order(**overrides) -> OrderRecord:
    defaults = dict(
        symbol="7203", seqno=1, time=20260602090013000,
        bidprice=443500, bidsize=100, askprice=444500, asksize=200,
        decimal=2, rcvtime=20260602080013000,
    )
    defaults.update(overrides)
    return OrderRecord(**defaults)


def _make_code(**overrides) -> ParsedCode:
    defaults = dict(
        symbol="7203", market=1, marketdesc="TSE", name="Toyota",
        money="JPY", type="1", subtype="", issueclass="",
        industrycode="", isincode="", lotsize=100,
        limitup=449000, limitdown=438000, decimal=2,
        rcvtime=20260602080000000, businessday="20260602", baseprice=443500,
    )
    defaults.update(overrides)
    return ParsedCode(**defaults)


# --- Header Tests ---

class TestTickfileHeader:
    def test_column_count_is_65(self):
        assert len(TICKFILE_HEADER.split(',')) == 65

    def test_first_column_is_instrument_id(self):
        assert TICKFILE_HEADER.split(',')[0] == 'InstrumentID'

    def test_last_column_is_close_auction_volume(self):
        assert TICKFILE_HEADER.split(',')[-1] == 'CloseAuctionVolume'


# --- build_tickfile_row Tests ---

class TestBuildTickfileRow:
    def test_full_row_has_65_columns(self):
        snap = _make_snapshot()
        order = _make_order()
        code = _make_code()
        getter = lambda s, c=code: c if s == "7203" else None
        row = build_tickfile_row(snap, order, 1, getter)
        assert len(row.split(',')) == 65

    def test_instrument_id(self):
        snap = _make_snapshot(symbol="6758")
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[0] == "6758"

    def test_trading_day_from_snapshot_time(self):
        snap = _make_snapshot(time=20260602090013000)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[1] == "20260602"

    def test_lastprice_decimal_conversion(self):
        snap = _make_snapshot(lastprice=444000.0, decimal=2)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[2] == "4440.0"

    def test_lastprice_zero_is_na(self):
        snap = _make_snapshot(lastprice=0.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[2] == NA

    def test_preclose_zero_is_na(self):
        snap = _make_snapshot(preclose=0.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[4] == NA

    def test_preclose_nonzero_decimal(self):
        snap = _make_snapshot(preclose=443500.0, decimal=2)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[4] == "4435.0"

    def test_volume_output_as_int(self):
        snap = _make_snapshot(totalvol=1234)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[9] == "1234"

    def test_volume_none_snapshot_is_na(self):
        # Both None → function returns single "NA" — this case is filtered by select_tickfile_records
        row = build_tickfile_row(None, None, 1, None)
        assert row == NA

    def test_turnover_decimal_conversion(self):
        snap = _make_snapshot(totalamount=350251000.0, decimal=2)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[10] == "3502510.0"

    def test_turnover_zero_is_zero_not_na(self):
        snap = _make_snapshot(totalamount=0.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[10] == "0.0"

    def test_update_time_format(self):
        snap = _make_snapshot(rcvtime=20260602080013000)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[16] == "20260602 08:00:00"

    def test_update_time_from_order_when_no_snapshot(self):
        order = _make_order(rcvtime=20260602090015000)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[16] == "20260602 09:00:00"

    def test_update_time_short_rcvtime_is_na(self):
        snap = _make_snapshot(rcvtime=2026052209)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[16] == NA

    def test_bidprice1_from_order(self):
        order = _make_order(bidprice=443500.0, decimal=2)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[17] == "4435.0"

    def test_bidprice1_zero_is_na(self):
        order = _make_order(bidprice=0.0)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[17] == NA

    def test_bidvolume1_from_order(self):
        order = _make_order(bidsize=100)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[18] == "100"

    def test_bidvolume1_none_order_is_na(self):
        row = build_tickfile_row(_make_snapshot(), None, 1, None)
        assert row.split(',')[18] == NA

    def test_askprice1_from_order(self):
        order = _make_order(askprice=444500.0, decimal=2)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[19] == "4445.0"

    def test_askvolume1_from_order(self):
        order = _make_order(asksize=200)
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[20] == "200"

    def test_bidprice2_through_10_are_na(self):
        row = build_tickfile_row(_make_snapshot(), _make_order(), 1, None)
        cols = row.split(',')
        for i in range(21, 56, 2):
            assert cols[i] == NA, f"Column {i} should be NA"

    def test_bidvolume2_through_10_are_na(self):
        row = build_tickfile_row(_make_snapshot(), _make_order(), 1, None)
        cols = row.split(',')
        for i in range(22, 57, 2):
            assert cols[i] == NA, f"Column {i} should be NA"

    def test_action_day_matches_trading_day(self):
        snap = _make_snapshot(time=20260602090013000)
        row = build_tickfile_row(snap, None, 1, None)
        cols = row.split(',')
        assert cols[1] == cols[57]

    def test_type_is_69(self):
        row = build_tickfile_row(_make_snapshot(), None, 1, None)
        assert row.split(',')[58] == "69"

    def test_seqno_passed_through(self):
        row = build_tickfile_row(_make_snapshot(), None, 42, None)
        assert row.split(',')[59] == "42"

    def test_local_time_format(self):
        snap = _make_snapshot(time=20260602090013000)
        row = build_tickfile_row(snap, None, 1, None)
        cols = row.split(',')
        # time=20260602090013000 → s[14:17]="000" (ms=0), s[12:14]="13" (sec=13)
        assert cols[60] == "2026-06-02 09:00:13.000000"

    def test_local_time_from_order_when_no_snapshot(self):
        order = _make_order(time=20260602090105000)
        row = build_tickfile_row(None, order, 1, None)
        cols = row.split(',')
        # time=20260602090105000 → s[14:17]="000" (ms=0)
        assert cols[60] == "2026-06-02 09:01:05.000000"

    def test_intra_daily_return_calculation(self):
        snap = _make_snapshot(lastprice=444000.0, preclose=443500.0, decimal=2)
        row = build_tickfile_row(snap, None, 1, None)
        ret = float(row.split(',')[61])
        assert abs(ret - 0.001127) < 0.0001

    def test_intra_daily_return_preclose_zero_is_na(self):
        snap = _make_snapshot(preclose=0.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[61] == NA

    def test_intra_daily_return_negative_zero_normalized(self):
        snap = _make_snapshot(lastprice=444000.0, preclose=444000.0, decimal=2)
        row = build_tickfile_row(snap, None, 1, None)
        ret_str = row.split(',')[61]
        assert ret_str == "0.0"

    def test_is_short_restricted_0_is_N(self):
        snap = _make_snapshot(shortsellflag=0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[62] == "N"

    def test_is_short_restricted_1_is_Y(self):
        snap = _make_snapshot(shortsellflag=1)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[62] == "Y"

    def test_is_short_restricted_other_is_na(self):
        snap = _make_snapshot(shortsellflag=2)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[62] == NA

    def test_is_short_restricted_no_snapshot_is_na(self):
        order = _make_order()
        row = build_tickfile_row(None, order, 1, None)
        assert row.split(',')[62] == NA

    def test_upper_limit_price_from_code_table(self):
        snap = _make_snapshot()
        code = _make_code(limitup=449000, decimal=2)
        getter = lambda s, c=code: c if s == "7203" else None
        row = build_tickfile_row(snap, None, 1, getter)
        assert row.split(',')[14] == "4490.0"

    def test_lower_limit_price_from_code_table(self):
        snap = _make_snapshot()
        code = _make_code(limitdown=438000, decimal=2)
        getter = lambda s, c=code: c if s == "7203" else None
        row = build_tickfile_row(snap, None, 1, getter)
        assert row.split(',')[15] == "4380.0"

    def test_limit_price_na_when_no_code_table(self):
        snap = _make_snapshot()
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[14] == NA
        assert row.split(',')[15] == NA

    def test_limit_price_na_when_code_missing_symbol(self):
        snap = _make_snapshot(symbol="9999")
        code = _make_code(symbol="7203")
        getter = lambda s, c=code: c if s == "7203" else None
        row = build_tickfile_row(snap, None, 1, getter)
        assert row.split(',')[14] == NA

    def test_decimal_zero_no_scaling(self):
        snap = _make_snapshot(decimal=0, lastprice=448000.0, preclose=447000.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[2] == "448000.0"

    def test_decimal_negative_protection(self):
        snap = _make_snapshot(decimal=-1, lastprice=448000.0)
        row = build_tickfile_row(snap, None, 1, None)
        assert row.split(',')[2] == "448000.0"

    def test_snapshot_none_order_exists(self):
        order = _make_order(time=20260602090100000, rcvtime=20260602080100000)
        row = build_tickfile_row(None, order, 1, None)
        cols = row.split(',')
        assert cols[0] == "7203"
        assert cols[1] == "20260602"
        assert cols[2] == NA
        assert cols[9] == NA
        assert cols[17] != NA
        assert cols[57] == "20260602"

    def test_order_none_snapshot_exists(self):
        snap = _make_snapshot()
        row = build_tickfile_row(snap, None, 1, None)
        cols = row.split(',')
        assert cols[17] == NA
        assert cols[18] == NA
        assert cols[19] == NA
        assert cols[20] == NA

    def test_open_auction_volume_always_na(self):
        row = build_tickfile_row(_make_snapshot(), _make_order(), 1, None)
        assert row.split(',')[63] == NA

    def test_close_auction_volume_always_na(self):
        row = build_tickfile_row(_make_snapshot(), _make_order(), 1, None)
        assert row.split(',')[64] == NA


# --- select_tickfile_records Tests ---

class TestSelectTickfileRecords:
    def test_raw_records_preferred_over_snapshot_copy(self):
        raw_snap = _make_snapshot(time=20260602090001000, symbol="7203")
        carry_snap = _make_snapshot(time=20260602085959000, symbol="7203")
        raw_records = {"7203": [raw_snap]}
        snapshot_copy = {"7203": carry_snap}
        result = select_tickfile_records(raw_records, snapshot_copy, [], {})
        assert len(result) == 1
        assert result[0][1].time == 20260602090001000

    def test_carry_forward_when_no_raw_records(self):
        carry_snap = _make_snapshot(symbol="7203")
        result = select_tickfile_records({}, {"7203": carry_snap}, [], {})
        assert len(result) == 1
        assert result[0][1] is carry_snap

    def test_order_from_current_minute(self):
        snap = _make_snapshot(symbol="7203")
        order = _make_order(symbol="7203")
        result = select_tickfile_records(
            {"7203": [snap]}, {}, [order], {}
        )
        assert len(result) == 1
        assert result[0][2] is order

    def test_order_carry_forward(self):
        snap = _make_snapshot(symbol="7203")
        old_order = _make_order(symbol="7203", time=20260602090000000)
        result = select_tickfile_records(
            {"7203": [snap]}, {}, [], {"7203": old_order}
        )
        assert len(result) == 1
        assert result[0][2] is old_order

    def test_both_none_excluded(self):
        result = select_tickfile_records({}, {}, [], {})
        assert len(result) == 0

    def test_sorted_by_symbol(self):
        s1 = _make_snapshot(symbol="9999")
        s2 = _make_snapshot(symbol="1111")
        s3 = _make_snapshot(symbol="5555")
        raw = {"9999": [s1], "1111": [s2], "5555": [s3]}
        result = select_tickfile_records(raw, {}, [], {})
        assert [r[0] for r in result] == ["1111", "5555", "9999"]

    def test_earliest_raw_record_selected(self):
        early = _make_snapshot(time=20260602090001000, symbol="7203")
        late = _make_snapshot(time=20260602090030000, symbol="7203")
        raw = {"7203": [late, early]}
        result = select_tickfile_records(raw, {}, [], {})
        assert result[0][1].time == 20260602090001000

    def test_earliest_order_selected(self):
        snap = _make_snapshot(symbol="7203")
        early = _make_order(symbol="7203", time=20260602090001000)
        late = _make_order(symbol="7203", time=20260602090030000)
        result = select_tickfile_records(
            {"7203": [snap]}, {}, [late, early], {}
        )
        assert result[0][2].time == 20260602090001000

    def test_order_carry_forward_across_gap(self):
        snap = _make_snapshot(symbol="7203")
        old_order = _make_order(symbol="7203", time=20260602090000000)
        result = select_tickfile_records(
            {"7203": [snap]}, {}, [], {"7203": old_order}
        )
        assert result[0][2].time == 20260602090000000

    def test_symbol_union(self):
        snap1 = _make_snapshot(symbol="A")
        snap2 = _make_snapshot(symbol="B")
        order_only = _make_order(symbol="C")
        raw = {"A": [snap1]}
        snapshot_copy = {"B": snap2}
        orders = [order_only]
        result = select_tickfile_records(raw, snapshot_copy, orders, {})
        symbols = [r[0] for r in result]
        assert "A" in symbols
        assert "B" in symbols
        assert "C" in symbols

    def test_snapshot_none_order_exists_outputs_row(self):
        order = _make_order(symbol="7203")
        result = select_tickfile_records({}, {}, [order], {})
        assert len(result) == 1
        assert result[0][0] == "7203"
        assert result[0][1] is None
        assert result[0][2] is order

    def test_tie_breaking_same_time_uses_rcvtime(self):
        r1 = _make_snapshot(time=20260602090001000, rcvtime=20260602080002000, symbol="7203")
        r2 = _make_snapshot(time=20260602090001000, rcvtime=20260602080001000, symbol="7203")
        raw = {"7203": [r1, r2]}
        result = select_tickfile_records(raw, {}, [], {})
        assert result[0][1].rcvtime == 20260602080001000
