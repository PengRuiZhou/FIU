"""Tests for aggregator and validator."""
import pytest
from minute_bar.aggregator import MAX_LATE_SNAPSHOT_QUEUE_SIZE, SharedState, build_snapshot_record
from minute_bar.csv_parser import ParsedSnapshot
from minute_bar.validator import validate_snapshot


def make_parsed_snapshot(symbol="1301", time_=20260520093000999, lastprice=450000,
                         totalvol=78300, totalamount=350251000, decimal=2, status="T",
                         lasttradeqty=100, **kwargs):
    defaults = dict(
        symbol=symbol, time=time_, preclose=443500, lastprice=lastprice,
        open=440000, high=451000, low=443500, close=450000,
        lasttradeprice=450000, lasttradeqty=lasttradeqty,
        totalvol=totalvol, totalamount=totalamount, sessionid=1,
        tradetype="", status=status, direction=0, pflag="Y",
        decimal=decimal, vwap=4450000, shortsellflag=0,
        rcvtime=20260520083000999,
    )
    defaults.update(kwargs)
    return ParsedSnapshot(**defaults)


class TestBuildSnapshotRecord:
    def test_decimal_division(self):
        parsed = make_parsed_snapshot(lastprice=450000, decimal=2)
        rec = build_snapshot_record(parsed, seqno=1)
        assert rec.lastprice == 450000.0  # raw value, no division
        assert rec.totalamount == 350251000.0

    def test_decimal_zero(self):
        parsed = make_parsed_snapshot(lastprice=450000, decimal=0)
        rec = build_snapshot_record(parsed, seqno=1)
        assert rec.lastprice == 450000.0

    def test_seqno_assigned(self):
        parsed = make_parsed_snapshot()
        rec = build_snapshot_record(parsed, seqno=42)
        assert rec.seqno == 42


class TestSharedState:
    def test_process_single_snapshot(self):
        state = SharedState()
        parsed = make_parsed_snapshot()
        state.process_snapshot(parsed)
        assert "1301" in state.latest_snapshot
        assert state.seqno == 1
        assert state.first_data_received

    def test_process_multiple_symbols(self):
        state = SharedState()
        for sym in ["1301", "1305", "1310"]:
            state.process_snapshot(make_parsed_snapshot(symbol=sym))
        assert len(state.latest_snapshot) == 3
        assert state.seqno == 3

    def test_invalid_snapshot_skipped(self):
        state = SharedState()
        parsed = make_parsed_snapshot(time_=0)  # invalid time
        state.process_snapshot(parsed)
        assert state.seqno == 0
        assert len(state.latest_snapshot) == 0

    def test_ohlcv_aggregation(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(lastprice=450000, totalvol=100, totalamount=45000000))
        state.process_snapshot(make_parsed_snapshot(lastprice=452000, totalvol=150, totalamount=67800000))
        assert state.seqno == 2
        minute_key = "202605200930"
        assert minute_key in state.ohlcv_buffers
        agg = state.ohlcv_buffers[minute_key]["1301"]
        assert agg.open == 4500.0  # first price
        assert agg.high == 4520.0
        assert agg.close == 4520.0  # last price


class TestValidator:
    def test_valid_snapshot(self):
        parsed = make_parsed_snapshot()
        assert validate_snapshot(parsed)

    def test_invalid_time(self):
        parsed = make_parsed_snapshot(time_=123)
        assert not validate_snapshot(parsed)

    def test_decimal_out_of_range(self):
        parsed = make_parsed_snapshot(decimal=10)
        validate_snapshot(parsed)
        assert parsed.decimal == 0  # corrected to 0

    def test_negative_lastprice_warning(self):
        parsed = make_parsed_snapshot(lastprice=-1)
        validate_snapshot(parsed)  # should still pass with warning


class TestSharedStateLateRecords:
    def test_late_snapshot_routed_to_late_queue(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert len(state._late_snapshot_records) == 1
        mk, rec = state._late_snapshot_records[0]
        assert mk == "202605200930"
        assert rec.symbol == "1301"

    def test_late_snapshot_not_in_normal_buffers(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert "202605200930" not in state.ohlcv_buffers
        assert "202605200930" not in state.raw_snapshot_buffers

    def test_late_snapshot_does_not_update_latest(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert "1301" not in state.latest_snapshot

    def test_normal_snapshot_not_affected_by_late_check(self):
        state = SharedState()
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        assert len(state._late_snapshot_records) == 0
        assert "202605200930" in state.ohlcv_buffers
        assert "1301" in state.latest_snapshot

    def test_pop_late_snapshot_records(self):
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")
        parsed = make_parsed_snapshot(time_=20260520093000999)
        state.process_snapshot(parsed)
        records = state.pop_late_snapshot_records()
        assert len(records) == 1
        assert len(state._late_snapshot_records) == 0

    def test_pop_late_snapshot_records_empty(self):
        state = SharedState()
        records = state.pop_late_snapshot_records()
        assert records == []

    def test_late_snapshot_queue_limit(self):
        """When late queue exceeds MAX_LATE_SNAPSHOT_QUEUE_SIZE, new late records are dropped."""
        state = SharedState()
        state.flushed_snapshot_minutes.add("202605200930")

        # Fill the queue to the limit
        for i in range(MAX_LATE_SNAPSHOT_QUEUE_SIZE):
            parsed = make_parsed_snapshot(symbol=f"SYM{i:05d}", time_=20260520093000999)
            state.process_snapshot(parsed)
        assert len(state._late_snapshot_records) == MAX_LATE_SNAPSHOT_QUEUE_SIZE

        # One more should be dropped
        parsed = make_parsed_snapshot(symbol="OVERFLOW", time_=20260520093000999)
        result = state.process_snapshot(parsed)
        assert result is False
        assert len(state._late_snapshot_records) == MAX_LATE_SNAPSHOT_QUEUE_SIZE


class TestMaybeUpdateLatest:
    def test_first_record_sets_latest(self):
        state = SharedState()
        rec = build_snapshot_record(make_parsed_snapshot(), seqno=1)
        with state.lock:
            state.maybe_update_latest_unlocked(rec)
        assert "1301" in state.latest_snapshot

    def test_newer_record_updates(self):
        state = SharedState()
        rec_old = build_snapshot_record(make_parsed_snapshot(time_=20260520093000999), seqno=1)
        rec_new = build_snapshot_record(make_parsed_snapshot(time_=20260520093100999), seqno=2)
        with state.lock:
            state.maybe_update_latest_unlocked(rec_old)
            state.maybe_update_latest_unlocked(rec_new)
        assert state.latest_snapshot["1301"].seqno == 2

    def test_older_record_does_not_update(self):
        state = SharedState()
        rec_new = build_snapshot_record(make_parsed_snapshot(time_=20260520093100999), seqno=2)
        rec_old = build_snapshot_record(make_parsed_snapshot(time_=20260520093000999), seqno=1)
        with state.lock:
            state.maybe_update_latest_unlocked(rec_new)
            state.maybe_update_latest_unlocked(rec_old)
        assert state.latest_snapshot["1301"].seqno == 2


class TestCurrentMinuteMonotonic:
    def test_current_minute_advances_forward(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(time_=20260521090000999))
        assert state.current_minute == "202605210900"
        state.process_snapshot(make_parsed_snapshot(time_=20260521090100999))
        assert state.current_minute == "202605210901"

    def test_current_minute_does_not_regress(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(time_=20260521090500999))
        assert state.current_minute == "202605210905"
        state.process_snapshot(make_parsed_snapshot(time_=20260521090300999))
        assert state.current_minute == "202605210905"
        state.process_snapshot(make_parsed_snapshot(time_=20260521090600999))
        assert state.current_minute == "202605210906"

    def test_current_minute_first_assignment(self):
        state = SharedState()
        assert state.current_minute == ""
        state.process_snapshot(make_parsed_snapshot(time_=20260521093000999))
        assert state.current_minute == "202605210930"

    def test_same_minute_does_not_change(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(time_=20260521090000999))
        assert state.current_minute == "202605210900"
        state.process_snapshot(make_parsed_snapshot(time_=20260521090000500))
        assert state.current_minute == "202605210900"


class TestPerMinuteSnapshot:
    def test_snapshot_captured_on_minute_advance(self):
        """Snapshot is captured when current_minute advances."""
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999))
        state.process_snapshot(make_parsed_snapshot(symbol="B", time_=20260521090000500))
        # Still minute 0900, no snapshot captured yet
        assert "202605210900" not in state._snapshot_at_minute_end

        # First record of 0901 triggers snapshot for 0900
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090100999))
        assert "202605210900" in state._snapshot_at_minute_end
        assert "202605210901" not in state._snapshot_at_minute_end

    def test_snapshot_contains_pre_advance_state(self):
        """Snapshot for minute N must not contain N+1 data."""
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999, lastprice=100))
        # Advance to 0901 — snapshot for 0900 captured BEFORE this record updates latest_snapshot
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090100999, lastprice=999))
        snap = state._snapshot_at_minute_end["202605210900"]
        assert snap["A"].lastprice == 100  # pre-advance value, not 999

    def test_snapshot_not_captured_for_same_minute(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999))
        state.process_snapshot(make_parsed_snapshot(symbol="B", time_=20260521090000500))
        assert "202605210900" not in state._snapshot_at_minute_end

    def test_snapshot_not_captured_for_first_minute(self):
        """No snapshot for empty current_minute on first data."""
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(time_=20260521090000999))
        assert len(state._snapshot_at_minute_end) == 0

    def test_multiple_minute_advances(self):
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999))
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090100999))
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090200999))
        assert "202605210900" in state._snapshot_at_minute_end
        assert "202605210901" in state._snapshot_at_minute_end
        assert "202605210902" not in state._snapshot_at_minute_end

    def test_late_record_does_not_trigger_snapshot(self):
        """Late records don't go through process_snapshot's minute advance path."""
        state = SharedState()
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999))
        state.flushed_snapshot_minutes.add("202605210900")
        state.process_snapshot(make_parsed_snapshot(symbol="A", time_=20260521090000999))
        # No snapshot captured — late record didn't trigger minute advance
        assert "202605210900" not in state._snapshot_at_minute_end
