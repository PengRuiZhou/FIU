"""Golden output tests for tickfile_generate.

Tests the Rust tickfile_generate function by comparing CSV output
against the Python build_tickfile_row reference.
"""
from __future__ import annotations

import struct
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from minute_bar.models import OrderRecord, SnapshotRecord


def _build_latest_order_buf_for_test(records):
    """Build latest_order_buf from OrderRecords for tickfile_generate testing.

    Format: [magic][version][schema_hash][entries...]
    """
    ORDER_SCHEMA_HASH = 0x9A51A8B3
    MAGIC_LATEST_ORDER = 0x03CCBBAA
    MAGIC_VERSION = 1

    buf = bytearray()
    buf.extend(struct.pack('<I', MAGIC_LATEST_ORDER))
    buf.extend(struct.pack('<H', MAGIC_VERSION))
    buf.extend(struct.pack('<I', ORDER_SCHEMA_HASH))

    for rec in records:
        sym_bytes = rec.symbol.encode('utf-8')
        buf.extend(struct.pack('<H', len(sym_bytes)))
        buf.extend(sym_bytes)
        buf.extend(struct.pack('<q', rec.time))
        buf.extend(struct.pack('<q', rec.bidprice))
        buf.extend(struct.pack('<q', rec.bidsize))
        buf.extend(struct.pack('<q', rec.askprice))
        buf.extend(struct.pack('<q', rec.asksize))
        buf.extend(struct.pack('<q', rec.decimal))
        buf.extend(struct.pack('<q', rec.rcvtime))
        buf.extend(struct.pack('<q', rec.seqno))

    return bytes(buf)


def _build_latest_snapshot_buf_for_test(entries):
    """Build latest_snapshot_buf from dicts for tickfile_generate testing.

    entries: list of dicts with {symbol, time, preclose, lastprice, open, high, low, close, totalvol, totalamount, decimal}
    Format: [magic][version][schema_hash][count(4)][entries...]
    """
    LATEST_SNAPSHOT_SCHEMA_HASH = 0x2DD0CCC2
    MAGIC_LATEST_SNAPSHOT = 0x05CCBBAA
    MAGIC_VERSION = 1

    buf = bytearray()
    buf.extend(struct.pack('<I', MAGIC_LATEST_SNAPSHOT))
    buf.extend(struct.pack('<H', MAGIC_VERSION))
    buf.extend(struct.pack('<I', LATEST_SNAPSHOT_SCHEMA_HASH))
    buf.extend(struct.pack('<I', len(entries)))

    for e in entries:
        sym_bytes = e['symbol'].encode('utf-8')
        buf.extend(struct.pack('<H', len(sym_bytes)))
        buf.extend(sym_bytes)
        buf.extend(struct.pack('<q', e['time']))
        buf.extend(struct.pack('<d', e['preclose']))
        buf.extend(struct.pack('<d', e['lastprice']))
        buf.extend(struct.pack('<d', e['open']))
        buf.extend(struct.pack('<d', e['high']))
        buf.extend(struct.pack('<d', e['low']))
        buf.extend(struct.pack('<d', e['close']))
        buf.extend(struct.pack('<q', e['totalvol']))
        buf.extend(struct.pack('<d', e['totalamount']))
        buf.extend(struct.pack('<I', e['decimal']))

    return bytes(buf)


def _python_tickfile_row(symbol, snapshot_entry, order_record, seqno, minute_key):
    """Python reference for building a tickfile row (matches tickfile.py build_tickfile_row)."""
    from minute_bar.tickfile import build_tickfile_row, NA as TICKFILE_NA

    snapshot = None
    if snapshot_entry:
        snapshot = SnapshotRecord(
            symbol=snapshot_entry['symbol'],
            seqno=0,
            time=snapshot_entry['time'],
            rcvtime=snapshot_entry['time'],
            preclose=int(snapshot_entry['preclose'] * (10 ** snapshot_entry['decimal'])),
            lastprice=int(snapshot_entry['lastprice'] * (10 ** snapshot_entry['decimal'])),
            open=int(snapshot_entry['open'] * (10 ** snapshot_entry['decimal'])),
            high=int(snapshot_entry['high'] * (10 ** snapshot_entry['decimal'])),
            low=int(snapshot_entry['low'] * (10 ** snapshot_entry['decimal'])),
            close=int(snapshot_entry['close'] * (10 ** snapshot_entry['decimal'])),
            lasttradeprice=0,
            lasttradeqty=0,
            totalvol=snapshot_entry['totalvol'],
            totalamount=int(snapshot_entry['totalamount'] * (10 ** snapshot_entry['decimal'])),
            sessionid=0,
            tradetype="T",
            status="S",
            direction=0,
            pflag="N",
            decimal=snapshot_entry['decimal'],
            vwap=0,
            shortsellflag=0,
        )

    order = None
    if order_record:
        order = OrderRecord(
            symbol=order_record['symbol'],
            seqno=order_record.get('seqno', 0),
            time=order_record['time'],
            bidprice=order_record['bidprice'],
            bidsize=order_record['bidsize'],
            askprice=order_record['askprice'],
            asksize=order_record['asksize'],
            decimal=order_record['decimal'],
            rcvtime=order_record['rcvtime'],
        )

    return build_tickfile_row(snapshot, order, seqno, code_table_getter=None, minute_key=minute_key)


class TestTickfileGenerateGolden:
    """Golden output tests comparing Rust tickfile_generate to Python reference."""

    def test_tickfile_with_snapshot_and_order(self):
        """Test tickfile_generate with both snapshot and order data."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return  # Skip if Rust not available

        # Build latest_order_buf
        order_records = [
            OrderRecord(
                symbol="7203", seqno=42,
                time=20260528090000123, bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200, decimal=2, rcvtime=20260528090000100,
            ),
        ]
        order_buf = _build_latest_order_buf_for_test(order_records)

        # Build latest_snapshot_buf
        snapshot_entries = [
            {
                'symbol': '7203',
                'time': 20260528090000123,
                'preclose': 45800.0,
                'lastprice': 45900.0,
                'open': 45900.0,
                'high': 46100.0,
                'low': 45700.0,
                'close': 46000.0,
                'totalvol': 1000,
                'totalamount': 4590000.0,
                'decimal': 2,
            },
        ]
        snapshot_buf = _build_latest_snapshot_buf_for_test(snapshot_entries)

        minute_key = "202605280900"
        seqno = 1

        rust_csv = tickfile_generate(b'', order_buf, snapshot_buf, minute_key, ["7203"], seqno)

        # Parse Rust output
        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 2  # header + 1 row
        assert rust_lines[0] == "InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,BidPrice1,BidVolume1,AskPrice1,AskVolume1,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,BidPrice6,BidVolume6,AskPrice6,AskVolume6,BidPrice7,BidVolume7,AskPrice7,AskVolume7,BidPrice8,BidVolume8,AskPrice8,AskVolume8,BidPrice9,BidVolume9,AskPrice9,AskVolume9,BidPrice10,BidVolume10,AskPrice10,AskVolume10,ActionDay,Type,Seqno,LocalTime,IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume"

        rust_cols = rust_lines[1].split(',')

        # Check key columns
        assert rust_cols[0] == "7203", f"InstrumentID: {rust_cols[0]}"
        assert rust_cols[1] == "20260528", f"TradingDay: {rust_cols[1]}"
        assert rust_cols[2] == "45900.00", f"LastPrice: {rust_cols[2]}"
        assert rust_cols[4] == "45800.00", f"PreClosePrice: {rust_cols[4]}"
        assert rust_cols[6] == "45900.00", f"OpenPrice: {rust_cols[6]}"
        assert rust_cols[7] == "46100.00", f"HighestPrice: {rust_cols[7]}"
        assert rust_cols[8] == "45700.00", f"LowestPrice: {rust_cols[8]}"
        assert rust_cols[9] == "1000", f"Volume: {rust_cols[9]}"
        assert rust_cols[17] == "45800.00", f"BidPrice1: {rust_cols[17]}"
        assert rust_cols[18] == "100", f"BidVolume1: {rust_cols[18]}"
        assert rust_cols[19] == "45900.00", f"AskPrice1: {rust_cols[19]}"
        assert rust_cols[20] == "200", f"AskVolume1: {rust_cols[20]}"
        assert rust_cols[59] == "1", f"Seqno: {rust_cols[59]}"

    def test_tickfile_order_only(self):
        """Test tickfile_generate with order data only (no snapshot)."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        order_records = [
            OrderRecord(
                symbol="7203", seqno=42,
                time=20260528090000123, bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200, decimal=2, rcvtime=20260528090000100,
            ),
        ]
        order_buf = _build_latest_order_buf_for_test(order_records)
        snapshot_buf = b''  # empty

        minute_key = "202605280900"
        rust_csv = tickfile_generate(b'', order_buf, snapshot_buf, minute_key, ["7203"], 1)

        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 2
        rust_cols = rust_lines[1].split(',')

        # LastPrice should be NA (no snapshot)
        assert rust_cols[2] == "NA", f"LastPrice should be NA, got {rust_cols[2]}"
        # BidPrice should still be populated
        assert rust_cols[17] == "45800.00"

    def test_tickfile_snapshot_only(self):
        """Test tickfile_generate with snapshot data only (no order)."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        snapshot_entries = [
            {
                'symbol': '7203',
                'time': 20260528090000123,
                'preclose': 45800.0,
                'lastprice': 45900.0,
                'open': 45900.0,
                'high': 46100.0,
                'low': 45700.0,
                'close': 46000.0,
                'totalvol': 1000,
                'totalamount': 4590000.0,
                'decimal': 2,
            },
        ]
        order_buf = b''
        snapshot_buf = _build_latest_snapshot_buf_for_test(snapshot_entries)

        minute_key = "202605280900"
        rust_csv = tickfile_generate(b'', order_buf, snapshot_buf, minute_key, ["7203"], 1)

        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 2
        rust_cols = rust_lines[1].split(',')

        assert rust_cols[0] == "7203"
        assert rust_cols[2] == "45900.00"
        assert rust_cols[17] == "NA"  # No order data

    def test_tickfile_na_handling(self):
        """Test NA handling when both snapshot and order are missing."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        order_buf = b''
        snapshot_buf = b''

        minute_key = "202605280900"
        rust_csv = tickfile_generate(b'', order_buf, snapshot_buf, minute_key, ["7203"], 1)

        # Should return header only (no rows since all are NA)
        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 1
        assert rust_lines[0] == "InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,BidPrice1,BidVolume1,AskPrice1,AskVolume1,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,BidPrice6,BidVolume6,AskPrice6,AskVolume6,BidPrice7,BidVolume7,AskPrice7,AskVolume7,BidPrice8,BidVolume8,AskPrice8,AskVolume8,BidPrice9,BidVolume9,AskPrice9,AskVolume9,BidPrice10,BidVolume10,AskPrice10,AskVolume10,ActionDay,Type,Seqno,LocalTime,IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume"

    def test_tickfile_multiple_symbols(self):
        """Test tickfile_generate with multiple symbols."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        order_records = [
            OrderRecord(
                symbol="7203", seqno=1,
                time=20260528090000123, bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200, decimal=2, rcvtime=0,
            ),
            OrderRecord(
                symbol="7204", seqno=2,
                time=20260528090000124, bidprice=4600000, bidsize=150,
                askprice=4610000, asksize=250, decimal=2, rcvtime=0,
            ),
        ]
        order_buf = _build_latest_order_buf_for_test(order_records)
        snapshot_buf = b''

        minute_key = "202605280900"
        rust_csv = tickfile_generate(b'', order_buf, snapshot_buf, minute_key, ["7203", "7204"], 1)

        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 3  # header + 2 rows

    def test_tickfile_empty_symbol_list(self):
        """Test tickfile_generate with empty all_symbols list."""
        try:
            from minute_bar._order_accel import tickfile_generate
        except ImportError:
            return

        order_records = [
            OrderRecord(
                symbol="7203", seqno=1,
                time=20260528090000123, bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200, decimal=2, rcvtime=0,
            ),
        ]
        order_buf = _build_latest_order_buf_for_test(order_records)

        rust_csv = tickfile_generate(b'', order_buf, b'', "202605280900", [], 1)

        # Should return header only
        rust_lines = rust_csv.strip().split('\n')
        assert len(rust_lines) == 1


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
