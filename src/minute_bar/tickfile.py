from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Tuple

from minute_bar.clock import parse_17digit_time
from minute_bar.csv_parser import ParsedCode
from minute_bar.models import OrderRecord, SnapshotRecord

logger = logging.getLogger(__name__)

TICKFILE_HEADER = "InstrumentID,TradingDay,LastPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,UpdateTime,BidPrice1,BidVolume1,AskPrice1,AskVolume1,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,BidPrice6,BidVolume6,AskPrice6,AskVolume6,BidPrice7,BidVolume7,AskPrice7,AskVolume7,BidPrice8,BidVolume8,AskPrice8,AskVolume8,BidPrice9,BidVolume9,AskPrice9,AskVolume9,BidPrice10,BidVolume10,AskPrice10,AskVolume10,ActionDay,Type,Seqno,LocalTime,IntraDailyReturn,IsShortRestricted,OpenAuctionVolume,CloseAuctionVolume"

NA = "NA"


def _safe_divide(value: float, decimal: int) -> float:
    d = 10 ** decimal if decimal > 0 else 1
    return value / d


def _price_or_na(value: float, decimal: int) -> str:
    if value == 0:
        return NA
    return str(_safe_divide(value, decimal))


def build_tickfile_row(
    snapshot: Optional[SnapshotRecord],
    order: Optional[OrderRecord],
    seqno: int,
    code_table_getter: Optional[Callable[[str], Optional[ParsedCode]]] = None,
    minute_key: str = "",
) -> str:
    if snapshot is not None:
        symbol = snapshot.symbol
    elif order is not None:
        symbol = order.symbol
    else:
        return NA

    primary_time = snapshot.time if snapshot is not None else order.time
    primary_rcvtime = snapshot.rcvtime if snapshot is not None else order.rcvtime

    # Column 0: InstrumentID
    instrument_id = symbol

    # Column 1: TradingDay
    trading_day = str(primary_time)[:8]

    # Column 2: LastPrice
    if snapshot is not None:
        last_price = _price_or_na(snapshot.lastprice, snapshot.decimal)
    else:
        last_price = NA

    # Column 3: PreSettlementPrice
    pre_settlement = NA

    # Column 4: PreClosePrice
    if snapshot is not None:
        pre_close = _price_or_na(snapshot.preclose, snapshot.decimal)
    else:
        pre_close = NA

    # Column 5: PreOpenInterest
    pre_open_interest = NA

    # Column 6: OpenPrice
    if snapshot is not None:
        open_price = _price_or_na(snapshot.open, snapshot.decimal)
    else:
        open_price = NA

    # Column 7: HighestPrice
    if snapshot is not None:
        high_price = _price_or_na(snapshot.high, snapshot.decimal)
    else:
        high_price = NA

    # Column 8: LowestPrice
    if snapshot is not None:
        low_price = _price_or_na(snapshot.low, snapshot.decimal)
    else:
        low_price = NA

    # Column 9: Volume
    if snapshot is not None:
        volume = str(int(snapshot.totalvol))
    else:
        volume = NA

    # Column 10: Turnover
    if snapshot is not None:
        turnover = str(_safe_divide(snapshot.totalamount, snapshot.decimal))
    else:
        turnover = NA

    # Column 11: OpenInterest
    open_interest = NA

    # Column 12: ClosePrice
    if snapshot is not None:
        close_price = _price_or_na(snapshot.close, snapshot.decimal)
    else:
        close_price = NA

    # Column 13: SettlementPrice
    settlement = NA

    # Column 14-15: UpperLimitPrice, LowerLimitPrice
    upper_limit = NA
    lower_limit = NA
    if code_table_getter is not None and snapshot is not None:
        code_entry = code_table_getter(symbol)
        if code_entry is not None:
            if code_entry.limitup != 0:
                upper_limit = str(_safe_divide(float(code_entry.limitup), code_entry.decimal))
            if code_entry.limitdown != 0:
                lower_limit = str(_safe_divide(float(code_entry.limitdown), code_entry.decimal))
            if snapshot.decimal != code_entry.decimal:
                logger.warning(
                    "Snapshot decimal (%d) != code decimal (%d) for symbol %s",
                    snapshot.decimal, code_entry.decimal, symbol,
                )

    # Column 16: UpdateTime (local time when this tick record was generated, CST/UTC+8)
    # Derived from minute_key (the minute being generated), NOT from snapshot rcvtime.
    # This ensures carry-forward rows also have the correct minute-level UpdateTime.
    if minute_key and len(minute_key) >= 12:
        update_time = minute_key[:8] + " " + minute_key[8:10] + ":" + minute_key[10:12] + ":00"
    else:
        # Fallback: derive from rcvtime (backward compatible)
        rcvtime_str = str(primary_rcvtime)
        if len(rcvtime_str) >= 12:
            update_time = rcvtime_str[:8] + " " + rcvtime_str[8:10] + ":" + rcvtime_str[10:12] + ":00"
        else:
            update_time = NA

    # Columns 17-20: BidPrice1, BidVolume1, AskPrice1, AskVolume1
    if order is not None:
        order_decimal = order.decimal
        bid_price1 = _price_or_na(order.bidprice, order_decimal)
        bid_volume1 = str(order.bidsize)
        ask_price1 = _price_or_na(order.askprice, order_decimal)
        ask_volume1 = str(order.asksize)
    else:
        bid_price1 = NA
        bid_volume1 = NA
        ask_price1 = NA
        ask_volume1 = NA

    # Columns 21-56: BidPrice2-10, BidVolume2-10, AskPrice2-10, AskVolume2-10 — all NA
    levels_2_10 = ",".join([NA] * 36)

    # Column 57: ActionDay
    action_day = str(primary_time)[:8]

    # Column 58: Type
    type_val = "69"

    # Column 59: Seqno
    seqno_str = str(seqno)

    # Column 60: LocalTime (JST, from time field)
    local_time = parse_17digit_time(primary_time).strftime('%Y-%m-%d %H:%M:%S.%f')

    # Column 61: IntraDailyReturn
    if snapshot is not None and snapshot.preclose != 0:
        last_price_f = _safe_divide(snapshot.lastprice, snapshot.decimal) if snapshot.lastprice != 0 else 0.0
        pre_close_f = _safe_divide(snapshot.preclose, snapshot.decimal)
        intra_ret = (last_price_f - pre_close_f) / pre_close_f
        intra_ret = intra_ret + 0.0  # normalize -0.0 → 0.0
        intra_daily_return = str(intra_ret)
    else:
        intra_daily_return = NA

    # Column 62: IsShortRestricted
    if snapshot is not None:
        if snapshot.shortsellflag == 1:
            is_short = "Y"
        elif snapshot.shortsellflag == 0:
            is_short = "N"
        else:
            is_short = NA
    else:
        is_short = NA

    # Column 63-64: OpenAuctionVolume, CloseAuctionVolume
    open_auction_vol = NA
    close_auction_vol = NA

    return ",".join([
        instrument_id, trading_day, last_price, pre_settlement, pre_close,
        pre_open_interest, open_price, high_price, low_price, volume,
        turnover, open_interest, close_price, settlement, upper_limit,
        lower_limit, update_time, bid_price1, bid_volume1, ask_price1,
        ask_volume1, levels_2_10, action_day, type_val, seqno_str,
        local_time, intra_daily_return, is_short, open_auction_vol,
        close_auction_vol,
    ])


def select_tickfile_records(
    raw_records: Dict[str, List[SnapshotRecord]],
    snapshot_copy: Dict[str, SnapshotRecord],
    orders: List[OrderRecord],
    latest_order_by_symbol: Dict[str, OrderRecord],
) -> List[Tuple[str, Optional[SnapshotRecord], Optional[OrderRecord]]]:
    all_symbols = (
        set(raw_records.keys())
        | set(snapshot_copy.keys())
        | {o.symbol for o in orders}
        | set(latest_order_by_symbol.keys())
    )

    orders_by_symbol: Dict[str, OrderRecord] = {}
    for o in orders:
        existing = orders_by_symbol.get(o.symbol)
        if existing is None or (o.time, o.rcvtime) < (existing.time, existing.rcvtime):
            orders_by_symbol[o.symbol] = o

    result = []
    for symbol in all_symbols:
        snap = None
        if symbol in raw_records and raw_records[symbol]:
            recs = raw_records[symbol]
            snap = min(recs, key=lambda r: (r.time, r.rcvtime))
        elif symbol in snapshot_copy:
            snap = snapshot_copy[symbol]

        order = orders_by_symbol.get(symbol)
        if order is None:
            order = latest_order_by_symbol.get(symbol)

        if snap is None and order is None:
            continue

        result.append((symbol, snap, order))

    result.sort(key=lambda x: x[0])
    return result
