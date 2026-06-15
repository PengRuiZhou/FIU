from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple


@dataclass(frozen=True)
class SnapshotRecord:
    symbol: str
    seqno: int
    time: int
    rcvtime: int
    preclose: float
    lastprice: float
    open: float
    high: float
    low: float
    close: float
    lasttradeprice: float
    lasttradeqty: int
    totalvol: int
    totalamount: float
    sessionid: int
    tradetype: str
    status: str
    direction: int
    pflag: str
    decimal: int
    vwap: float
    shortsellflag: int


@dataclass
class OHLCVAggregate:
    symbol: str
    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    volume: int = 0
    amount: float = 0.0
    count: int = 0
    start_totalvol: int = 0
    start_totalamount: float = 0.0
    end_totalvol: int = 0
    end_totalamount: float = 0.0
    any_lasttradeqty_positive: bool = False
    seqno: int = 0
    decimal: int = 2

    def update(self, record: SnapshotRecord, base_vol: int, base_amt: float) -> None:
        d = 10 ** record.decimal if record.decimal > 0 else 1
        price = record.lastprice / d
        if self.count == 0:
            self.open = price
            self.start_totalvol = record.totalvol
            self.start_totalamount = record.totalamount
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.end_totalvol = record.totalvol
        self.end_totalamount = record.totalamount
        self.count += 1
        self.seqno = record.seqno
        self.decimal = record.decimal
        if record.lasttradeqty > 0:
            self.any_lasttradeqty_positive = True
        self.volume = max(0, self.end_totalvol - base_vol)
        self.amount = max(0.0, (self.end_totalamount - base_amt) / d)


class OrderRecord(NamedTuple):
    symbol: str
    seqno: int
    time: int
    bidprice: int
    bidsize: int
    askprice: int
    asksize: int
    decimal: int
    rcvtime: int


@dataclass
class FileState:
    offset: int = 0
    pending_line: bytes = b""
    date: str = ""
