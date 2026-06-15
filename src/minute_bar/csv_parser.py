from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from minute_bar.clock import current_system_timestamp_17digit
from minute_bar.models import OrderRecord

logger = logging.getLogger(__name__)

SNAPSHOT_MIN_COLS = 17
SNAPSHOT_MAX_COLS = 21
CODE_MIN_COLS = 7
CODE_MAX_COLS = 17
ORDER_MIN_COLS = 6
ORDER_MAX_COLS = 8

# ── Rust order acceleration import with fallback ──
_RUST_ACCEL_LOADED = False
_RUST_ACCEL_AVAILABLE = False

try:
    from minute_bar._order_accel import parse_order_batch as _rust_parse_batch
    from minute_bar._order_accel import parse_order_batch_flat as _rust_parse_batch_flat
    from minute_bar._order_accel import is_available as _rust_available
    _RUST_ACCEL_LOADED = True
    _RUST_ACCEL_AVAILABLE = _rust_available()
except (ImportError, AttributeError, RuntimeError, OSError) as e:
    _RUST_ACCEL_AVAILABLE = False
    logger.warning("Rust order accel not available, using Python fallback: %s: %s", type(e).__name__, e)


# ── Phase 21 Rust imports ──
_RUST_PHASE21_LOADED = False
_RUST_PHASE21_AVAILABLE = False

try:
    from minute_bar._order_accel import (
        process_order_batch,
        parse_snapshot_batch,
        aggregate_snapshot_batch,
        tickfile_generate,
        tickfile_get_raw_buffer,
        tickfile_get_latest_snapshot,
        rust_reset_state,
        rust_reset_snapshot_state,
        is_available as _rust_phase21_available,
    )
    _RUST_PHASE21_LOADED = True
    # Phase 21 is available only if the is_available self-test passes
    _RUST_PHASE21_AVAILABLE = _rust_phase21_available()
except (ImportError, AttributeError, RuntimeError, OSError) as e:
    _RUST_PHASE21_AVAILABLE = False
    logger.warning("Phase 21 Rust functions not available: %s: %s", type(e).__name__, e)


def has_rust_accel() -> bool:
    """Check if Rust acceleration is available AND self-test passed."""
    return _RUST_ACCEL_AVAILABLE


def use_rust_accel(config=None) -> bool:
    """Check if Rust acceleration should be used based on availability and config."""
    if not _RUST_ACCEL_AVAILABLE:
        return False
    if config is not None and not getattr(config.input, 'enable_order_accel', False):
        return False
    return True


def set_rust_available(value: bool) -> None:
    """Update Rust availability flag (used by warmup self-test on failure)."""
    global _RUST_ACCEL_AVAILABLE
    _RUST_ACCEL_AVAILABLE = value


# ── Flat binary decoder for Rust parse_order_batch_flat ──
import struct

def decode_flat_batch(buf: bytes) -> list:
    """Decode flat binary buffer from Rust parse_order_batch_flat.
    Format per record: u16 LE symbol_len + symbol_bytes + 7 × i64 LE.
    Returns list of (symbol, time, bidprice, bidsize, askprice, asksize, decimal, rcvtime).
    Raises struct.error on corruption — caller should fall back to Python per-line parsing.
    """
    mv = memoryview(buf)
    offset = 0
    results = []
    end = len(mv)
    while offset < end:
        # IMPORTANT: '<H' (unsigned short) matches Rust u16 — NOT '<h' (signed)
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
        offset += sym_len
        fields = struct.unpack_from('<7q', mv, offset)  # 7 × signed 64-bit LE
        offset += 56  # 7 * 8 bytes
        results.append((symbol, *fields))
    return results


# ── Phase 21 Flat Binary Decoders ──────────────────────────────────────────────

# Magic bytes and schema hashes for Phase 21 buffers
_ORDER_SCHEMA_HASH = 0x9A51A8B3
_SNAPSHOT_SCHEMA_HASH = 0x2E91C449
_OHLCV_SCHEMA_HASH = 0xC62F76E5
_LATEST_SNAPSHOT_SCHEMA_HASH = 0x2DD0CCC2
_MAGIC_PER_MINUTE = b'\xaa\xbb\xcc\x01'
_MAGIC_LATE_ORDER = b'\xaa\xbb\xcc\x02'
_MAGIC_LATEST_ORDER = b'\xaa\xbb\xcc\x03'
_MAGIC_OHLCV = b'\xaa\xbb\xcc\x04'
_MAGIC_LATEST_SNAPSHOT = b'\xaa\xbb\xcc\x05'


def _validate_magic_header(buf: bytes, expected_magic: bytes, expected_hash: int) -> memoryview:
    """Validate magic bytes and schema_hash in buffer header.

    Format: [4 bytes magic][2 bytes version][4 bytes schema_hash][...]
    Returns memoryview offset to after header (position 10).
    Raises ValueError on mismatch.
    """
    if len(buf) < 10:
        raise ValueError(f"Buffer too short: {len(buf)} bytes (need >= 10)")

    mv = memoryview(buf)

    magic = mv[0:4]
    if magic != expected_magic:
        raise ValueError(
            f"Magic mismatch: expected {expected_magic.hex()}, got {magic.hex()}"
        )

    version = struct.unpack_from('<H', mv, 4)[0]
    if version != 1:
        raise ValueError(f"Version mismatch: expected 1, got {version}")

    schema_hash = struct.unpack_from('<I', mv, 6)[0]
    if schema_hash != expected_hash:
        raise ValueError(
            f"Schema hash mismatch: expected 0x{expected_hash:08X}, got 0x{schema_hash:08X}"
        )

    return mv[10:]


def decode_order_per_minute_buf(buf: bytes) -> dict[str, list[OrderRecord]]:
    """Decode per_minute_buf: minute_key → List[OrderRecord].

    Buffer format:
      [magic][version][schema_hash] = 10 bytes
      [count u32] = number of per-minute buffers
      Then for each minute buffer:
        [magic][version][schema_hash] = 10 bytes (sub-header)
        [2 mk_len][mk_bytes][4 record_count]
        Then record_count ×:
          [2 sym_len][sym_bytes][8 time][8 bidprice][8 bidsize]
          [8 askprice][8 asksize][8 decimal][8 rcvtime]
    """
    if len(buf) == 0:
        return {}

    mv = memoryview(buf)

    # Validate outer header
    if mv[0:4] != _MAGIC_PER_MINUTE:
        raise ValueError(f"per_minute_buf magic mismatch: expected {_MAGIC_PER_MINUTE.hex()}, got {mv[0:4].hex()}")
    if struct.unpack_from('<H', mv, 4)[0] != 1:
        raise ValueError("Version mismatch")
    if struct.unpack_from('<I', mv, 6)[0] != _ORDER_SCHEMA_HASH:
        raise ValueError(f"Schema hash mismatch: expected 0x{_ORDER_SCHEMA_HASH:08X}")

    # Read count of minute buffers
    offset = 10
    if offset + 4 > len(mv):
        return {}
    count = struct.unpack_from('<I', mv, offset)[0]
    offset += 4

    if count == 0:
        return {}

    result: dict[str, list[OrderRecord]] = {}

    for _ in range(count):
        # Validate sub-header
        if offset + 10 > len(mv):
            break
        if mv[offset:offset+4] != _MAGIC_PER_MINUTE:
            break
        offset += 10  # skip sub-header (magic + version + schema_hash)

        # minute_key
        if offset + 2 > len(mv):
            break
        mk_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + mk_len > len(mv):
            break
        minute_key = mv[offset:offset + mk_len].tobytes().decode('utf-8')
        offset += mk_len

        # record count
        if offset + 4 > len(mv):
            break
        rec_count = struct.unpack_from('<I', mv, offset)[0]
        offset += 4

        records: list[OrderRecord] = []
        for _ in range(rec_count):
            if offset + 2 > len(mv):
                break
            sym_len = struct.unpack_from('<H', mv, offset)[0]
            offset += 2
            if offset + sym_len > len(mv):
                break
            symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
            offset += sym_len

            if offset + 56 > len(mv):  # 7 * 8 = 56
                break
            fields = struct.unpack_from('<7q', mv, offset)
            offset += 56

            records.append(OrderRecord(
                symbol=symbol,
                seqno=0,
                time=fields[0],
                bidprice=fields[1],
                bidsize=fields[2],
                askprice=fields[3],
                asksize=fields[4],
                decimal=fields[5],
                rcvtime=fields[6],
            ))

        result[minute_key] = records

    return result


def decode_late_order_buf(buf: bytes) -> list[OrderRecord]:
    """Decode late_order_buf: returns flat list of OrderRecord.

    Buffer format:
      [4 magic][2 version][4 schema_hash] = 10 bytes header
      Then record_count ×:
        [2 sym_len][sym_bytes][8 time][8 bidprice][8 bidsize]
        [8 askprice][8 asksize][8 decimal][8 rcvtime]
    """
    if len(buf) == 0:
        return []

    mv = _validate_magic_header(buf, _MAGIC_LATE_ORDER, _ORDER_SCHEMA_HASH)

    result: list[OrderRecord] = []
    offset = 0

    while offset < len(mv):
        if offset + 2 > len(mv):
            break
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + sym_len > len(mv):
            break
        symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
        offset += sym_len

        if offset + 56 > len(mv):
            break
        fields = struct.unpack_from('<7q', mv, offset)
        offset += 56

        result.append(OrderRecord(
            symbol=symbol,
            seqno=0,
            time=fields[0],
            bidprice=fields[1],
            bidsize=fields[2],
            askprice=fields[3],
            asksize=fields[4],
            decimal=fields[5],
            rcvtime=fields[6],
        ))

    return result


def decode_latest_order_buf(buf: bytes) -> dict[str, OrderRecord]:
    """Decode latest_order_buf: symbol → OrderRecord.

    Buffer format:
      [4 magic][2 version][4 schema_hash] = 10 bytes header
      Then record_count ×:
        [2 sym_len][sym_bytes][8 time][8 bidprice][8 bidsize]
        [8 askprice][8 asksize][8 decimal][8 rcvtime][8 seqno]
    """
    if len(buf) == 0:
        return {}

    mv = _validate_magic_header(buf, _MAGIC_LATEST_ORDER, _ORDER_SCHEMA_HASH)

    result: dict[str, OrderRecord] = {}
    offset = 0

    while offset < len(mv):
        if offset + 2 > len(mv):
            break
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + sym_len > len(mv):
            break
        symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
        offset += sym_len

        if offset + 64 > len(mv):  # 8 * 8 = 64
            break
        fields = struct.unpack_from('<8q', mv, offset)  # time, bidprice, bidsize, askprice, asksize, decimal, rcvtime, seqno
        offset += 64

        result[symbol] = OrderRecord(
            symbol=symbol,
            seqno=fields[7],
            time=fields[0],
            bidprice=fields[1],
            bidsize=fields[2],
            askprice=fields[3],
            asksize=fields[4],
            decimal=fields[5],
            rcvtime=fields[6],
        )

    return result


def decode_ohlcv_buf(buf: bytes) -> dict[str, list[dict]]:
    """Decode ohlcv_buf: minute_key → List[OHLCVEntry dicts].

    Buffer format:
      [4 magic][2 version][4 schema_hash] = 10 bytes header
      Then for each (minute_key, symbol) pair:
        [2 mk_len][mk_bytes][2 sym_len][sym_bytes]
        [8 open][8 high][8 low][8 close][8 volume]
        [4 count][4 decimal]

    Returns dict of minute_key → list of entry dicts with keys:
      symbol, open, high, low, close, volume, count, decimal
    """
    if len(buf) == 0:
        return {}

    mv = _validate_magic_header(buf, _MAGIC_OHLCV, _OHLCV_SCHEMA_HASH)

    result: dict[str, list[dict]] = {}
    offset = 0

    while offset < len(mv):
        # Peek at next section to detect end of buffer
        if offset + 2 > len(mv):
            break

        # Check if this might be a new minute_key (starts with digit chars)
        next_len = struct.unpack_from('<H', mv, offset)[0]
        if offset + 2 + next_len <= len(mv):
            potential = mv[offset + 2:offset + 2 + next_len].tobytes()
            if len(potential) == next_len and potential.decode('utf-8', errors='replace').isdigit():
                # This is a new minute_key entry
                pass
            else:
                break
        else:
            break

        # minute_key
        mk_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + mk_len > len(mv):
            break
        minute_key = mv[offset:offset + mk_len].tobytes().decode('utf-8')
        offset += mk_len

        if minute_key not in result:
            result[minute_key] = []

        # Read entries for this minute until we hit the next minute_key or end
        while offset < len(mv):
            # Peek: check if next is another minute_key
            if offset + 2 > len(mv):
                break

            next_len = struct.unpack_from('<H', mv, offset)[0]
            if offset + 2 + next_len <= len(mv):
                potential = mv[offset + 2:offset + 2 + next_len].tobytes()
                if len(potential) == next_len and potential.decode('utf-8', errors='replace').isdigit():
                    # Next minute_key — break out to outer loop
                    break

            # symbol
            if offset + 2 > len(mv):
                break
            sym_len = struct.unpack_from('<H', mv, offset)[0]
            offset += 2
            if offset + sym_len > len(mv):
                break
            symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
            offset += sym_len

            if offset + 48 > len(mv):  # 4*8 + 8 + 4 + 4 = 48 bytes
                break

            open_, high, low, close = struct.unpack_from('<4d', mv, offset)
            offset += 32
            volume = struct.unpack_from('<q', mv, offset)[0]  # i64
            offset += 8
            count = struct.unpack_from('<I', mv, offset)[0]
            offset += 4
            decimal = struct.unpack_from('<I', mv, offset)[0]
            offset += 4

            result[minute_key].append({
                'symbol': symbol,
                'open': open_,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'count': count,
                'decimal': decimal,
            })

    return result


def decode_snapshot_buf(buf: bytes) -> dict[str, dict]:
    """Decode latest_snapshot_buf: symbol → SnapshotData dict.

    Buffer format:
      [4 magic][2 version][4 schema_hash][4 entry_count] = 14 bytes header
      Then entry_count ×:
        [2 sym_len][sym_bytes][8 time][8 preclose][8 lastprice]
        [8 open][8 high][8 low][8 close][8 totalvol]
        [8 totalamount][4 decimal]
    """
    if len(buf) == 0:
        return {}

    if len(buf) < 14:
        raise ValueError(f"snapshot_buf too short: {len(buf)} bytes (need >= 14)")

    mv = memoryview(buf)

    magic = mv[0:4]
    if magic != _MAGIC_LATEST_SNAPSHOT:
        raise ValueError(
            f"snapshot_buf magic mismatch: expected {_MAGIC_LATEST_SNAPSHOT.hex()}, got {magic.hex()}"
        )

    version = struct.unpack_from('<H', mv, 4)[0]
    if version != 1:
        raise ValueError(f"Version mismatch: expected 1, got {version}")

    schema_hash = struct.unpack_from('<I', mv, 6)[0]
    if schema_hash != _LATEST_SNAPSHOT_SCHEMA_HASH:
        raise ValueError(
            f"Schema hash mismatch: expected 0x{_LATEST_SNAPSHOT_SCHEMA_HASH:08X}, got 0x{schema_hash:08X}"
        )

    entry_count = struct.unpack_from('<I', mv, 10)[0]
    offset = 14

    result: dict[str, dict] = {}

    for _ in range(entry_count):
        if offset + 2 > len(mv):
            break
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + sym_len > len(mv):
            break
        symbol = mv[offset:offset + sym_len].tobytes().decode('utf-8')
        offset += sym_len

        if offset + 76 > len(mv):  # 1*i64 + 6*f64 + i64 + f64 + u32 = 8+48+8+8+4 = 76
            break

        # Layout: time(i64) + 6 f64 + totalvol(i64) + totalamount(f64) + decimal(u32)
        time_ = struct.unpack_from('<q', mv, offset)[0]  # i64
        offset += 8
        preclose, lastprice, open_, high, low, close_ = struct.unpack_from('<6d', mv, offset)
        offset += 48
        totalvol = struct.unpack_from('<q', mv, offset)[0]  # i64
        offset += 8
        totalamount = struct.unpack_from('<d', mv, offset)[0]  # f64
        offset += 8
        decimal = struct.unpack_from('<I', mv, offset)[0]
        offset += 4

        result[symbol] = {
            'symbol': symbol,
            'time': time_,
            'preclose': preclose,
            'lastprice': lastprice,
            'open': open_,
            'high': high,
            'low': low,
            'close': close_,
            'totalvol': totalvol,
            'totalamount': totalamount,
            'decimal': decimal,
        }

    return result


def phase21_available() -> bool:
    """Check if Phase 21 Rust functions are available and functional."""
    return _RUST_PHASE21_AVAILABLE


def use_phase21_order_batch(config=None) -> bool:
    """Check if Phase 21 process_order_batch should be used."""
    if not _RUST_PHASE21_AVAILABLE:
        return False
    if config is not None:
        enable_flag = getattr(config.input, 'enable_rust_order_full_batch', False)
        if not enable_flag:
            return False
    return True


def use_phase21_snapshot_batch(config=None) -> bool:
    """Check if Phase 21 aggregate_snapshot_batch should be used."""
    if not _RUST_PHASE21_AVAILABLE:
        return False
    if config is not None:
        enable_flag = getattr(config.input, 'enable_rust_snapshot_batch', False)
        if not enable_flag:
            return False
    return True


def use_phase21_tickfile(config=None) -> bool:
    """Check if Phase 21 tickfile_generate should be used."""
    if not _RUST_PHASE21_AVAILABLE:
        return False
    if config is not None:
        enable_flag = getattr(config.input, 'enable_rust_tickfile', False)
        if not enable_flag:
            return False
    return True


@dataclass
class ParsedSnapshot:
    symbol: str
    time: int
    preclose: int
    lastprice: int
    open: int
    high: int
    low: int
    close: int
    lasttradeprice: int
    lasttradeqty: int
    totalvol: int
    totalamount: int
    sessionid: int
    tradetype: str
    status: str
    direction: int
    pflag: str
    decimal: int
    vwap: int
    shortsellflag: int
    rcvtime: int


@dataclass
class ParsedCode:
    symbol: str
    market: int
    marketdesc: str
    name: str
    money: str
    type: str
    subtype: str
    issueclass: str
    industrycode: str
    isincode: str
    lotsize: int
    limitup: int
    limitdown: int
    decimal: int
    rcvtime: int
    businessday: str
    baseprice: int


def parse_snapshot_line(line_bytes: bytes, encoding: str = "utf-8") -> Optional[ParsedSnapshot]:
    try:
        line = line_bytes.decode(encoding)
    except UnicodeDecodeError:
        logger.error("Failed to decode snapshot line: %r", line_bytes[:100])
        return None

    if line.startswith("symbol,"):
        return None

    fields = line.split(",")
    n = len(fields)

    if n < SNAPSHOT_MIN_COLS:
        logger.error("Snapshot line has %d columns, need >= %d: %s", n, SNAPSHOT_MIN_COLS, line[:200])
        return None
    if n > SNAPSHOT_MAX_COLS:
        logger.error("Snapshot line has %d columns, max %d: %s", n, SNAPSHOT_MAX_COLS, line[:200])
        return None

    try:
        symbol = fields[0].strip()
        if not symbol:
            logger.error("Snapshot symbol is empty")
            return None

        time_ = int(fields[1].strip())
        preclose = int(fields[2].strip())
        lastprice = int(fields[3].strip())
        open_ = int(fields[4].strip())
        high = int(fields[5].strip())
        low = int(fields[6].strip())
        close = int(fields[7].strip())
        lasttradeprice = int(fields[8].strip())
        lasttradeqty = int(fields[9].strip())
        totalvol = int(fields[10].strip())
        totalamount = int(fields[11].strip())
        sessionid = int(fields[12].strip())
        tradetype = fields[13].strip() if n > 13 else ""
        status = fields[14].strip() if n > 14 else "T"
        direction = int(fields[15].strip()) if n > 15 else 0
        pflag = fields[16].strip() if n > 16 else "N"
        decimal = int(fields[17].strip()) if n > 17 else 2
        vwap = int(fields[18].strip()) if n > 18 else 0
        shortsellflag = int(fields[19].strip()) if n > 19 else 0
        rcvtime = int(fields[20].strip()) if n > 20 else current_system_timestamp_17digit()
    except (ValueError, IndexError) as e:
        logger.error("Failed to parse snapshot fields: %s, line: %s", e, line[:200])
        return None

    return ParsedSnapshot(
        symbol=symbol, time=time_, preclose=preclose, lastprice=lastprice,
        open=open_, high=high, low=low, close=close,
        lasttradeprice=lasttradeprice, lasttradeqty=lasttradeqty,
        totalvol=totalvol, totalamount=totalamount, sessionid=sessionid,
        tradetype=tradetype, status=status, direction=direction, pflag=pflag,
        decimal=decimal, vwap=vwap, shortsellflag=shortsellflag, rcvtime=rcvtime,
    )


def parse_code_line(line_bytes: bytes, encoding: str = "utf-8") -> Optional[ParsedCode]:
    try:
        line = line_bytes.decode(encoding)
    except UnicodeDecodeError:
        logger.error("Failed to decode code line: %r", line_bytes[:100])
        return None

    if line.startswith("symbol,"):
        return None

    fields = line.split(",")
    n = len(fields)

    if n < CODE_MIN_COLS:
        logger.error("Code line has %d columns, need >= %d: %s", n, CODE_MIN_COLS, line[:200])
        return None
    if n > CODE_MAX_COLS:
        logger.error("Code line has %d columns, max %d: %s", n, CODE_MAX_COLS, line[:200])
        return None

    try:
        symbol = fields[0].strip()
        market = int(fields[1].strip())
        marketdesc = fields[2].strip() if n > 2 else ""
        name = fields[3].strip() if n > 3 else ""
        money = fields[4].strip() if n > 4 else ""
        type_ = fields[5].strip() if n > 5 else ""
        subtype = fields[6].strip() if n > 6 else ""
        issueclass = fields[7].strip() if n > 7 else ""
        industrycode = fields[8].strip() if n > 8 else ""
        isincode = fields[9].strip() if n > 9 else ""
        lotsize = int(fields[10].strip()) if n > 10 and fields[10].strip() else 0
        limitup = int(fields[11].strip()) if n > 11 and fields[11].strip() else 0
        limitdown = int(fields[12].strip()) if n > 12 and fields[12].strip() else 0
        decimal = int(fields[13].strip()) if n > 13 and fields[13].strip() else 2
        rcvtime = int(fields[14].strip()) if n > 14 and fields[14].strip() else 0
        businessday = fields[15].strip() if n > 15 else ""
        baseprice = int(fields[16].strip()) if n > 16 and fields[16].strip() else 0
    except (ValueError, IndexError) as e:
        logger.error("Failed to parse code fields: %s, line: %s", e, line[:200])
        return None

    return ParsedCode(
        symbol=symbol, market=market, marketdesc=marketdesc, name=name,
        money=money, type=type_, subtype=subtype, issueclass=issueclass,
        industrycode=industrycode, isincode=isincode, lotsize=lotsize,
        limitup=limitup, limitdown=limitdown, decimal=decimal,
        rcvtime=rcvtime, businessday=businessday, baseprice=baseprice,
    )


@dataclass
class ParsedOrder:
    symbol: str
    time: int
    bidprice: int
    bidsize: int
    askprice: int
    asksize: int
    decimal: int
    rcvtime: int


def parse_order_line(line_bytes: bytes, encoding: str = "utf-8") -> Optional[ParsedOrder]:
    try:
        line = line_bytes.decode(encoding)
    except UnicodeDecodeError:
        logger.error("Failed to decode order line: %r", line_bytes[:100])
        return None

    if line.startswith("symbol,"):
        return None

    fields = line.split(",")
    n = len(fields)

    if n < ORDER_MIN_COLS:
        logger.error("Order line has %d columns, need >= %d: %s", n, ORDER_MIN_COLS, line[:200])
        return None
    if n > ORDER_MAX_COLS:
        logger.error("Order line has %d columns, max %d: %s", n, ORDER_MAX_COLS, line[:200])
        return None

    try:
        symbol = fields[0].strip()
        if not symbol:
            logger.error("Order symbol is empty")
            return None

        time_ = int(fields[1].strip())
        bidprice = int(fields[2].strip())
        bidsize = int(fields[3].strip())
        askprice = int(fields[4].strip())
        asksize = int(fields[5].strip())
        decimal = int(fields[6].strip()) if n > 6 else 2
        rcvtime = int(fields[7].strip()) if n > 7 else 0
    except (ValueError, IndexError) as e:
        logger.error("Failed to parse order fields: %s, line: %s", e, line[:200])
        return None

    return ParsedOrder(
        symbol=symbol, time=time_, bidprice=bidprice, bidsize=bidsize,
        askprice=askprice, asksize=asksize, decimal=decimal, rcvtime=rcvtime,
    )


def parse_order_record(
    line_bytes: bytes | str,
    seqno: int,
    encoding: str = "utf-8",
) -> Optional[OrderRecord]:
    """Fast path: bytes → OrderRecord in one pass. No strip(), no ParsedOrder.

    This is the hot-path for the order thread — every microsecond counts.
    Source CSV has no whitespace in fields, so strip() is unnecessary.
    Accepts both bytes (production) and str (test mocks).
    """
    if isinstance(line_bytes, str):
        line = line_bytes
    else:
        try:
            line = line_bytes.decode(encoding)
        except UnicodeDecodeError:
            return None

    if line.startswith("symbol,"):
        return None

    fields = line.split(",")
    n = len(fields)

    if n < ORDER_MIN_COLS or n > ORDER_MAX_COLS:
        return None

    # Empty/whitespace-only symbol check — matches parse_order_line (slow path).
    # This was a pre-existing bug: slow path rejected empty symbols, hot path did not.
    if not fields[0].strip():
        return None

    try:
        return OrderRecord(
            symbol=fields[0],
            seqno=seqno,
            time=int(fields[1]),
            bidprice=int(fields[2]),
            bidsize=int(fields[3]),
            askprice=int(fields[4]),
            asksize=int(fields[5]),
            decimal=int(fields[6]) if n > 6 else 2,
            rcvtime=int(fields[7]) if n > 7 else 0,
        )
    except (ValueError, IndexError):
        return None
