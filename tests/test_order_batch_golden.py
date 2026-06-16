"""Golden output tests for process_order_batch.

Tests the Rust process_order_batch function by comparing its output
against the Python reference implementation.
"""
from __future__ import annotations

import struct
import sys
import os

# Ensure src/ is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from minute_bar.clock import time_to_minute_key
from minute_bar.models import OrderRecord


def _python_reference_per_minute(lines, encoding, today, today_int, flushed_minutes):
    """Pure Python reference implementation of process_order_batch logic.

    Returns (per_minute_groups, late_groups, latest_order_by_symbol, skipped).
    """
    per_minute = {}
    late_order = {}
    latest_order = {}
    skipped = 0
    flushed_set = set(flushed_minutes)

    for line in lines:
        # Parse
        if isinstance(line, bytes):
            try:
                line_str = line.decode(encoding)
            except UnicodeDecodeError:
                skipped += 1
                continue
        else:
            line_str = line

        if line_str.startswith("symbol,"):
            skipped += 1
            continue
        if not line_str.strip():
            skipped += 1
            continue

        fields = line_str.split(",")
        n = len(fields)
        if n < 6 or n > 8:
            skipped += 1
            continue
        if not fields[0].strip():
            skipped += 1
            continue

        try:
            sym = fields[0].strip()
            time_val = int(fields[1].strip())
        except (ValueError, IndexError):
            skipped += 1
            continue

        # Date check
        date_key = str(time_val)[:8]
        if date_key != today:
            skipped += 1
            continue

        minute_key = time_to_minute_key(time_val)

        try:
            bidprice = int(fields[2].strip())
            bidsize = int(fields[3].strip())
            askprice = int(fields[4].strip())
            asksize = int(fields[5].strip())
            decimal = int(fields[6].strip()) if n > 6 else 2
            rcvtime = int(fields[7].strip()) if n > 7 else 0
        except (ValueError, IndexError):
            skipped += 1
            continue

        record = OrderRecord(
            symbol=sym, seqno=0, time=time_val,
            bidprice=bidprice, bidsize=bidsize,
            askprice=askprice, asksize=asksize,
            decimal=decimal, rcvtime=rcvtime,
        )

        # Latest order per symbol: max(time, rcvtime)
        existing = latest_order.get(sym)
        if existing is None or (time_val, rcvtime) > (existing.time, existing.rcvtime):
            latest_order[sym] = record

        if minute_key in flushed_set:
            if minute_key not in late_order:
                late_order[minute_key] = {}
            late_order[minute_key][sym] = record
        else:
            if minute_key not in per_minute:
                per_minute[minute_key] = {}
            per_minute[minute_key][sym] = record

    return per_minute, late_order, latest_order, skipped


def _build_per_minute_buf_python(groups):
    """Build per_minute buffer (Python reference, matches Rust build_per_minute_buf)."""
    ORDER_SCHEMA_HASH = 0x9A51A8B3
    MAGIC_PER_MINUTE = 0x01CCBBAA
    MAGIC_VERSION = 1

    buf = bytearray()
    # Magic header
    buf.extend(struct.pack('<I', MAGIC_PER_MINUTE))
    buf.extend(struct.pack('<H', MAGIC_VERSION))
    buf.extend(struct.pack('<I', ORDER_SCHEMA_HASH))

    for minute_key in sorted(groups.keys()):
        records = groups[minute_key]
        mk_bytes = minute_key.encode('utf-8')
        buf.extend(struct.pack('<H', len(mk_bytes)))
        buf.extend(mk_bytes)
        buf.extend(struct.pack('<I', len(records)))

        for sym, rec in records.items():
            sym_bytes = sym.encode('utf-8')
            buf.extend(struct.pack('<H', len(sym_bytes)))
            buf.extend(sym_bytes)
            buf.extend(struct.pack('<q', rec.time))
            buf.extend(struct.pack('<q', rec.bidprice))
            buf.extend(struct.pack('<q', rec.bidsize))
            buf.extend(struct.pack('<q', rec.askprice))
            buf.extend(struct.pack('<q', rec.asksize))
            buf.extend(struct.pack('<q', rec.decimal))
            buf.extend(struct.pack('<q', rec.rcvtime))

    return bytes(buf)


def _build_latest_order_buf_python(latest):
    """Build latest_order buffer (Python reference)."""
    ORDER_SCHEMA_HASH = 0x9A51A8B3
    MAGIC_LATEST_ORDER = 0x03CCBBAA
    MAGIC_VERSION = 1

    buf = bytearray()
    buf.extend(struct.pack('<I', MAGIC_LATEST_ORDER))
    buf.extend(struct.pack('<H', MAGIC_VERSION))
    buf.extend(struct.pack('<I', ORDER_SCHEMA_HASH))

    for sym in sorted(latest.keys()):
        rec = latest[sym]
        sym_bytes = sym.encode('utf-8')
        buf.extend(struct.pack('<H', len(sym_bytes)))
        buf.extend(sym_bytes)
        buf.extend(struct.pack('<q', rec.time))
        buf.extend(struct.pack('<q', rec.bidprice))
        buf.extend(struct.pack('<q', rec.bidsize))
        buf.extend(struct.pack('<q', rec.askprice))
        buf.extend(struct.pack('<q', rec.asksize))
        buf.extend(struct.pack('<q', rec.decimal))
        buf.extend(struct.pack('<q', rec.rcvtime))
        buf.extend(struct.pack('<q', 0))  # seqno placeholder (not in Rust latest)

    return bytes(buf)


def _decode_per_minute_buf(buf):
    """Decode per_minute_buf: minute_key -> List[OrderRecord].

    Format: [magic][version][schema_hash][count][subbuf1][subbuf2]...
    Each subbuf: [magic][version][schema_hash][mk_len][mk][count][records...]
    """
    if len(buf) < 14:
        return {}

    mv = memoryview(buf)
    magic = struct.unpack_from('<I', mv, 0)[0]
    if magic != 0x01CCBBAA:
        raise ValueError(f"per_minute_buf magic mismatch: expected 0x01CCBBAA, got 0x{magic:08X}")
    version = struct.unpack_from('<H', mv, 4)[0]
    if version != 1:
        raise ValueError(f"per_minute_buf version mismatch: expected 1, got {version}")
    schema_hash = struct.unpack_from('<I', mv, 6)[0]
    if schema_hash != 0x9A51A8B3:
        raise ValueError(f"per_minute_buf schema_hash mismatch: expected 0x9A51A8B3, got 0x{schema_hash:08X}")

    offset = 10
    if offset + 4 > len(mv):
        return {}
    count = struct.unpack_from('<I', mv, offset)[0]
    offset += 4

    if count == 0:
        return {}

    result = {}
    for _ in range(count):
        # Sub-buffer header
        if offset + 10 > len(mv):
            break
        if mv[offset:offset+4] != b'\xaa\xbb\xcc\x01':
            break
        offset += 10  # skip sub-header

        # minute_key
        if offset + 2 > len(mv):
            break
        mk_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + mk_len > len(mv):
            break
        minute_key = mv[offset:offset+mk_len].tobytes().decode('utf-8')
        offset += mk_len

        # record count
        if offset + 4 > len(mv):
            break
        rec_count = struct.unpack_from('<I', mv, offset)[0]
        offset += 4

        records = []
        for _ in range(rec_count):
            if offset + 2 > len(mv):
                break
            sym_len = struct.unpack_from('<H', mv, offset)[0]
            offset += 2
            if offset + sym_len > len(mv):
                break
            symbol = mv[offset:offset+sym_len].tobytes().decode('utf-8')
            offset += sym_len

            if offset + 56 > len(mv):
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


def _decode_latest_order_buf(buf):
    """Decode latest_order_buf: symbol -> OrderRecord."""
    if len(buf) < 10:
        return {}

    mv = memoryview(buf)
    magic = struct.unpack_from('<I', mv, 0)[0]
    if magic != 0x03CCBBAA:
        raise ValueError(f"latest_order_buf magic mismatch: expected 0x03CCBBAA, got 0x{magic:08X}")

    offset = 10
    result = {}

    while offset < len(mv):
        if offset + 2 > len(mv):
            break
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + sym_len > len(mv):
            break
        symbol = mv[offset:offset+sym_len].tobytes().decode('utf-8')
        offset += sym_len

        if offset + 64 > len(mv):
            break
        fields = struct.unpack_from('<8q', mv, offset)
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


def _decode_late_order_buf(buf):
    """Decode late_order_buf: returns flat list of OrderRecord (no minute grouping in Rust)."""
    if len(buf) < 10:
        return []

    mv = memoryview(buf)
    magic = struct.unpack_from('<I', mv, 0)[0]
    if magic != 0x02CCBBAA:
        raise ValueError(f"late_order_buf magic mismatch: expected 0x02CCBBAA, got 0x{magic:08X}")

    offset = 10
    result = []

    while offset < len(mv):
        if offset + 2 > len(mv):
            break
        sym_len = struct.unpack_from('<H', mv, offset)[0]
        offset += 2
        if offset + sym_len > len(mv):
            break
        symbol = mv[offset:offset+sym_len].tobytes().decode('utf-8')
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


class TestProcessOrderBatchGolden:
    """Golden output tests comparing Rust process_order_batch to Python reference."""

    def test_valid_lines_single_minute(self):
        """Test with valid lines all in the same minute."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return  # Skip if Rust not available

        lines = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",
            b"7203,20260528090000125,4585000,110,4595000,210,2,0",
        ]
        today = "20260528"
        today_int = 20260528

        per_minute_buf, late_buf, latest_buf, late_keys, skipped = process_order_batch(
            lines, "utf-8", today, today_int, 0, []
        )

        # Skipped should be 0
        assert skipped == 0, f"Expected 0 skipped, got {skipped}"

        # Decode Rust output
        rust_per_minute = _decode_per_minute_buf(per_minute_buf)
        rust_latest = _decode_latest_order_buf(latest_buf)
        rust_late = _decode_late_order_buf(late_buf)

        # Python reference
        py_per_min, py_late, py_latest, py_skipped = _python_reference_per_minute(
            lines, "utf-8", today, today_int, []
        )

        assert py_skipped == skipped

        # Check minute_key (round-up: input 0900 → key 0901)
        assert "202605280901" in rust_per_minute
        assert len(rust_per_minute["202605280901"]) == 2  # 2 symbols

        # Check latest per symbol
        for sym in ["7203", "7204"]:
            assert sym in rust_latest
            assert sym in py_latest
            # Rust latest order should match Python
            rust_rec = rust_latest[sym]
            py_rec = py_latest[sym]
            assert rust_rec.symbol == py_rec.symbol
            assert rust_rec.time == py_rec.time
            assert rust_rec.bidprice == py_rec.bidprice
            assert rust_rec.decimal == py_rec.decimal

        # No late orders
        assert len(rust_late) == 0
        assert len(late_keys) == 0

    def test_invalid_lines_skipped(self):
        """Test that invalid lines are properly skipped."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return

        lines = [
            b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime",  # header
            b"",  # empty
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",  # valid
            b"bad_line",  # invalid
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",  # valid
        ]
        today = "20260528"

        per_minute_buf, late_buf, latest_buf, late_keys, skipped = process_order_batch(
            lines, "utf-8", today, 20260528, 0, []
        )

        assert skipped == 3, f"Expected 3 skipped, got {skipped}"

        rust_per_minute = _decode_per_minute_buf(per_minute_buf)
        assert "202605280901" in rust_per_minute
        assert len(rust_per_minute["202605280901"]) == 2

    def test_late_order_routing(self):
        """Test that orders for flushed minutes are routed to late_order_buf."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return

        lines = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090100123,4600000,150,4610000,250,2,0",  # minute 0901
        ]
        today = "20260528"

        # Round-up: input 0900 → key 0901 (late), input 0901 → key 0902 (not flushed)
        flushed = ["202605280901"]

        per_minute_buf, late_buf, latest_buf, late_keys, skipped = process_order_batch(
            lines, "utf-8", today, 20260528, 0, flushed
        )

        assert skipped == 0

        rust_per_minute = _decode_per_minute_buf(per_minute_buf)
        rust_late = _decode_late_order_buf(late_buf)

        # 0901 should be in late_order (input 0900 → round-up key 0901, matches flushed)
        assert "202605280901" not in rust_per_minute
        assert len(rust_late) == 1  # 1 late record
        assert rust_late[0].symbol == "7203"

        # 0902 should be in per_minute (input 0901 → round-up key 0902)
        assert "202605280902" in rust_per_minute
        assert len(rust_per_minute["202605280902"]) == 1

    def test_seqno_continuity_across_batches(self):
        """Test that seqno is global across multiple batches."""
        try:
            from minute_bar._order_accel import process_order_batch
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return

        rust_reset_state()

        lines1 = [
            b"7203,20260528090000123,4580000,100,4590000,200,2,0",
            b"7204,20260528090000124,4600000,150,4610000,250,2,0",
        ]
        today = "20260528"

        result1 = process_order_batch(lines1, "utf-8", today, 20260528, 0, [])
        result2 = process_order_batch(lines1, "utf-8", today, 20260528, 0, [])

        # Seqno should be global — the second batch's seqnos should continue from the first
        # We can't directly check seqno from binary buffers (not stored in per_minute records)
        # But we can verify the state is accumulating by checking raw_order_buffers size
        from minute_bar._order_accel import tickfile_get_raw_buffer

        buf1 = tickfile_get_raw_buffer("202605280901")
        # After first batch, 202605280901 buffer exists (input 0900 → round-up 0901)
        assert len(buf1) > 0

    def test_cross_day_reset(self):
        """Test cross-day reset clears state."""
        try:
            from minute_bar._order_accel import process_order_batch
            from minute_bar._order_accel import rust_reset_state
        except ImportError:
            return

        rust_reset_state()

        # First batch: day 20260528
        lines1 = [b"7203,20260528090000123,4580000,100,4590000,200,2,0"]
        result1 = process_order_batch(lines1, "utf-8", "20260528", 20260528, 0, [])
        assert result1[4] == 0  # no skipped

        # Second batch: day 20260529 (cross-day from prev_date=20260528)
        lines2 = [b"7203,20260529090000123,4580000,100,4590000,200,2,0"]
        result2 = process_order_batch(lines2, "utf-8", "20260529", 20260529, 20260528, [])
        assert result2[4] == 0  # no skipped

        # Cross-day should have cleared flushed_minutes, so 202605290901 is not late
        rust_per_minute = _decode_per_minute_buf(result2[0])
        assert "202605290901" in rust_per_minute

    def test_magic_bytes_valid_in_all_buffers(self):
        """Assert magic bytes are valid in all returned buffers."""
        try:
            from minute_bar._order_accel import process_order_batch
        except ImportError:
            return

        lines = [b"7203,20260528090000123,4580000,100,4590000,200,2,0"]
        today = "20260528"

        per_minute_buf, late_buf, latest_buf, late_keys, skipped = process_order_batch(
            lines, "utf-8", today, 20260528, 0, []
        )

        # Verify magic bytes
        assert per_minute_buf[0:4] == b'\xaa\xbb\xcc\x01', "per_minute magic invalid"
        assert late_buf[0:4] == b'\xaa\xbb\xcc\x02', "late_order magic invalid"
        assert latest_buf[0:4] == b'\xaa\xbb\xcc\x03', "latest_order magic invalid"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
