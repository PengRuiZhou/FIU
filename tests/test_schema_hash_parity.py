"""
Schema hash parity test for Phase 21 Rust buffers.

This test verifies that CRC32 values of field-layout strings match between
Python and Rust implementations. Any change to buffer schemas must update
the expected CRC32 values here, otherwise CI will fail and catch schema drift.

Schema hash algorithm: CRC32 of ASCII concatenation of "{name}:{type}," for each
field in order, INCLUDING trailing comma after the last field.
"""

import zlib


# Field-layout schema strings (must match Rust exactly)
ORDER_SCHEMA = "symbol:str,time:i64,bidprice:i64,bidsize:i64,askprice:i64,asksize:i64,decimal:i64,rcvtime:i64,"
SNAPSHOT_SCHEMA = "symbol:str,time:i64,preclose:i64,lastprice:i64,lasttradeqty:i64,totalvol:i64,totalamount:i64,sessionid:i64,tradetype:str,status:str,direction:i64,pflag:str,decimal:i64,vwap:i64,shortsellflag:i64,rcvtime:i64,"
OHLCV_SCHEMA = "symbol:str,open:f64,high:f64,low:f64,close:f64,volume:i64,count:i64,decimal:i64,"
LATEST_SNAPSHOT_SCHEMA = "symbol:str,time:i64,preclose:f64,lastprice:f64,open:f64,high:f64,low:f64,close:f64,totalvol:i64,totalamount:f64,decimal:i64,"

# Verified CRC32 values (computed via Python zlib.crc32 on 2026-06-12)
# These must match Rust crc32fast::hash of the same schema strings
ORDER_CRC32 = 0x9A51A8B3
SNAPSHOT_CRC32 = 0x2E91C449
OHLCV_CRC32 = 0xC62F76E5
LATEST_SNAPSHOT_CRC32 = 0x2DD0CCC2


def compute_crc32(schema: str) -> int:
    """Compute CRC32 of a schema string using zlib.crc32."""
    return zlib.crc32(schema.encode('utf-8')) & 0xFFFFFFFF


def test_order_schema_hash():
    """Verify order record schema CRC32."""
    actual = compute_crc32(ORDER_SCHEMA)
    assert actual == ORDER_CRC32, (
        f"Order schema hash mismatch: got {hex(actual)}, "
        f"expected {hex(ORDER_CRC32)}"
    )


def test_snapshot_schema_hash():
    """Verify snapshot record schema CRC32."""
    actual = compute_crc32(SNAPSHOT_SCHEMA)
    assert actual == SNAPSHOT_CRC32, (
        f"Snapshot schema hash mismatch: got {hex(actual)}, "
        f"expected {hex(SNAPSHOT_CRC32)}"
    )


def test_ohlcv_schema_hash():
    """Verify OHLCV entry schema CRC32."""
    actual = compute_crc32(OHLCV_SCHEMA)
    assert actual == OHLCV_CRC32, (
        f"OHLCV schema hash mismatch: got {hex(actual)}, "
        f"expected {hex(OHLCV_CRC32)}"
    )


def test_latest_snapshot_schema_hash():
    """Verify latest snapshot schema CRC32."""
    actual = compute_crc32(LATEST_SNAPSHOT_SCHEMA)
    assert actual == LATEST_SNAPSHOT_CRC32, (
        f"Latest snapshot schema hash mismatch: got {hex(actual)}, "
        f"expected {hex(LATEST_SNAPSHOT_CRC32)}"
    )


def test_all_schemas():
    """Print all computed CRC32 values for verification."""
    schemas = [
        ("order", ORDER_SCHEMA, ORDER_CRC32),
        ("snapshot", SNAPSHOT_SCHEMA, SNAPSHOT_CRC32),
        ("ohlcv", OHLCV_SCHEMA, OHLCV_CRC32),
        ("latest_snapshot", LATEST_SNAPSHOT_SCHEMA, LATEST_SNAPSHOT_CRC32),
    ]

    print("\n=== Schema Hash Parity ===")
    for name, schema, expected in schemas:
        actual = compute_crc32(schema)
        status = "PASS" if actual == expected else "FAIL"
        print(f"{name}: {hex(actual)} [{status}]")

    # This test always passes - it's just for reporting
    assert True
