"""Validate Rust parse_order_batch produces identical results to Python parse_order_record."""

import pytest
from minute_bar.csv_parser import parse_order_record, use_rust_accel

# Realistic test data — varied symbols, 6-col and 8-col, edge cases
SAMPLE_LINES = [
    b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime",  # header
    b"7203,20260528090000123,4580000,100,4590000,200,2,20260528090000100",
    b"6501,20260528090000456,2345000,500,2350000,300,2,20260528090000400",
    b"9984,20260528090000789,8900000,200,8910000,100,2,20260528090000700",
    b"6758,20260528090001000,12340000,50,12350000,75,2,20260528090000900",
    b"8306,20260528090001234,1500000,1000,1510000,800,2,0",  # rcvtime=0
    b"7203,20260528090001500,4585000,300,4595000,400,2,20260528090001400",
    b"",  # empty line
    b"6501,20260528090001789,2350000,100,2360000,200,2,20260528090001700",
    b"invalid_line",  # should be skipped
    b"7203,20260528090002000,4590000,150,4600000,250,2,20260528090001900",
    b"9984,20260528090002222,8920000,300,8930000,200,2,20260528090002100",
    b",20260528090003000,1000000,500,1010000,400",  # empty symbol → skip
    b"7203,20260529090000123,4580000,100,4590000,200,2,20260529090000100",  # cross-day
]

# today_int for parity test (matches 20260528)
TODAY_INT = 20260528


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_rust_matches_python_field_by_field():
    """Rust parse_order_batch must produce identical field values to Python parse_order_record."""
    from minute_bar._order_accel import parse_order_batch

    rust_batch, rust_skipped = parse_order_batch(SAMPLE_LINES, "utf-8")

    # Python path — simulate engine seqno assignment
    py_records = []
    for line in SAMPLE_LINES:
        r = parse_order_record(line, 0, "utf-8")  # seqno=0 placeholder
        if r is not None:
            py_records.append(r)

    # Count must match
    assert len(rust_batch) == len(py_records), (
        f"Record count mismatch: Rust={len(rust_batch)}, Python={len(py_records)}"
    )

    # Field-by-field comparison
    for i, (rust_fields, py_rec) in enumerate(zip(rust_batch, py_records)):
        assert rust_fields[0] == py_rec.symbol, f"Row {i}: symbol mismatch"
        assert rust_fields[1] == py_rec.time, f"Row {i}: time mismatch"
        assert rust_fields[2] == py_rec.bidprice, f"Row {i}: bidprice mismatch"
        assert rust_fields[3] == py_rec.bidsize, f"Row {i}: bidsize mismatch"
        assert rust_fields[4] == py_rec.askprice, f"Row {i}: askprice mismatch"
        assert rust_fields[5] == py_rec.asksize, f"Row {i}: asksize mismatch"
        assert rust_fields[6] == py_rec.decimal, f"Row {i}: decimal mismatch"
        assert rust_fields[7] == py_rec.rcvtime, f"Row {i}: rcvtime mismatch"


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_seqno_assigned_after_date_check():
    """Verify seqno increments only AFTER date check passes, matching engine.py."""
    from minute_bar._order_accel import parse_order_batch
    from minute_bar.models import OrderRecord

    rust_batch, _ = parse_order_batch(SAMPLE_LINES, "utf-8")

    # Simulate engine.py seqno assignment with date filter
    seqno = 0
    valid_records = []
    for fields in rust_batch:
        # Date check BEFORE seqno increment — exactly like engine.py
        record_date = fields[1] // 1_000_000_000  # fields[1] = time
        if record_date != TODAY_INT:
            continue  # skip cross-day record WITHOUT incrementing seqno
        seqno += 1
        record = OrderRecord(
            symbol=fields[0], seqno=seqno, time=fields[1],
            bidprice=fields[2], bidsize=fields[3],
            askprice=fields[4], asksize=fields[5],
            decimal=fields[6], rcvtime=fields[7],
        )
        valid_records.append(record)

    # Seqnos should be 1, 2, 3, ... (dense, monotonically increasing)
    seqnos = [r.seqno for r in valid_records]
    assert seqnos == list(range(1, len(valid_records) + 1))

    # Cross-day record (20260529) should NOT be in valid_records
    for r in valid_records:
        assert r.time // 1_000_000_000 == TODAY_INT


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_crlf_handling():
    """CRLF-terminated lines must parse identically to LF-terminated lines."""
    from minute_bar._order_accel import parse_order_batch

    lf_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    crlf_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r\n"
    cr_only_line = b"7203,20260528090000123,4580000,100,4590000,200,2,0\r"

    lf_result, _ = parse_order_batch([lf_line], "utf-8")
    crlf_result, _ = parse_order_batch([crlf_line], "utf-8")
    cr_result, _ = parse_order_batch([cr_only_line], "utf-8")

    assert len(lf_result) == 1 and len(crlf_result) == 1 and len(cr_result) == 1
    assert lf_result[0] == crlf_result[0] == cr_result[0]


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_order_csv_byte_identical():
    """Full-scale parity: Rust and Python paths produce byte-identical CSV output."""
    from minute_bar._order_accel import parse_order_batch
    from minute_bar.models import OrderRecord
    from minute_bar.writer import _format_order_row

    # Generate 10,000 varied lines including cross-day boundary
    lines = []
    symbols = [b"7203", b"6501", b"9984", b"6758", b"8306", b"7974"]
    for i in range(10000):
        sym = symbols[i % len(symbols)]
        # Most records use today's date (20260528)
        time_val = 20260528090000000 + (i * 100) % 1000000
        line = f"{sym.decode()},{time_val},{4500000+i},{100+i},{4600000+i},{200+i},2,{time_val-23}".encode()
        lines.append(line)
    # Add cross-day records (20260529) — these affect seqno assignment
    for i in range(5):
        time_val = 20260529090000000 + i * 100
        lines.append(f"7203,{time_val},4600000,100,4610000,200,2,{time_val}".encode())
    # Add empty symbol (both Rust and fixed Python should skip)
    lines.append(b",20260528090099999,1000000,100,1010000,200")
    # Add whitespace-only symbol (both paths should skip after Phase 0 fix)
    lines.append(b"   ,20260528090088888,2000000,200,2010000,300")
    # Add 6-col lines (missing decimal and rcvtime — defaults applied)
    for i in range(20):
        lines.append(f"6758,202605281000{i:05d},5000000,{100+i},5010000,{200+i}".encode())
    # Add 7-col lines (missing rcvtime — default 0)
    for i in range(10):
        lines.append(f"8306,202605281000{i:05d},3000000,{50+i},3010000,{100+i},2".encode())
    # Add CRLF-terminated lines
    lines.append(b"7203,20260528110000001,4600000,100,4610000,200,2,0\r\n")
    lines.append(b"6501,20260528110000002,2400000,300,2410000,400,2,0\r\n")
    # Add whitespace in numeric fields
    lines.append(b"9984, 20260528110000003 , 8900000 , 150 , 8910000 , 250 , 2 , 0")
    lines.insert(0, b"symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime")

    # Both paths: apply date filter (today=20260528), seqno only after filter passes
    TODAY = 20260528

    # Rust path
    rust_batch, _ = parse_order_batch(lines, "utf-8")
    rust_csv_lines = []
    seqno = 0
    for fields in rust_batch:
        record_date = fields[1] // 1_000_000_000
        if record_date != TODAY:
            continue
        seqno += 1
        rec = OrderRecord(
            symbol=fields[0], seqno=seqno, time=fields[1],
            bidprice=fields[2], bidsize=fields[3],
            askprice=fields[4], asksize=fields[5],
            decimal=fields[6], rcvtime=fields[7],
        )
        rust_csv_lines.append(_format_order_row(rec))

    # Python path — MUST match engine.py exactly: str[:8] for date extraction
    py_csv_lines = []
    seqno = 0
    for line in lines:
        r = parse_order_record(line, 0, "utf-8")
        if r is not None:
            record_date = str(r.time)[:8]  # EXACT engine.py behavior — NOT //10^9
            if record_date != str(TODAY):  # EXACT engine.py behavior — string comparison
                continue
            seqno += 1
            rec = OrderRecord(r.symbol, seqno, r.time, r.bidprice, r.bidsize,
                              r.askprice, r.asksize, r.decimal, r.rcvtime)
            py_csv_lines.append(_format_order_row(rec))

    assert rust_csv_lines == py_csv_lines, (
        f"CSV output mismatch: {len(rust_csv_lines)} vs {len(py_csv_lines)} rows"
    )


def test_fallback_when_rust_unavailable():
    """Verify Python fallback works when Rust extension is not loaded."""
    # This test always runs (even without Rust extension)
    from minute_bar.csv_parser import parse_order_record
    from minute_bar.models import OrderRecord

    line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    record = parse_order_record(line, 1, "utf-8")
    assert record is not None
    assert record.symbol == "7203"
    assert record.time == 20260528090000123


def test_empty_symbol_rejected_python():
    """Verify Python parse_order_record rejects empty symbols (Phase 0 fix)."""
    from minute_bar.csv_parser import parse_order_record

    # Empty symbol
    assert parse_order_record(b",20260528090000123,4580000,100,4590000,200", 1, "utf-8") is None
    # Whitespace-only symbol
    assert parse_order_record(b"   ,20260528090000123,4580000,100,4590000,200", 1, "utf-8") is None


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_rust_batch_exception_fallback():
    """If Rust batch parse raises, engine.py falls back to Python per-line."""
    from unittest.mock import patch
    from minute_bar._order_accel import parse_order_batch

    # Verify MAX_BATCH_SIZE guard works
    with pytest.raises(Exception):
        # Create a batch larger than MAX_BATCH_SIZE
        huge_batch = [b"7203,20260528090000123,4580000,100,4590000,200,2,0"] * 1_000_001
        parse_order_batch(huge_batch, "utf-8")


@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_is_available():
    """Rust is_available self-test must return True."""
    from minute_bar._order_accel import is_available
    assert is_available() is True


def test_rust_accel_config_guard():
    """use_rust_accel returns False when config disables it."""
    from minute_bar.csv_parser import use_rust_accel
    from unittest.mock import MagicMock

    # Config with enable_order_accel=False
    config = MagicMock()
    config.input.enable_order_accel = False
    # Even if Rust is available, should return False when config says so
    # (assuming Rust is available — if not, this just tests the config check)
    if use_rust_accel():
        assert use_rust_accel(config) is False
    else:
        assert use_rust_accel(config) is False


def test_time_to_minute_key_integer_div():
    """Verify str(time // 100_000) produces same result as str(time)[:12] for 17-digit timestamps."""
    # This validates the optimization used in the Rust path
    test_times = [
        20260528090000123,
        20260528113000123,
        20260528150000123,
        20260528080000000,
    ]
    for t in test_times:
        str_method = str(t)[:12]
        int_method = str(t // 100_000)
        assert str_method == int_method, f"Mismatch for {t}: str[:12]={str_method}, //100_000={int_method}"


# ── Phase 4: Engine-Level Integration Tests ──

@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
class TestEngineRustIntegration:
    """Engine-level integration: exercise shared _process_parsed_record path
    used by both Rust and Python parsing."""

    def test_rust_path_processes_records_correctly(self):
        """Verify shared processing path produces correct OrderRecords."""
        from minute_bar.engine import Engine, _OrderMinuteBuffer
        from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig
        from minute_bar.models import OrderRecord
        from unittest.mock import patch
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            config = AppConfig(
                input=InputConfig(
                    csv_dir="/tmp/input",
                    enable_order_accel=True,
                ),
                output=OutputConfig(output_dir=tmpdir, enable_order=True),
                recovery=RecoveryConfig(data_flush_delay_minutes=0),
            )

            with patch("minute_bar.engine.ClockWatermarkFlusher"), \
                 patch("minute_bar.engine.CodeTable"), \
                 patch("minute_bar.engine.FileTailer"), \
                 patch("minute_bar.engine.CheckpointManager"):
                engine = Engine(config)

            # Simulate _process_parsed_record with a Rust-parsed record
            record = OrderRecord(
                symbol="7203", seqno=1, time=20260528090000123,
                bidprice=4580000, bidsize=100,
                askprice=4590000, asksize=200,
                decimal=2, rcvtime=0,
            )
            buffers = {}
            pending_shared_orders = []
            late_order_per_minute = {}
            late_dropped_per_minute = {}

            seqno, total_late, cur_date, cur_min = engine._process_parsed_record(
                record=record,
                today_str="20260528",
                seqno=0,
                minute_key="202605280900",
                buffers=buffers,
                current_date=None,
                current_minute=None,
                pending_shared_orders=pending_shared_orders,
                late_order_per_minute=late_order_per_minute,
                late_dropped_per_minute=late_dropped_per_minute,
                output_dir=tmpdir,
                total_late_dropped=0,
            )

            assert seqno == 0  # seqno is NOT incremented by _process_parsed_record
            assert cur_date == "20260528"
            assert cur_min == "202605280900"
            assert "202605280900" in buffers
            assert len(buffers["202605280900"].records) == 1
            assert buffers["202605280900"].records[0].symbol == "7203"

    def test_rust_path_cross_day_flush(self):
        """Cross-day records trigger flush in shared processing path."""
        from minute_bar.engine import Engine, _OrderMinuteBuffer
        from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig
        from minute_bar.models import OrderRecord
        from unittest.mock import patch
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp/input", enable_order_accel=True),
                output=OutputConfig(output_dir=tmpdir, enable_order=True),
                recovery=RecoveryConfig(data_flush_delay_minutes=0),
            )

            with patch("minute_bar.engine.ClockWatermarkFlusher"), \
                 patch("minute_bar.engine.CodeTable"), \
                 patch("minute_bar.engine.FileTailer"), \
                 patch("minute_bar.engine.CheckpointManager"):
                engine = Engine(config)

            # Pre-fill buffer with day 20260528
            buffers = {"202605280900": _OrderMinuteBuffer()}
            buffers["202605280900"].records.append(
                OrderRecord("7203", 1, 20260528090000123, 4580000, 100, 4590000, 200, 2, 0)
            )

            # Process cross-day record (20260529)
            record = OrderRecord("7203", 2, 20260529090000123, 4600000, 100, 4610000, 200, 2, 0)

            seqno, total_late, cur_date, cur_min = engine._process_parsed_record(
                record=record,
                today_str="20260529",
                seqno=1,
                minute_key="202605290900",
                buffers=buffers,
                current_date="20260528",
                current_minute="202605280900",
                pending_shared_orders=[],
                late_order_per_minute={},
                late_dropped_per_minute={},
                output_dir=tmpdir,
                total_late_dropped=0,
            )

            assert cur_date == "20260529"
            # Cross-day flush should have cleared old buffers
            assert "202605280900" not in buffers


# ── Phase 4: Corrupted .pyd Rollback Tests ──

def test_corrupted_pyd_rollback():
    """Verify system degrades gracefully when .pyd is corrupted."""
    import minute_bar.csv_parser as csv_mod

    # Save original state
    original_available = csv_mod._RUST_ACCEL_AVAILABLE

    # Simulate ImportError scenario
    csv_mod._RUST_ACCEL_AVAILABLE = False
    csv_mod._RUST_ACCEL_LOADED = False

    # Verify fallback works
    assert not csv_mod.use_rust_accel()
    assert csv_mod.has_rust_accel() is False

    # Verify Python parsing still works
    from minute_bar.csv_parser import parse_order_record
    line = b"7203,20260528090000123,4580000,100,4590000,200,2,0"
    record = parse_order_record(line, 1, "utf-8")
    assert record is not None
    assert record.symbol == "7203"

    # Restore
    csv_mod._RUST_ACCEL_AVAILABLE = original_available


def test_engine_starts_without_rust():
    """Engine starts successfully when Rust accel is disabled by config."""
    from minute_bar.engine import Engine
    from minute_bar.config import AppConfig, InputConfig, OutputConfig
    from unittest.mock import patch
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        config = AppConfig(
            input=InputConfig(csv_dir="/tmp/input", enable_order_accel=False),
            output=OutputConfig(output_dir=tmpdir),
        )

        with patch("minute_bar.engine.ClockWatermarkFlusher"), \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            engine = Engine(config)
            # Should NOT raise — enable_order_accel=false, graceful degradation
            assert engine is not None


def test_warmup_failure_degrades_gracefully():
    """If Rust warmup self-test fails, set_rust_available(False) is called."""
    import minute_bar.csv_parser as csv_mod
    from unittest.mock import patch
    import tempfile

    # Only test if Rust is available — otherwise there's nothing to warm up
    if not csv_mod._RUST_ACCEL_AVAILABLE:
        pytest.skip("Rust extension not available")

    original = csv_mod._RUST_ACCEL_AVAILABLE

    try:
        # Simulate warmup failure by making parse_order_batch return wrong count
        from minute_bar.config import AppConfig, InputConfig, OutputConfig, RecoveryConfig
        from minute_bar.engine import Engine

        with tempfile.TemporaryDirectory() as tmpdir:
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp/input", enable_order_accel=True),
                output=OutputConfig(output_dir=tmpdir, enable_order=True),
                recovery=RecoveryConfig(data_flush_delay_minutes=0),
            )
            with patch("minute_bar.engine.ClockWatermarkFlusher"), \
                 patch("minute_bar.engine.CodeTable"), \
                 patch("minute_bar.engine.FileTailer"), \
                 patch("minute_bar.engine.CheckpointManager"), \
                 patch("minute_bar._order_accel.parse_order_batch", return_value=([0], 999)):
                engine = Engine(config)
                # After warmup failure, _RUST_ACCEL_AVAILABLE should be False
                assert not csv_mod._RUST_ACCEL_AVAILABLE, (
                    "Warmup failure should have set _RUST_ACCEL_AVAILABLE to False"
                )
    finally:
        # Restore for other tests
        csv_mod._RUST_ACCEL_AVAILABLE = original
