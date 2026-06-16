"""TDD for tickfile stale-row fix (shutdown skip + replay surgical fill).
Spec: docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md"""
import os
from unittest.mock import patch

import pytest

from minute_bar.aggregator import SharedState
from minute_bar.writer import get_tickfile_path


# --- Flusher construction helper (mirrors tests/test_tickfile_sync.py:_make_flusher) ---
def _make_flusher(state, tmp_path, enable_tickfile=True):
    from minute_bar.code_table import CodeTable
    from minute_bar.checkpoint import CheckpointManager
    from minute_bar.flusher import ClockWatermarkFlusher
    flusher = ClockWatermarkFlusher(
        state=state,
        code_table=CodeTable("dummy"),
        checkpoint=CheckpointManager("dummy", {}),
        output_dir=str(tmp_path),
        output_delay_sec=60,
        enable_order=True,
        enable_tickfile=enable_tickfile,
    )
    return flusher


def _make_state():
    state = SharedState()
    state.first_data_received = True
    return state


class TestShutdownSkipsUnreachedMinutes:
    """Part 1: flush_all_remaining skips minutes order_current_minute hasn't reached."""

    def test_unreached_minute_is_skipped(self, tmp_path):
        state = _make_state()
        state.order_current_minute = "202605281430"   # order reached 1430
        # 1429 reached (order_wm >= mk); 1500 unreached (order_wm < mk)
        state._tickfile_pending["202605281429"] = {"raw_records": {}, "snapshot_copy": {}}
        state._tickfile_pending["202605281500"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605281429" in called          # reached -> generated
        assert "202605281500" not in called       # unreached -> skipped

    def test_natural_eof_generates_last_minute(self, tmp_path):
        """order_current_minute == last pending minute must still generate it (>= not >)."""
        state = _make_state()
        state.order_current_minute = "202605281530"   # == close; must NOT be skipped
        state._tickfile_pending["202605281530"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605281530" in called            # close generated, not skipped

    def test_empty_order_watermark_skips_all(self, tmp_path):
        """No order ever flushed (empty watermark) -> skip all (all would be stale)."""
        state = _make_state()
        state.order_current_minute = ""             # order never flushed anything
        state._tickfile_pending["202605280900"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        assert mock_gen.call_count == 0             # nothing reached -> skip all


class TestCrossDaySkipsUnreachedMinutes:
    """Part 1: cross-day force-gen also skips order-unreached minutes."""

    def test_cross_day_skips_unreached(self, tmp_path):
        state = _make_state()
        state.last_output_date = "20260528"        # yesterday
        state.current_minute = "202605290930"      # today (triggers cross-day)
        state.order_current_minute = "202605280930"  # yesterday's last order minute
        # 0900 reached (<= 0930); 1500 unreached (> 0930)
        state._tickfile_pending["202605280900"] = {"raw_records": {}, "snapshot_copy": {}}
        state._tickfile_pending["202605281500"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher._step1_cross_day_check()

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605280900" in called
        assert "202605281500" not in called


class TestReplayScanExtractor:
    """Part 2: UpdateTime column -> minute_key extraction (the scan primitive)."""

    def test_extract_minutes_skips_header_and_reverses_updatetime(self, tmp_path):
        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig
        from minute_bar.tickfile import TICKFILE_HEADER
        from minute_bar.writer import get_tickfile_path
        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True))
        engine = ReplayEngine(config, date="20260528")
        # Build a valid 65-field tickfile row: InstrumentID(0),TradingDay(1),..UpdateTime(16)..Seqno(59),LocalTime(60)
        path = get_tickfile_path(str(tmp_path), "202605280900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fields = [""] * 65
        fields[0] = "7203"; fields[1] = "20260528"; fields[16] = "20260528 09:01:00"
        fields[59] = "1"; fields[60] = "2026-05-28 09:01:00.000000"
        row_0901 = ",".join(fields)
        fields2 = list(fields)
        fields2[16] = "20260528 09:02:00"; fields2[59] = "2"
        row_0902 = ",".join(fields2)
        with open(path, "w", encoding="utf-8") as f:
            f.write(TICKFILE_HEADER + "\n" + row_0901 + "\n" + row_0902 + "\n")

        present = engine._scan_generated_tickfile_minutes(str(tmp_path))

        assert present == {"202605280901", "202605280902"}

    def test_scan_returns_empty_when_no_tickfile(self, tmp_path):
        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig
        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True))
        engine = ReplayEngine(config, date="20260528")
        assert engine._scan_generated_tickfile_minutes(str(tmp_path)) == set()


class TestReplaySkipAlreadyGenerated:
    """Part 2: replay skips minutes already in the tickfile (no duplicate/corruption).
    The strongest assertion is the integration test (Task 5); this unit test exercises
    the guarded path (_flush_snapshot_minute) for a minute pre-seeded in
    _generated_tickfile_minutes and pins the guard invariant: a skipped minute writes
    no tickfile row and burns no seqno number."""

    def test_skip_does_not_write_or_advance_seqno(self, tmp_path):
        """A minute in _generated_tickfile_minutes is skipped inside _flush_snapshot_minute:
        write_tickfile_rows is never called and _tickfile_seqno is unchanged.
        (Full no-duplicate/corruption assertion is the Task 5 integration test.)"""
        from concurrent.futures import ThreadPoolExecutor

        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig

        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True,
                                               enable_kline=False))
        engine = ReplayEngine(config, date="20260528")
        # Minimal state the guarded method needs (lock + empty buffers). SharedState is
        # already imported at module top of this test file.
        engine._state = SharedState()
        engine._generated_tickfile_minutes = {"202605280901"}  # 0901 already present
        engine._tickfile_seqno = 5

        skipped_mk = "202605280901"
        seqno_before = engine._tickfile_seqno

        # write_snapshot_file / write_kline_file are MODULE-LEVEL imports in replay.py,
        # so patch them at the lookup site (minute_bar.replay.<name>) so the method's
        # snapshot write completes without real buffers. write_tickfile_rows is
        # LAZILY imported inside the method body (``from minute_bar.writer import ...``),
        # so it must be patched at its SOURCE (minute_bar.writer.write_tickfile_rows);
        # patching minute_bar.replay.write_tickfile_rows would NOT intercept the lookup.
        with patch("minute_bar.replay.write_snapshot_file") as mock_snap, \
             patch("minute_bar.replay.write_kline_file"), \
             patch("minute_bar.writer.write_tickfile_rows") as mock_write:
            engine._flush_snapshot_minute(
                minute_key=skipped_mk,
                output_dir=str(tmp_path),
                code_table=engine._code_table,
                full_snapshot=True,
                full_kline=False,
                enable_kline=False,
                write_executor=ThreadPoolExecutor(max_workers=1),
            )

        assert mock_write.call_count == 0, "skipped minute must NOT write a tickfile row"
        assert engine._tickfile_seqno == seqno_before, "skipped minute must NOT burn a seqno"
        # And it must not have registered the minute as newly generated.
        assert engine._generated_tickfile_minutes == {"202605280901"}
