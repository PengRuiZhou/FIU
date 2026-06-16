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
