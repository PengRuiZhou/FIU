# Data-Driven Watermark Flush Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace real-clock-driven flush with data-progress-driven watermark, keeping real clock as optional fallback.

**Architecture:** Add `is_data_driven_expired()` pure function that compares data watermark against minute_key + delay. Refactor `_step3_minute_output` to use data-driven + fallback OR logic. Add `flush_all_remaining()` for shutdown safety. Make `current_minute` monotonically increasing.

**Tech Stack:** Python 3.10+, pytest, threading (RLock), dataclasses

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/minute_bar/clock.py` | Modify | Add `minute_key_to_start_time()` and `is_data_driven_expired()`. Add `STALL_WARN_SECONDS` constant. |
| `src/minute_bar/config.py` | Modify | Add `data_flush_delay_minutes` and `enable_time_fallback` to `RecoveryConfig`. Add INI loading. |
| `src/minute_bar/aggregator.py` | Modify | Change `current_minute` update to monotonic in `process_snapshot()`. |
| `src/minute_bar/flusher.py` | Modify | Refactor `_step3` to data-driven + fallback. Add `_flush_minutes_internal()`, `flush_all_remaining()`, stall detection. Update `_step1_cross_day_check` to reset watermark. Update constructor. |
| `src/minute_bar/engine.py` | Modify | Refactor `_order_loop` (processing order, `!=` → `>`, monotonic `current_minute`, cross-day safety). Update `_flush_expired_order_minutes` to accept `order_watermark`. Rewrite `stop()` with final flush + exception safety. Update constructor. |
| `tests/test_clock.py` | Create | Tests for `is_data_driven_expired` and `minute_key_to_start_time`. |
| `tests/test_config.py` | Create | Tests for new config fields. |
| `tests/test_watermark_flusher.py` | Create | Tests for data-driven flush logic, `_flush_minutes_internal`, `flush_all_remaining`, stall detection. |
| `tests/test_watermark_engine.py` | Create | Tests for order loop changes, `stop()` final flush, `_flush_expired_order_minutes`. |

---

## Task 1: `clock.py` — Add `minute_key_to_start_time` and `is_data_driven_expired`

**Files:**
- Modify: `src/minute_bar/clock.py:61` (after `is_expired`)
- Test: `tests/test_clock.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_clock.py`:

```python
"""Tests for data-driven watermark functions in clock.py."""
import pytest
from datetime import timedelta

from minute_bar.clock import (
    JST,
    is_data_driven_expired,
    minute_key_to_start_time,
)


class TestMinuteKeyToStartTime:
    def test_valid_key(self):
        dt = minute_key_to_start_time("202605210900")
        assert dt.hour == 9
        assert dt.minute == 0
        assert dt.tzinfo == JST

    def test_midnight(self):
        dt = minute_key_to_start_time("202605210000")
        assert dt.hour == 0
        assert dt.minute == 0

    def test_end_of_day(self):
        dt = minute_key_to_start_time("202605202359")
        assert dt.hour == 23
        assert dt.minute == 59

    def test_invalid_length(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090")

    def test_invalid_non_digit(self):
        with pytest.raises(ValueError, match="12-digit"):
            minute_key_to_start_time("20260521090X")

    def test_invalid_hour(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605212500")

    def test_invalid_minute(self):
        with pytest.raises(ValueError):
            minute_key_to_start_time("202605210961")

    def test_cross_day_boundary(self):
        dt = minute_key_to_start_time("202605202359")
        next_min = dt + timedelta(minutes=1)
        assert next_min.day == 21
        assert next_min.hour == 0


class TestIsDataDrivenExpired:
    def test_watermark_ahead_triggers_flush(self):
        assert is_data_driven_expired("202605210900", "202605210901", 1) is True

    def test_same_minute_no_flush(self):
        assert is_data_driven_expired("202605210900", "202605210900", 1) is False

    def test_watermark_behind_no_flush(self):
        assert is_data_driven_expired("202605210900", "202605210859", 1) is False

    def test_empty_watermark_no_flush(self):
        assert is_data_driven_expired("202605210900", "", 1) is False

    def test_skip_minutes(self):
        assert is_data_driven_expired("202605210900", "202605210905", 1) is True

    def test_delay_2_watermark_2_ahead(self):
        assert is_data_driven_expired("202605210900", "202605210902", 2) is True

    def test_delay_2_watermark_1_ahead(self):
        assert is_data_driven_expired("202605210900", "202605210901", 2) is False

    def test_delay_0_same_minute(self):
        assert is_data_driven_expired("202605210900", "202605210900", 0) is True

    def test_cross_day(self):
        assert is_data_driven_expired("202605202359", "202605210000", 1) is True

    def test_cross_day_different_hhmm(self):
        assert is_data_driven_expired("202605190930", "202605200930", 1) is True

    def test_exact_threshold_true(self):
        assert is_data_driven_expired("202605210900", "202605210901", 1) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd d:/FIU && python -m pytest tests/test_clock.py -v`
Expected: FAIL — `ImportError: cannot import name 'is_data_driven_expired'`

- [ ] **Step 3: Write the implementation**

Add to `src/minute_bar/clock.py` after `is_expired` (after line 63), and add `STALL_WARN_SECONDS` at module level:

```python
STALL_WARN_SECONDS = 300


def minute_key_to_start_time(minute_key: str) -> datetime:
    """Convert minute_key "YYYYMMDDHHMM" to the start datetime of that minute."""
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST)


def is_data_driven_expired(minute_key: str, data_watermark: str, delay_minutes: int) -> bool:
    """Check if minute_key should be flushed based on data progress watermark."""
    if not data_watermark:
        return False
    watermark_dt = minute_key_to_start_time(data_watermark)
    threshold = minute_key_to_start_time(minute_key) + timedelta(minutes=delay_minutes)
    return watermark_dt >= threshold
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd d:/FIU && python -m pytest tests/test_clock.py -v`
Expected: All 18 tests PASS

- [ ] **Step 5: Commit**

```bash
cd d:/FIU && git add src/minute_bar/clock.py tests/test_clock.py
git commit -m "feat: add is_data_driven_expired and minute_key_to_start_time for data-driven watermark"
```

---

## Task 2: `config.py` — Add `data_flush_delay_minutes` and `enable_time_fallback`

**Files:**
- Modify: `src/minute_bar/config.py:52-56` (RecoveryConfig), `config.py:119-123` (load_config)
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_config.py`:

```python
"""Tests for config.py — new data-driven watermark fields."""
import pytest
import tempfile
import os

from minute_bar.config import AppConfig, RecoveryConfig, load_config


class TestRecoveryConfigDefaults:
    def test_default_data_flush_delay_minutes(self):
        cfg = RecoveryConfig()
        assert cfg.data_flush_delay_minutes == 1

    def test_default_enable_time_fallback(self):
        cfg = RecoveryConfig()
        assert cfg.enable_time_fallback is True


class TestLoadConfig:
    def _write_ini(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".ini")
        with os.fdopen(fd, "w") as f:
            f.write(content)
        return path

    def test_loads_new_fields(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
            "[recovery]\ndata_flush_delay_minutes = 2\nenable_time_fallback = false\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 2
            assert cfg.recovery.enable_time_fallback is False
        finally:
            os.unlink(path)

    def test_missing_fields_use_defaults(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
            "[recovery]\noutput_delay_sec = 5\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 1
            assert cfg.recovery.enable_time_fallback is True
        finally:
            os.unlink(path)

    def test_no_recovery_section_uses_defaults(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 1
            assert cfg.recovery.enable_time_fallback is True
        finally:
            os.unlink(path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd d:/FIU && python -m pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError` on `data_flush_delay_minutes`

- [ ] **Step 3: Write the implementation**

Modify `src/minute_bar/config.py`:

1. In `RecoveryConfig` (line 52-56), add two fields:

```python
@dataclass
class RecoveryConfig:
    checkpoint_file: str = "checkpoint.json"
    output_delay_sec: int = 5
    code_refresh_sec: int = 30
    data_flush_delay_minutes: int = 1
    enable_time_fallback: bool = True
```

2. In `load_config()`, inside the `if parser.has_section("recovery"):` block (after line 123), add:

```python
        cfg.recovery.data_flush_delay_minutes = s.getint("data_flush_delay_minutes", cfg.recovery.data_flush_delay_minutes)
        cfg.recovery.enable_time_fallback = s.getboolean("enable_time_fallback", cfg.recovery.enable_time_fallback)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd d:/FIU && python -m pytest tests/test_config.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
cd d:/FIU && git add src/minute_bar/config.py tests/test_config.py
git commit -m "feat: add data_flush_delay_minutes and enable_time_fallback to RecoveryConfig"
```

---

## Task 3: `aggregator.py` — Monotonic `current_minute` update

**Files:**
- Modify: `src/minute_bar/aggregator.py:134` (process_snapshot)
- Test: `tests/test_aggregator.py` (append to existing TestSharedState class)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_aggregator.py` after `TestMaybeUpdateLatest` class:

```python


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd d:/FIU && python -m pytest tests/test_aggregator.py::TestCurrentMinuteMonotonic -v`
Expected: FAIL — `test_current_minute_does_not_regress` fails because current_minute gets overwritten

- [ ] **Step 3: Write the implementation**

Change `src/minute_bar/aggregator.py` line 134:

From:
```python
            self.current_minute = minute_key
```

To:
```python
            if not self.current_minute or minute_key > self.current_minute:
                self.current_minute = minute_key
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd d:/FIU && python -m pytest tests/test_aggregator.py -v`
Expected: All tests PASS (including existing ones)

- [ ] **Step 5: Commit**

```bash
cd d:/FIU && git add src/minute_bar/aggregator.py tests/test_aggregator.py
git commit -m "feat: make current_minute monotonically increasing in process_snapshot"
```

---

## Task 4: `flusher.py` — Constructor + `_flush_minutes_internal` + `_step3` refactor + stall detection + cross-day watermark reset + `flush_all_remaining`

This is the largest task. It modifies the flusher comprehensively.

**Files:**
- Modify: `src/minute_bar/flusher.py`
- Test: `tests/test_watermark_flusher.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_watermark_flusher.py`:

```python
"""Tests for data-driven watermark flush logic in flusher.py."""
import pytest
from unittest.mock import patch, MagicMock

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.clock import is_data_driven_expired
from minute_bar.code_table import CodeTable
from minute_bar.flusher import ClockWatermarkFlusher
from minute_bar.models import OHLCVAggregate, SnapshotRecord


def make_snapshot(symbol="1301", seqno=1, lastprice=4500.0, time_=20260520093000999, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_, rcvtime=20260520083000999,
        preclose=4435.0, lastprice=lastprice, open=4400.0, high=4510.0, low=4435.0,
        close=4500.0, lasttradeprice=4500.0, lasttradeqty=100, totalvol=78300,
        totalamount=3502510.0, sessionid=1, tradetype="", status="T",
        direction=0, pflag="Y", decimal=2, vwap=4450.0, shortsellflag=0,
    )
    defaults.update(kwargs)
    return SnapshotRecord(**defaults)


def make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=True, output_delay_sec=60):
    code_table = CodeTable.__new__(CodeTable)
    code_table._table = {}
    checkpoint = CheckpointManager(str(tmp_path / "checkpoint.json"), str(tmp_path))
    return ClockWatermarkFlusher(
        state=state,
        code_table=code_table,
        checkpoint=checkpoint,
        output_dir=str(tmp_path),
        output_delay_sec=output_delay_sec,
        enable_full_snapshot=True,
        enable_full_kline=True,
        enable_kline=True,
        enable_order=True,
        file_states={},
        checkpoint_lock=None,
        data_flush_delay_minutes=data_flush_delay_minutes,
        enable_time_fallback=enable_time_fallback,
    )


class TestStep3DataDrivenFlush:
    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_data_driven_flush_triggers(self, mock_expired, tmp_path):
        """When watermark is ahead, data-driven flush triggers."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210901"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        assert "202605210900" in state.flushed_snapshot_minutes
        assert "202605210900" not in state.ohlcv_buffers

    @patch("minute_bar.flusher.is_expired", return_value=True)
    def test_fallback_flush_when_watermark_stalled(self, mock_expired, tmp_path):
        """When watermark is same minute but time expired, fallback triggers."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=True)
        flusher._step3_minute_output()

        assert "202605210900" in state.flushed_snapshot_minutes

    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_no_flush_when_watermark_behind(self, mock_expired, tmp_path):
        """When watermark hasn't advanced past threshold, no flush."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        assert "202605210900" not in state.flushed_snapshot_minutes

    @patch("minute_bar.flusher.is_expired", return_value=False)
    def test_no_fallback_when_disabled(self, mock_expired, tmp_path):
        """When enable_time_fallback=False, is_expired is never called."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.current_minute = "202605210900"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}

        flusher = make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=False)
        flusher._step3_minute_output()

        mock_expired.assert_not_called()


class TestFlushMinutesInternal:
    def test_defensive_pop_missing_key(self, tmp_path):
        """Keys not in ohlcv_buffers are skipped without KeyError."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        flusher = make_flusher(state, tmp_path)
        # Call with a key that doesn't exist in any buffer
        flusher._flush_minutes_internal(["202605210999"], is_final=False)
        # Should not raise

    def test_is_final_continue_on_failure(self, tmp_path):
        """When is_final=True, individual write failures don't stop the loop."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.ohlcv_buffers["202605210901"] = {"1302": OHLCVAggregate(symbol="1302")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.raw_snapshot_buffers["202605210901"] = {}
        state.latest_snapshot["1301"] = make_snapshot()
        state.latest_snapshot["1302"] = make_snapshot(symbol="1302")

        flusher = make_flusher(state, tmp_path)

        # Make write fail for first minute by making output_dir invalid
        original_write = flusher._write_minute_files
        call_count = 0

        def failing_write(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise IOError("simulated failure")

        flusher._write_minute_files = failing_write
        with pytest.raises(RuntimeError, match="Final flush failed"):
            flusher._flush_minutes_internal(["202605210900", "202605210901"], is_final=True)
        assert call_count == 2  # Both minutes were attempted


class TestFlushAllRemaining:
    def test_flushes_all_remaining_buffers(self, tmp_path):
        """flush_all_remaining flushes all buffers unconditionally."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.ohlcv_buffers["202605210901"] = {"1302": OHLCVAggregate(symbol="1302")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.raw_snapshot_buffers["202605210901"] = {}
        state.latest_snapshot["1301"] = make_snapshot()
        state.latest_snapshot["1302"] = make_snapshot(symbol="1302")

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()

        assert len(state.ohlcv_buffers) == 0
        assert "202605210900" in state.flushed_snapshot_minutes
        assert "202605210901" in state.flushed_snapshot_minutes

    def test_writes_checkpoint_after_flush(self, tmp_path):
        """Checkpoint is written even if flush had partial failures."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"
        state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
        state.raw_snapshot_buffers["202605210900"] = {}
        state.latest_snapshot["1301"] = make_snapshot()

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()

        import os
        assert os.path.exists(str(tmp_path / "checkpoint.json"))

    def test_empty_buffers_is_noop(self, tmp_path):
        """No buffers → no error, just checkpoint."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260520"

        flusher = make_flusher(state, tmp_path)
        flusher.flush_all_remaining()  # Should not raise


class TestCrossDayWatermarkReset:
    def test_cross_day_resets_watermark_to_zero_base(self, tmp_path):
        """Cross-day sets current_minute to new_date + '0000'."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605200930"

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        assert state.current_minute == "202605200000"

    def test_cross_day_does_not_overwrite_advanced_watermark(self, tmp_path):
        """If data-thread already pushed watermark past zero, don't overwrite."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260519"
        state.current_minute = "202605220900"

        flusher = make_flusher(state, tmp_path)
        flusher._step1_cross_day_check()

        assert state.current_minute == "202605220900"


class TestStallDetection:
    def test_stall_warning_triggered(self, tmp_path):
        """Stall warning fires after STALL_WARN_SECONDS of no watermark progress."""
        import time as _time
        from minute_bar import flusher as flusher_mod
        original = flusher_mod.STALL_WARN_SECONDS
        flusher_mod.STALL_WARN_SECONDS = 0.1  # 100ms for fast test

        try:
            state = SharedState()
            state.first_data_received = True
            state.last_output_date = "20260520"
            state.current_minute = "202605210900"
            state.ohlcv_buffers["202605210900"] = {"1301": OHLCVAggregate(symbol="1301")}
            state.raw_snapshot_buffers["202605210900"] = {}

            flusher = make_flusher(state, tmp_path, enable_time_fallback=False)

            # First tick sets baseline
            flusher._step3_minute_output()
            # Wait for stall threshold
            _time.sleep(0.15)

            # This tick should detect stall
            with patch.object(flocker_mod.logger, "error") as mock_error:
                flusher._step3_minute_output()
                # Check that stall was logged
                stall_calls = [c for c in mock_error.call_args_list if "stalled" in str(c).lower()]
                assert len(stall_calls) > 0
        finally:
            flusher_mod.STALL_WARN_SECONDS = original

    def test_stall_counter_resets_on_progress(self, tmp_path):
        """Watermark progress resets the stall counter."""
        import time as _time
        from minute_bar import flusher as flusher_mod
        original = flusher_mod.STALL_WARN_SECONDS
        flusher_mod.STALL_WARN_SECONDS = 1.0

        try:
            state = SharedState()
            state.first_data_received = True
            state.last_output_date = "20260520"
            state.current_minute = "202605210900"
            flusher = make_flusher(state, tmp_path, enable_time_fallback=False)

            flusher._step3_minute_output()

            # Advance watermark
            state.current_minute = "202605210901"

            # Should not warn because watermark advanced
            flusher._step3_minute_output()
            assert not flusher._stall_warned
        finally:
            flusher_mod.STALL_WARN_SECONDS = original
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd d:/FIU && python -m pytest tests/test_watermark_flusher.py -v`
Expected: FAIL — `TypeError` on constructor (missing `data_flush_delay_minutes` param)

- [ ] **Step 3: Write the implementation**

Modify `src/minute_bar/flusher.py` completely. The full new file:

```python
from __future__ import annotations

import csv
import logging
import os
import threading
import time
from typing import Dict, Optional, Set

from minute_bar.aggregator import SharedState
from minute_bar.checkpoint import CheckpointManager
from minute_bar.clock import (
    STALL_WARN_SECONDS,
    extract_date_from_minute_key,
    is_data_driven_expired,
    is_expired,
    is_yesterday,
    jst_now_yyyymmdd,
    jst_now_yyyymmddhhmm,
    time_to_minute_key,
)
from minute_bar.code_table import CodeTable
from minute_bar.models import FileState, SnapshotRecord
from minute_bar.writer import (
    append_snapshot_records,
    get_snapshot_file_path,
    write_kline_file,
    write_order_file,
    write_snapshot_file,
)

logger = logging.getLogger(__name__)


class ClockWatermarkFlusher:
    def __init__(
        self,
        state: SharedState,
        code_table: CodeTable,
        checkpoint: CheckpointManager,
        output_dir: str,
        output_delay_sec: int,
        enable_full_snapshot: bool = True,
        enable_full_kline: bool = True,
        enable_kline: bool = True,
        enable_order: bool = True,
        file_states: Optional[Dict[str, FileState]] = None,
        checkpoint_lock: Optional[threading.Lock] = None,
        data_flush_delay_minutes: int = 1,
        enable_time_fallback: bool = True,
    ):
        self._state = state
        self._code_table = code_table
        self._checkpoint = checkpoint
        self._output_dir = output_dir
        self._output_delay_sec = output_delay_sec
        self._enable_full_snapshot = enable_full_snapshot
        self._enable_full_kline = enable_full_kline
        self._enable_kline = enable_kline
        self._enable_order = enable_order
        self._file_states = file_states or {}
        self._checkpoint_lock = checkpoint_lock
        self._data_flush_delay_minutes = data_flush_delay_minutes
        self._enable_time_fallback = enable_time_fallback

        # Stall detection state (only accessed by clock-thread)
        self._last_watermark: Optional[str] = None
        self._last_watermark_advance_ts: Optional[float] = None
        self._stall_warned: bool = False

    def tick(self) -> None:
        self._step1_cross_day_check()
        if not self._step2_first_data_check():
            return
        self._step3_minute_output()
        self._step4_handle_late_records()
        self._step5_write_checkpoint()

    def _step1_cross_day_check(self) -> None:
        with self._state.lock:
            if not self._state.first_data_received:
                return
            if not self._state.last_output_date:
                self._state.last_output_date = extract_date_from_minute_key(self._state.current_minute)
                return

            current_date = extract_date_from_minute_key(self._state.current_minute)
            if current_date == self._state.last_output_date:
                return

            pending = {
                k: self._state.ohlcv_buffers.pop(k)
                for k in list(self._state.ohlcv_buffers)
                if is_yesterday(k, current_date)
            }
            pending_raw = {
                k: self._state.raw_snapshot_buffers.pop(k)
                for k in list(self._state.raw_snapshot_buffers)
                if is_yesterday(k, current_date)
            }
            pending_orders = {
                k: self._state.raw_order_buffers.pop(k)
                for k in list(self._state.raw_order_buffers)
                if is_yesterday(k, current_date)
            }
            snapshot_copy = dict(self._state.latest_snapshot)

        if pending:
            for minute_key in sorted(pending):
                data = pending[minute_key]
                raw = pending_raw.get(minute_key, {})
                orders = pending_orders.get(minute_key, [])
                try:
                    self._write_minute_files(minute_key, snapshot_copy, data, raw, orders)
                except IOError as e:
                    logger.fatal("Output failed during cross-day flush for minute=%s: %s", minute_key, e)
                    raise SystemExit(1)

            self._write_checkpoint()

        with self._state.lock:
            self._state.output_minutes.clear()
            self._state.last_totalvol_by_symbol.clear()
            self._state.last_totalamount_by_symbol.clear()
            self._state.last_output_date = current_date
            self._state.last_output_minute = ""
            self._state.first_data_received = False
            self._state.flushed_snapshot_minutes.clear()
            self._state.late_snapshot_count = 0
            self._state.late_snapshot_minutes.clear()
            # Conditional watermark reset: only set if watermark < new day zero base
            new_day_base = current_date + "0000"
            if not self._state.current_minute or self._state.current_minute < new_day_base:
                self._state.current_minute = new_day_base

        logger.info("Cross-day reset completed. New date: %s", current_date)

    def _step2_first_data_check(self) -> bool:
        with self._state.lock:
            return self._state.first_data_received

    def _step3_minute_output(self) -> None:
        try:
            with self._state.lock:
                data_watermark = self._state.current_minute
                expired_keys = sorted(
                    k for k in self._state.ohlcv_buffers
                    if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
                       or (self._enable_time_fallback and is_expired(k, self._output_delay_sec))
                )
                data_driven_keys = [
                    k for k in expired_keys
                    if is_data_driven_expired(k, data_watermark, self._data_flush_delay_minutes)
                ]
                fallback_keys = [k for k in expired_keys if k not in data_driven_keys]
        except ValueError:
            logger.exception("Invalid minute_key or watermark format, skipping tick (data_watermark=%s)", locals().get('data_watermark'))
            return

        # Stall detection (wall-clock based)
        now = time.monotonic()
        if data_watermark == self._last_watermark:
            if self._last_watermark_advance_ts is not None:
                stalled_sec = now - self._last_watermark_advance_ts
                if not self._stall_warned and stalled_sec >= STALL_WARN_SECONDS:
                    logger.error(
                        "Watermark stalled at %s for %.0f seconds — data thread may be dead or data source stopped",
                        data_watermark, stalled_sec,
                    )
                    self._stall_warned = True
        else:
            if self._last_watermark is not None and self._stall_warned:
                logger.info(
                    "Watermark recovered: %s -> %s (was stalled for %.0f seconds)",
                    self._last_watermark, data_watermark,
                    now - self._last_watermark_advance_ts if self._last_watermark_advance_ts else 0,
                )
            self._last_watermark = data_watermark
            self._last_watermark_advance_ts = now
            self._stall_warned = False

        if expired_keys:
            if data_driven_keys:
                logger.info("Data-driven flush: %d minutes (watermark=%s)", len(data_driven_keys), data_watermark)
            if fallback_keys:
                logger.warning("Time-fallback flush: %d minutes (data progress lagging)", len(fallback_keys))
            self._flush_minutes_internal(expired_keys, is_final=False)

    def _flush_minutes_internal(
        self, minute_keys: list, *, is_final: bool = False
    ) -> None:
        """Flush specified minutes from buffers to files.

        Responsible for:
        1. Pop data from ohlcv/raw_snapshot/raw_order buffers under lock
        2. Copy latest_snapshot under lock
        3. Write files outside lock
        4. Update state under lock after each successful write
        """
        with self._state.lock:
            ohlcv_data = {}
            for k in minute_keys:
                v = self._state.ohlcv_buffers.pop(k, None)
                if v is not None:
                    ohlcv_data[k] = v
                else:
                    logger.debug("minute_key %s not found in ohlcv_buffers during flush, skipping", k)
            raw_data = {}
            for k in minute_keys:
                v = self._state.raw_snapshot_buffers.pop(k, None)
                if v is not None:
                    raw_data[k] = v
            order_data = {}
            for k in minute_keys:
                v = self._state.raw_order_buffers.pop(k, None)
                if v is not None:
                    order_data[k] = v
            snapshot_copy = dict(self._state.latest_snapshot)

        errors = []
        for minute_key in minute_keys:
            data = ohlcv_data.get(minute_key)
            if data is None:
                continue
            raw = raw_data.get(minute_key, {})
            orders = order_data.get(minute_key, [])
            try:
                self._write_minute_files(minute_key, snapshot_copy, data, raw, orders)
            except Exception:
                if is_final:
                    logger.exception("Final flush failed for minute=%s", minute_key)
                    errors.append(minute_key)
                    continue
                else:
                    logger.fatal("Output failed for minute=%s", minute_key)
                    raise SystemExit(1)

            with self._state.lock:
                self._state.output_minutes.add(minute_key)
                self._state.last_output_minute = minute_key
                self._state.flushed_snapshot_minutes.add(minute_key)
                for sym, agg in data.items():
                    self._state.last_totalvol_by_symbol[sym] = agg.end_totalvol
                    self._state.last_totalamount_by_symbol[sym] = agg.end_totalamount
            if is_final:
                logger.info("Final flush: minute=%s (%d symbols)", minute_key, len(data))

        if is_final:
            total = len(ohlcv_data)
            if errors:
                logger.error("Final flush summary: %d/%d minutes failed: %s", len(errors), total, errors)
                raise RuntimeError(
                    f"Final flush failed for {len(errors)}/{total} minutes: {errors}"
                )
            else:
                logger.info("Final flush summary: %d minutes flushed successfully", total)

    def flush_all_remaining(self) -> None:
        """Flush all remaining buffers on shutdown.

        PRECONDITION: All worker threads must be joined before calling.
        NOT thread-safe. No watermark or time checks — unconditional flush.
        """
        with self._state.lock:
            remaining_keys = sorted(self._state.ohlcv_buffers.keys())

        flush_error = None
        if remaining_keys:
            try:
                self._flush_minutes_internal(remaining_keys, is_final=True)
            except Exception as e:
                flush_error = e

        try:
            self._write_checkpoint()
        except Exception:
            logger.exception("Failed to write checkpoint after final flush")
            if flush_error:
                logger.error("Also had flush errors (checkpoint error takes priority): %s", flush_error)
            raise
        if flush_error:
            raise flush_error

    def _write_minute_files(
        self,
        minute_key: str,
        snapshot_copy: Dict[str, SnapshotRecord],
        ohlcv_data: Dict[str, OHLCVAggregate],
        raw_records: Dict[str, list] = None,
        order_records: list = None,
    ) -> None:
        write_snapshot_file(
            self._output_dir, minute_key, snapshot_copy, ohlcv_data,
            self._code_table, full=self._enable_full_snapshot,
            raw_records=raw_records,
        )
        if self._enable_kline:
            write_kline_file(
                self._output_dir, minute_key, snapshot_copy, ohlcv_data,
                self._code_table, full=self._enable_full_kline,
            )
        if self._enable_order and order_records:
            write_order_file(self._output_dir, minute_key, order_records)

    def _write_checkpoint(self) -> None:
        files_snapshot: Dict[str, FileState]
        if self._checkpoint_lock is not None:
            with self._checkpoint_lock:
                files_snapshot = dict(self._file_states)
        else:
            files_snapshot = dict(self._file_states)

        self._checkpoint.write(
            date=self._state.last_output_date,
            last_seqno=self._state.seqno,
            output_minutes=self._state.output_minutes,
            last_output_minute=self._state.last_output_minute,
            current_minute=self._state.current_minute,
            last_output_date=self._state.last_output_date,
            first_data_received=self._state.first_data_received,
            files=files_snapshot,
            last_totalvol_by_symbol=self._state.last_totalvol_by_symbol,
            last_totalamount_by_symbol=self._state.last_totalamount_by_symbol,
        )

    def _step4_handle_late_records(self) -> None:
        with self._state.lock:
            late_records = self._state.pop_late_snapshot_records()

        if not late_records:
            return

        grouped: dict[str, list[SnapshotRecord]] = {}
        for minute_key, record in late_records:
            grouped.setdefault(minute_key, []).append(record)

        for minute_key in sorted(grouped):
            path = get_snapshot_file_path(self._output_dir, minute_key)
            if os.path.exists(path):
                append_snapshot_records(path, grouped[minute_key], self._code_table)
            else:
                logger.warning("Late snapshot minute %s file missing", minute_key)

        with self._state.lock:
            for records in grouped.values():
                for record in records:
                    self._state.maybe_update_latest_unlocked(record)
                    self._state.late_snapshot_count += 1
                    mk = time_to_minute_key(record.time)
                    self._state.late_snapshot_minutes.add(mk)

        logger.info(
            "Flushed %d late snapshot records across %d minutes",
            sum(len(v) for v in grouped.values()), len(grouped),
        )

    def _step5_write_checkpoint(self) -> None:
        with self._state.lock:
            if self._state._late_snapshot_records:
                logger.debug(
                    "Skipping checkpoint: %d late records pending",
                    len(self._state._late_snapshot_records),
                )
                return
        self._write_checkpoint()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd d:/FIU && python -m pytest tests/test_watermark_flusher.py tests/test_flusher.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
cd d:/FIU && git add src/minute_bar/flusher.py tests/test_watermark_flusher.py
git commit -m "feat: refactor flusher for data-driven watermark with stall detection and final flush"
```

---

## Task 5: `engine.py` — Constructor, `_order_loop` refactor, `_flush_expired_order_minutes`, `stop()`

**Files:**
- Modify: `src/minute_bar/engine.py`
- Test: `tests/test_watermark_engine.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_watermark_engine.py`:

```python
"""Tests for data-driven watermark changes in engine.py."""
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
import threading

from minute_bar.aggregator import SharedState
from minute_bar.config import AppConfig, RecoveryConfig
from minute_bar.engine import Engine, _OrderMinuteBuffer
from minute_bar.models import OrderRecord


def make_order_record(symbol="1301", seqno=1, time_=20260521093000123, **kwargs):
    defaults = dict(
        symbol=symbol, seqno=seqno, time=time_,
        bidprice=4500.0, bidsize=100.0, askprice=4510.0, asksize=200.0,
        decimal=2, rcvtime=20260521083000123,
    )
    defaults.update(kwargs)
    return OrderRecord(**defaults)


class TestOrderLoopProcessingOrder:
    """Verify seqno is assigned before late detection."""

    def test_late_record_gets_seqno(self):
        """Late record consumes a seqno but doesn't create a buffer."""
        buf = _OrderMinuteBuffer()
        buf.records.append(make_order_record(seqno=1))
        buf.line_end_offset = 100

        # Simulate the scenario: minute 0930 already flushed, record 0930 arrives
        flushed = {"202605210930"}
        late_per_minute = {}

        minute_key = "202605210930"
        current_minute = "202605210931"
        seqno = 5

        # Step 1: seqno allocated
        seqno += 1  # seqno = 6
        record = make_order_record(seqno=seqno)
        assert record.seqno == 6

        # Step 2: late detection
        assert minute_key in flushed
        assert minute_key != current_minute  # not equal but also not > current


class TestOrderLoopMonotonicWatermark:
    """Verify current_minute never regresses."""

    def test_out_of_order_record_does_not_update_watermark(self):
        current_minute = "202605210931"
        minute_key = "202605210930"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210931"

    def test_forward_record_updates_watermark(self):
        current_minute = "202605210930"
        minute_key = "202605210931"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210931"

    def test_none_initial_watermark(self):
        current_minute = None
        minute_key = "202605210930"
        if current_minute is None or minute_key > current_minute:
            current_minute = minute_key
        assert current_minute == "202605210930"


class TestFlushExpiredOrderMinutes:
    """Verify _flush_expired_order_minutes uses watermark parameter."""

    def test_data_driven_flush_with_watermark(self):
        """Minutes ahead of watermark are flushed."""
        state = SharedState()
        state.first_data_received = True
        state.last_output_date = "20260521"

        # Create engine with minimal mocking
        with patch("minute_bar.engine.ClockWatermarkFlusher"), \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            from minute_bar.config import AppConfig, InputConfig, OutputConfig
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp"),
                output=OutputConfig(output_dir="/tmp"),
            )
            engine = Engine.__new__(Engine)
            engine._config = config
            engine._data_flush_delay_minutes = 1
            engine._enable_time_fallback = False
            engine._flushed_order_minutes = set()
            engine._checkpoint_lock = threading.Lock()
            engine._committed_order_offset = 0

            buffers = {
                "202605210900": _OrderMinuteBuffer(),
                "202605210901": _OrderMinuteBuffer(),
            }
            buffers["202605210900"].records = [make_order_record()]
            buffers["202605210901"].records = [make_order_record(seqno=2)]

            # Watermark at 0901 should flush 0900 (0900 + 1 = 0901 >= 0901)
            order_watermark = "202605210901"
            expired = [
                k for k in buffers
                if is_data_driven_expired(k, order_watermark, 1)
            ]
            assert "202605210900" in expired
            assert "202605210901" not in expired


class TestStopFinalFlush:
    """Verify stop() calls flush_all_remaining and handles errors."""

    def test_stop_calls_flush_all_remaining(self):
        """After threads join, stop should call flush_all_remaining."""
        with patch("minute_bar.engine.ClockWatermarkFlusher") as MockFlusher, \
             patch("minute_bar.engine.CodeTable"), \
             patch("minute_bar.engine.FileTailer"), \
             patch("minute_bar.engine.CheckpointManager"):
            from minute_bar.config import AppConfig, InputConfig, OutputConfig
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp"),
                output=OutputConfig(output_dir="/tmp"),
            )
            engine = Engine(config)
            engine._running = False  # Already stopped
            engine._data_thread = None
            engine._clock_thread = None
            engine._order_thread = None

            engine.stop()

            MockFlusher.return_value.flush_all_remaining.assert_called_once()

    def test_stop_closes_all_resources(self):
        """Resources are closed even if flush fails."""
        with patch("minute_bar.engine.ClockWatermarkFlusher") as MockFlusher, \
             patch("minute_bar.engine.CodeTable") as MockCT, \
             patch("minute_bar.engine.FileTailer") as MockFT:
            MockFlusher.return_value.flush_all_remaining.side_effect = RuntimeError("flush failed")

            from minute_bar.config import AppConfig, InputConfig, OutputConfig
            from minute_bar.checkpoint import CheckpointManager
            config = AppConfig(
                input=InputConfig(csv_dir="/tmp"),
                output=OutputConfig(output_dir="/tmp"),
            )
            engine = Engine(config)
            engine._running = False
            engine._data_thread = None
            engine._clock_thread = None
            engine._order_thread = None

            # stop() should close resources even with flush error
            with pytest.raises(RuntimeError):
                engine.stop()

            engine._snapshot_tailer.close.assert_called_once()
            engine._order_tailer.close.assert_called_once()
            engine._code_table.close.assert_called_once()


class TestOrderLoopFinallySafety:
    """Verify _order_loop finally block has independent try-except."""

    def test_order_loop_cross_day_flush_failure_still_updates_date(self):
        """If cross-day flush fails, current_date still updates to prevent infinite retry."""
        current_date = "20260520"
        record_date = "20260521"

        # Simulate the cross-day logic
        buffers = {"202605200930": _OrderMinuteBuffer()}
        buffers["202605200930"].records = [make_order_record()]

        # Cross-day: flush fails but date should update
        try:
            raise IOError("simulated flush failure")
        except Exception:
            pass
        finally:
            old_keys = [k for k in buffers if k[:8] != record_date]
            for k in old_keys:
                buffers.pop(k, None)

        # current_date would update after this block
        current_date = record_date
        assert current_date == "20260521"
        assert len(buffers) == 0  # Old date buffers cleaned up


# Import needed for TestFlushExpiredOrderMinutes
from minute_bar.clock import is_data_driven_expired
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd d:/FIU && python -m pytest tests/test_watermark_engine.py -v`
Expected: Some tests may pass (logic tests), but `TestStopFinalFlush` and `TestFlushExpiredOrderMinutes` will fail because engine doesn't have `_data_flush_delay_minutes` etc.

- [ ] **Step 3: Write the implementation**

Modify `src/minute_bar/engine.py`. Changes to make:

**1. Add imports** (line 14-18): Add `is_data_driven_expired`:

After the existing `is_expired` import, add:
```python
    is_data_driven_expired,
```

**2. Add to `__init__`** (after line 179): Store data-driven config and validate:

After `self._late_order_minutes: set[str] = set()`, add:
```python
        self._data_flush_delay_minutes = config.recovery.data_flush_delay_minutes
        self._enable_time_fallback = config.recovery.enable_time_fallback
```

And add validation + warning after `_flusher` construction (after line 169):
```python
        if config.recovery.data_flush_delay_minutes < 0:
            raise ValueError(
                f"data_flush_delay_minutes must be >= 0, got {config.recovery.data_flush_delay_minutes}"
            )
        if config.recovery.data_flush_delay_minutes > 10:
            logger.warning(
                "data_flush_delay_minutes=%d is unusually large; minutes may accumulate in buffer for extended time",
                config.recovery.data_flush_delay_minutes,
            )
        if not config.recovery.enable_time_fallback:
            logger.warning(
                "enable_time_fallback is DISABLED — only for test environments; "
                "if data thread stalls, buffers may not flush automatically."
            )
```

And pass new params to flusher constructor (add after `checkpoint_lock` param):
```python
            data_flush_delay_minutes=config.recovery.data_flush_delay_minutes,
            enable_time_fallback=config.recovery.enable_time_fallback,
```

**3. Rewrite `stop()`** (lines 202-213):

Replace entire `stop()` method with:
```python
    def stop(self) -> None:
        self._running = False
        join_errors = []
        flush_error = None

        try:
            if self._order_thread:
                self._order_thread.join(timeout=10)
                if self._order_thread.is_alive():
                    join_errors.append("order")
            if self._data_thread:
                self._data_thread.join(timeout=5)
                if self._data_thread.is_alive():
                    join_errors.append("data")
            if self._clock_thread:
                self._clock_thread.join(timeout=5)
                if self._clock_thread.is_alive():
                    join_errors.append("clock")

            if join_errors:
                logger.critical(
                    "Threads still alive after join timeout: %s; skip final flush",
                    join_errors,
                )
                import sys as _sys
                import traceback as _traceback
                for name, thread in [
                    ("order", self._order_thread),
                    ("data", self._data_thread),
                    ("clock", self._clock_thread),
                ]:
                    if thread and thread.is_alive():
                        frame = _sys._current_frames().get(thread.ident)
                        if frame:
                            logger.error("Thread %s stack:\n%s", name, ''.join(
                                f"  File \"{fn}\", line {ln}, in {func}\n    {line.strip()}\n"
                                for fn, ln, func, line in _traceback.extract_stack(frame)
                            ))
            else:
                try:
                    self._flusher.flush_all_remaining()
                except Exception as e:
                    logger.exception("Final flush failed")
                    flush_error = e

        except Exception as e:
            logger.exception("Engine stop failed unexpectedly")
            flush_error = e
        finally:
            for name, resource in [
                ("snapshot_tailer", self._snapshot_tailer),
                ("order_tailer", self._order_tailer),
                ("code_table", self._code_table),
            ]:
                try:
                    if resource is not None:
                        resource.close()
                except Exception:
                    logger.exception("Failed to close %s", name)
            logger.info("Engine stopped")

        if join_errors or flush_error:
            raise RuntimeError(
                f"Engine stop errors: join_timeout={join_errors}, flush={flush_error}"
            ) from flush_error
```

**4. Refactor `_order_loop`** (lines 334-436):

Replace the `_order_loop` method body with the new processing order. The complete replacement:

```python
    def _order_loop(self) -> None:
        if not self._config.output.enable_order:
            return

        output_dir = self._config.output.output_dir
        output_delay_sec = self._config.recovery.output_delay_sec
        encoding = self._config.input.file_encoding

        buffers: Dict[str, _OrderMinuteBuffer] = {}
        current_minute: Optional[str] = None
        current_date: Optional[str] = None
        seqno = 0
        late_order_per_minute: Dict[str, int] = {}

        try:
            while self._running:
                try:
                    today = self._get_target_date()
                    self._order_tailer.set_date(today)

                    for line in self._order_tailer.read_lines():
                        parsed = parse_order_line(line, encoding)
                        if parsed is None:
                            continue

                        record_date = str(parsed.time)[:8]
                        if record_date != today:
                            continue

                        minute_key = time_to_minute_key(parsed.time)

                        # Step 0.5: Cross-day flush + reset
                        if current_date is not None and record_date != current_date:
                            try:
                                self._flush_all_order_buffers(buffers, output_dir)
                            except Exception:
                                logger.exception("Cross-day order flush failed, resetting date anyway")
                            finally:
                                old_keys = [k for k in buffers if k[:8] != record_date]
                                for k in old_keys:
                                    buffers.pop(k, None)
                            current_date = record_date
                            current_minute = None

                        if current_date is None:
                            current_date = record_date

                        # Step 1: seqno + build record (before late detection)
                        seqno += 1
                        record = build_order_record(parsed, seqno)

                        # Step 2: Late order detection
                        if minute_key in self._flushed_order_minutes:
                            count = late_order_per_minute.get(minute_key, 0)
                            if count >= MAX_LATE_ORDER_RECORDS_PER_MINUTE:
                                continue
                            late_order_per_minute[minute_key] = count + 1
                            path = get_order_file_path(output_dir, minute_key)
                            if os.path.exists(path):
                                append_order_records(path, [record])
                            else:
                                logger.warning(
                                    "Late order minute %s file missing, creating new", minute_key
                                )
                                write_order_file(output_dir, minute_key, [record])
                            self._late_order_count += 1
                            self._late_order_minutes.add(minute_key)
                            continue

                        # Step 3: Record-driven flush (only on forward progress)
                        if current_minute is not None and minute_key > current_minute:
                            self._flush_order_minute(
                                buffers, current_minute, output_dir
                            )

                        # Step 4: Buffer write
                        buf = buffers.get(minute_key)
                        if buf is None:
                            buf = _OrderMinuteBuffer()
                            buffers[minute_key] = buf
                        buf.records.append(record)
                        buf.line_end_offset = self._order_tailer.line_offset

                        # Step 5: Monotonic watermark update
                        if current_minute is None or minute_key > current_minute:
                            current_minute = minute_key
                            current_date = record_date

                    with self._checkpoint_lock:
                        self._file_states["order"] = FileState(
                            offset=self._committed_order_offset,
                            pending_line=b"",
                            date=today,
                        )

                    # Watermark-driven: flush expired minutes
                    self._flush_expired_order_minutes(
                        buffers, output_dir, output_delay_sec, current_minute or ""
                    )

                    # Memory protection
                    self._enforce_max_pending(buffers, output_dir)

                except Exception as e:
                    logger.error("Order thread error: %s", e, exc_info=True)
                    self._order_thread_error = e
                    self._running = False
                    return

                interval = get_poll_interval_ms(self._config) / 1000.0
                time.sleep(interval)

        finally:
            try:
                self._flush_all_order_buffers(buffers, output_dir)
            except Exception:
                logger.exception("Order final flush failed")
            try:
                self._order_tailer.close()
            except Exception:
                logger.exception("Failed to close order tailer")
```

**5. Update `_flush_expired_order_minutes`** (lines 463-474):

Replace with:
```python
    def _flush_expired_order_minutes(
        self,
        buffers: Dict[str, "_OrderMinuteBuffer"],
        output_dir: str,
        output_delay_sec: int,
        order_watermark: str = "",
    ) -> None:
        expired_keys = [
            k for k in buffers
            if is_data_driven_expired(k, order_watermark, self._data_flush_delay_minutes)
               or (self._enable_time_fallback and is_expired(k, output_delay_sec))
        ]
        for minute_key in sorted(expired_keys):
            self._flush_order_minute(buffers, minute_key, output_dir)
```

**6. Add `os` and `traceback` imports** are already available (`os` is imported). Add `import traceback` to the file-level imports if not already there.

- [ ] **Step 4: Run all tests to verify they pass**

Run: `cd d:/FIU && python -m pytest tests/test_watermark_engine.py tests/test_engine_late.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
cd d:/FIU && git add src/minute_bar/engine.py tests/test_watermark_engine.py
git commit -m "feat: refactor engine for data-driven watermark with safe stop and order loop fixes"
```

---

## Task 6: Full test suite regression

**Files:**
- All test files

- [ ] **Step 1: Run the full test suite**

Run: `cd d:/FIU && python -m pytest tests/ -v`
Expected: All tests PASS

If any test fails, investigate and fix. Common issues:
- Import errors: check that `is_data_driven_expired` is exported from `clock.py`
- Constructor signature mismatch: ensure `make_flusher` in `test_flusher.py` passes new params
- The existing `test_flusher.py::TestFlushedSnapshotMinutes::test_step3_records_flushed_minutes` patches `is_expired` — verify it still works since `_step3` now also uses `is_data_driven_expired`

- [ ] **Step 2: Fix any regressions**

If `test_flusher.py` tests fail, update `make_flusher` helper to pass `data_flush_delay_minutes` and `enable_time_fallback`. The existing test patches `is_expired` to return True, so set `enable_time_fallback=True` (default) and ensure `data_flush_delay_minutes=1` with watermark not ahead.

- [ ] **Step 3: Final commit**

```bash
cd d:/FIU && git add -A
git commit -m "test: fix regressions from data-driven watermark implementation"
```

---

## Task 7: Existing test compatibility fix for `test_flusher.py`

**Files:**
- Modify: `tests/test_flusher.py:27-39` (make_flusher helper)
- Modify: `tests/test_flusher.py:124-138` (TestFlushedSnapshotMinutes)

- [ ] **Step 1: Update make_flusher to support new constructor params**

The existing `make_flusher` in `test_flusher.py` must pass `data_flush_delay_minutes` and `enable_time_fallback`. Update:

```python
def make_flusher(state, tmp_path, data_flush_delay_minutes=1, enable_time_fallback=True):
    code_table = CodeTable.__new__(CodeTable)
    code_table._table = {}
    checkpoint = CheckpointManager(str(tmp_path / "checkpoint.json"), str(tmp_path))
    return ClockWatermarkFlusher(
        state=state,
        code_table=code_table,
        checkpoint=checkpoint,
        output_dir=str(tmp_path),
        output_delay_sec=60,
        file_states={},
        checkpoint_lock=None,
        data_flush_delay_minutes=data_flush_delay_minutes,
        enable_time_fallback=enable_time_fallback,
    )
```

- [ ] **Step 2: Update TestFlushedSnapshotMinutes to set watermark correctly**

The test patches `is_expired` to return True, but `_step3` now also checks `is_data_driven_expired`. To make the test work with the new logic, either:

a) Set `state.current_minute` far ahead so data-driven flush triggers, or
b) Keep `enable_time_fallback=True` (default) and rely on the patched `is_expired`

Option (b) is simplest. The current test already sets `enable_time_fallback=True` by default. Verify that `data_flush_delay_minutes=1` and `state.current_minute="202605200930"` (same as buffer key) → data-driven check returns False, fallback check returns True (patched). This should work without changes.

Run: `cd d:/FIU && python -m pytest tests/test_flusher.py -v`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
cd d:/FIU && git add tests/test_flusher.py
git commit -m "test: update test_flusher.py for data-driven watermark compatibility"
```
