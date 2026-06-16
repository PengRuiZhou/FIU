"""Tests for data-driven watermark functions in clock.py."""
import pytest
from datetime import timedelta

from minute_bar.clock import (
    JST,
    is_data_driven_expired,
    minute_key_to_start_time,
    time_to_minute_key,
)


class TestMinuteKeyToStartTime:
    """Round-up: bar M covers [(M-1):00, M:00), so start_time(M) = M-1 minute."""

    def test_valid_key(self):
        # bar 0900 starts at 08:59
        dt = minute_key_to_start_time("202605210900")
        assert dt.hour == 8
        assert dt.minute == 59
        assert dt.tzinfo == JST

    def test_midnight(self):
        # bar 0000 starts at previous-day 23:59
        dt = minute_key_to_start_time("202605210000")
        assert dt.hour == 23
        assert dt.minute == 59

    def test_end_of_day(self):
        # bar 2359 starts at 23:58
        dt = minute_key_to_start_time("202605202359")
        assert dt.hour == 23
        assert dt.minute == 58

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
        # bar 0000 starts at prev-day 23:59; +1min crosses into day 21 00:00
        dt = minute_key_to_start_time("202605210000")
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


class TestTimeToMinuteKeyRoundUp:
    """Left-open right-closed: bar M covers ((M-1):00.000, M:00.000].

    Exact-minute-boundary timestamps (SSMMM=0) belong to their OWN minute;
    sub-minute timestamps (>0) round up to the NEXT minute.
    """

    def test_second_after_minute_start(self):
        # 09:00:01.000 (sub>0) → 0901
        assert time_to_minute_key(20260528090001000) == "202605280901"

    def test_just_before_minute_end(self):
        # 09:00:59.000 (sub>0) → 0901
        assert time_to_minute_key(20260528090059000) == "202605280901"

    def test_exact_minute_boundary_stays(self):
        # 09:01:00.000 (exact boundary, SSMMM=0) → 0901 (own minute, NOT +1)
        assert time_to_minute_key(20260528090100000) == "202605280901"

    def test_close_timestamp_at_1530(self):
        # 15:30:00.000 (the session close, exact boundary) → 1530 (NOT 1531)
        assert time_to_minute_key(20260528153000000) == "202605281530"

    def test_sub_minute_after_close_rounds_up(self):
        # 15:30:01.000 (sub>0, not the exact close) → 1531
        assert time_to_minute_key(20260528153001000) == "202605281531"

    def test_cross_hour_carry(self):
        # 09:59:01.000 (sub>0) → 1000 (mm=59+1=60 → hh+1, mm=0)
        assert time_to_minute_key(20260528095901000) == "202605281000"

    def test_cross_day_carry_defensive(self):
        # 23:59:01.000 (sub>0) → next-day 0000 (hh=24 → date+1, hh=0)
        assert time_to_minute_key(20260528235901000) == "202605290000"
