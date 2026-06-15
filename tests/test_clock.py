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
