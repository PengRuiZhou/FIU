from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from minute_bar.config import AppConfig, SessionConfig

JST = timezone(timedelta(hours=9))
CST = timezone(timedelta(hours=8))


def now_jst() -> datetime:
    return datetime.now(JST)


def now_cst() -> datetime:
    return datetime.now(CST)


def jst_now_hhmm() -> str:
    return now_jst().strftime("%H%M")


def jst_now_yyyymmdd() -> str:
    return now_jst().strftime("%Y%m%d")


def jst_now_yyyymmddhhmm() -> str:
    return now_jst().strftime("%Y%m%d%H%M")


def extract_date_from_minute_key(minute_key: str) -> str:
    return minute_key[:8]


def extract_hhmm_from_minute_key(minute_key: str) -> str:
    return minute_key[8:12]


def minute_key_to_end_time(minute_key: str) -> datetime:
    """Round-up: bar M covers [(M-1):00, M:00); the end/cutoff is M's own moment."""
    yyyymmdd = minute_key[:8]
    hhmm = minute_key[8:12]
    hh, mm = int(hhmm[:2]), int(hhmm[2:])
    date_part = datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=JST)
    return date_part.replace(hour=hh, minute=mm)


def time_to_minute_key(time_17digit: int) -> str:
    """Round-UP: a timestamp marks a minute-end snapshot, so it belongs to the NEXT minute.

    09:00:01.000 → '0901' | 09:59:xx → '1000' | 23:59:xx → next-day '0000'.
    See spec 2026-06-16-minute-key-round-up-design.
    """
    s = str(time_17digit)
    if len(s) < 12:
        return s[:12]  # malformed input — preserve prior best-effort behavior
    base = datetime.strptime(s[:12], "%Y%m%d%H%M")
    return (base + timedelta(minutes=1)).strftime("%Y%m%d%H%M")


def is_trading_minute(minute_key: str, session: SessionConfig) -> bool:
    hhmm = extract_hhmm_from_minute_key(minute_key)
    return (session.morning_open <= hhmm <= session.morning_close) or (
        session.afternoon_open <= hhmm <= session.afternoon_close
    )


def is_expired(minute_key: str, delay_sec: int) -> bool:
    end_time = minute_key_to_end_time(minute_key)
    return end_time + timedelta(seconds=delay_sec) <= now_jst()


# Kept for backward compat; flusher now uses configurable stall_flush_sec (RecoveryConfig).
STALL_WARN_SECONDS = 300


def minute_key_to_start_time(minute_key: str) -> datetime:
    """Round-up: bar M covers [(M-1):00, M:00); the start is M-1 minute."""
    if len(minute_key) != 12 or not minute_key.isdigit():
        raise ValueError(f"Invalid minute_key format: '{minute_key}', expected 12-digit YYYYMMDDHHMM")
    return datetime.strptime(minute_key, "%Y%m%d%H%M").replace(tzinfo=JST) - timedelta(minutes=1)


def is_data_driven_expired(minute_key: str, data_watermark: str, delay_minutes: int) -> bool:
    """Check if minute_key should be flushed based on data progress watermark."""
    if not data_watermark:
        return False
    watermark_dt = minute_key_to_start_time(data_watermark)
    threshold = minute_key_to_start_time(minute_key) + timedelta(minutes=delay_minutes)
    return watermark_dt >= threshold


def is_yesterday(minute_key: str, current_date: str) -> bool:
    return extract_date_from_minute_key(minute_key) != current_date


def get_poll_interval_ms(config: AppConfig) -> int:
    hhmm = jst_now_hhmm()
    s = config.session
    if s.pre_market_start <= hhmm < s.morning_open:
        return config.input.buffer_poll_interval_ms
    if s.morning_open <= hhmm < s.morning_close:
        return config.input.poll_interval_ms
    if s.morning_close <= hhmm < s.afternoon_open:
        return config.input.idle_poll_interval_ms
    if s.afternoon_open <= hhmm < s.post_market_end:
        return config.input.poll_interval_ms
    return config.input.idle_poll_interval_ms


def parse_17digit_time(ts: int) -> datetime:
    s = str(ts)
    return datetime(
        year=int(s[0:4]),
        month=int(s[4:6]),
        day=int(s[6:8]),
        hour=int(s[8:10]),
        minute=int(s[10:12]),
        second=int(s[12:14]),
        microsecond=int(s[14:17]) * 1000,
        tzinfo=JST,
    )


def current_system_timestamp_17digit(tz: timezone = CST) -> int:
    now = datetime.now(tz)
    ms = now.microsecond // 1000
    return int(now.strftime("%Y%m%d%H%M%S") + f"{ms:03d}")
