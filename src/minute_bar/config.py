from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class InputConfig:
    csv_dir: str = ""
    poll_interval_ms: int = 200
    idle_poll_interval_ms: int = 5000
    buffer_poll_interval_ms: int = 1000
    chunk_size_bytes: int = 65536
    order_chunk_size_bytes: int = 65536
    file_encoding: str = "utf-8"
    target_date: str = ""
    enable_order_accel: bool = False  # Rust acceleration for order parse (default OFF)
    # Phase 21 flags
    enable_rust_order_full_batch: bool = False  # Full Rust pipeline for order (parse + group + buffer)
    enable_rust_snapshot_batch: bool = False     # Full Rust pipeline for snapshot (parse + aggregate)
    enable_rust_tickfile: bool = False          # Rust tickfile_generate (Part C)


@dataclass
class OutputConfig:
    output_dir: str = ""
    format: str = "csv"
    enable_kline: bool = True
    enable_full_snapshot: bool = True
    enable_full_kline: bool = True
    enable_order: bool = True
    enable_tickfile: bool = False


@dataclass
class AggregationConfig:
    first_seen_volume_base: str = "start_totalvol"


@dataclass
class TimezoneConfig:
    exchange_tz: str = "Asia/Tokyo"
    local_tz: str = "Asia/Shanghai"


@dataclass
class SessionConfig:
    morning_open: str = "0900"
    morning_close: str = "1130"
    afternoon_open: str = "1230"
    afternoon_close: str = "1500"
    pre_market_start: str = "0800"
    post_market_end: str = "1530"


@dataclass
class RecoveryConfig:
    checkpoint_file: str = "checkpoint.json"
    output_delay_sec: int = 5
    code_refresh_sec: int = 30
    data_flush_delay_minutes: int = 1
    enable_time_fallback: bool = True
    stall_flush_sec: int = 300
    # Maximum late order records per minute before discarding.
    # Applies to ALL modes (live, replay), not just tickfile mode.
    # WARNING: Each record uses ~300-400 bytes of Python memory (frozen dataclass overhead).
    # At 1,000,000 records, peak memory per minute is ~400 MB.
    # If multiple minutes accumulate late records simultaneously, total could reach 2-3 GB.
    # Production real-time: late records are minimal (<1000/min); this cap is a safety valve.
    # 100x speed test: busiest minute (0900) has ~750K records; 1M gives 33% headroom.
    max_late_order_records_per_minute: int = 1000000
    # Tickfile commit-marker + truncate recovery (spec 2026-06-17).
    # When True: write sidecar commit file + fcntl.flock; recover via sidecar + truncate.
    # When False: legacy behavior (no sidecar/flock; row-based fallback recovery).
    # Process-static: read once at __init__; change requires restart (INV-CM-KILLSWITCH-CONSISTENCY).
    enable_tickfile_commit_marker: bool = True


@dataclass
class LoggingConfig:
    error_log_dir: str = "errors/"
    max_file_size_mb: int = 100
    max_backup_count: int = 5
    log_level: str = "INFO"
    structured: bool = False  # emit file-handler logs as JSON lines (machine-parseable)


@dataclass
class AppConfig:
    input: InputConfig = field(default_factory=InputConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    aggregation: AggregationConfig = field(default_factory=AggregationConfig)
    timezone: TimezoneConfig = field(default_factory=TimezoneConfig)
    session: SessionConfig = field(default_factory=SessionConfig)
    recovery: RecoveryConfig = field(default_factory=RecoveryConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_config(path: str | Path) -> AppConfig:
    cfg = AppConfig()
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")

    if parser.has_section("input"):
        s = parser["input"]
        cfg.input.csv_dir = s.get("csv_dir", cfg.input.csv_dir)
        cfg.input.poll_interval_ms = s.getint("poll_interval_ms", cfg.input.poll_interval_ms)
        cfg.input.idle_poll_interval_ms = s.getint("idle_poll_interval_ms", cfg.input.idle_poll_interval_ms)
        cfg.input.buffer_poll_interval_ms = s.getint("buffer_poll_interval_ms", cfg.input.buffer_poll_interval_ms)
        cfg.input.chunk_size_bytes = s.getint("chunk_size_bytes", cfg.input.chunk_size_bytes)
        cfg.input.order_chunk_size_bytes = s.getint("order_chunk_size_bytes", cfg.input.order_chunk_size_bytes)
        cfg.input.file_encoding = s.get("file_encoding", cfg.input.file_encoding)
        cfg.input.target_date = s.get("target_date", cfg.input.target_date)
        cfg.input.enable_order_accel = s.getboolean("enable_order_accel", cfg.input.enable_order_accel)
        cfg.input.enable_rust_order_full_batch = s.getboolean(
            "enable_rust_order_full_batch", cfg.input.enable_rust_order_full_batch
        )
        cfg.input.enable_rust_snapshot_batch = s.getboolean(
            "enable_rust_snapshot_batch", cfg.input.enable_rust_snapshot_batch
        )
        cfg.input.enable_rust_tickfile = s.getboolean(
            "enable_rust_tickfile", cfg.input.enable_rust_tickfile
        )

    if parser.has_section("output"):
        s = parser["output"]
        cfg.output.output_dir = s.get("output_dir", cfg.output.output_dir)
        cfg.output.format = s.get("format", cfg.output.format)
        cfg.output.enable_kline = s.getboolean("enable_kline", cfg.output.enable_kline)
        cfg.output.enable_full_snapshot = s.getboolean("enable_full_snapshot", cfg.output.enable_full_snapshot)
        cfg.output.enable_full_kline = s.getboolean("enable_full_kline", cfg.output.enable_full_kline)
        cfg.output.enable_order = s.getboolean("enable_order", cfg.output.enable_order)
        cfg.output.enable_tickfile = s.getboolean("enable_tickfile", cfg.output.enable_tickfile)

    if parser.has_section("aggregation"):
        s = parser["aggregation"]
        cfg.aggregation.first_seen_volume_base = s.get("first_seen_volume_base", cfg.aggregation.first_seen_volume_base)

    if parser.has_section("timezone"):
        s = parser["timezone"]
        cfg.timezone.exchange_tz = s.get("exchange_tz", cfg.timezone.exchange_tz)
        cfg.timezone.local_tz = s.get("local_tz", cfg.timezone.local_tz)

    if parser.has_section("session"):
        s = parser["session"]
        cfg.session.morning_open = s.get("morning_open", cfg.session.morning_open)
        cfg.session.morning_close = s.get("morning_close", cfg.session.morning_close)
        cfg.session.afternoon_open = s.get("afternoon_open", cfg.session.afternoon_open)
        cfg.session.afternoon_close = s.get("afternoon_close", cfg.session.afternoon_close)
        cfg.session.pre_market_start = s.get("pre_market_start", cfg.session.pre_market_start)
        cfg.session.post_market_end = s.get("post_market_end", cfg.session.post_market_end)

    if parser.has_section("recovery"):
        s = parser["recovery"]
        cfg.recovery.checkpoint_file = s.get("checkpoint_file", cfg.recovery.checkpoint_file)
        cfg.recovery.output_delay_sec = s.getint("output_delay_sec", cfg.recovery.output_delay_sec)
        cfg.recovery.code_refresh_sec = s.getint("code_refresh_sec", cfg.recovery.code_refresh_sec)
        cfg.recovery.data_flush_delay_minutes = s.getint("data_flush_delay_minutes", cfg.recovery.data_flush_delay_minutes)
        cfg.recovery.enable_time_fallback = s.getboolean("enable_time_fallback", cfg.recovery.enable_time_fallback)
        cfg.recovery.stall_flush_sec = s.getint("stall_flush_sec", cfg.recovery.stall_flush_sec)
        cfg.recovery.enable_tickfile_commit_marker = s.getboolean(
            "enable_tickfile_commit_marker", cfg.recovery.enable_tickfile_commit_marker
        )
        cfg.recovery.max_late_order_records_per_minute = s.getint(
            "max_late_order_records_per_minute",
            cfg.recovery.max_late_order_records_per_minute
        )

    if parser.has_section("logging"):
        s = parser["logging"]
        cfg.logging.error_log_dir = s.get("error_log_dir", cfg.logging.error_log_dir)
        cfg.logging.max_file_size_mb = s.getint("max_file_size_mb", cfg.logging.max_file_size_mb)
        cfg.logging.max_backup_count = s.getint("max_backup_count", cfg.logging.max_backup_count)
        cfg.logging.log_level = s.get("log_level", cfg.logging.log_level)
        cfg.logging.structured = s.getboolean("structured", cfg.logging.structured)

    if not cfg.input.csv_dir:
        raise ValueError("config [input] csv_dir is required")
    if not cfg.output.output_dir:
        raise ValueError("config [output] output_dir is required")

    return cfg
