#!/usr/bin/env python3
"""FIU 日股分钟级行情数据生成器 — 主入口"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import signal
import sys

from minute_bar.config import load_config
from minute_bar.engine import Engine
from minute_bar.replay import ReplayEngine


def _handle_stop_signal(signum, frame):
    """Translate SIGTERM (and SIGINT) to KeyboardInterrupt so engine.start()'s
    except-KeyboardInterrupt path runs the graceful stop (flush + tickfile drain +
    commit-marker finalize). Without this, stop.sh / systemd SIGTERM force-kills
    the process and the commit-marker finalization never runs."""
    raise KeyboardInterrupt


def install_stop_signal_handler() -> None:
    """Register SIGTERM → KeyboardInterrupt so deploy stop (stop.sh / systemd)
    triggers graceful shutdown. Idempotent."""
    signal.signal(signal.SIGTERM, _handle_stop_signal)


def setup_logging(config) -> None:
    log_dir = config.logging.error_log_dir
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.logging.log_level.upper(), logging.INFO))

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_dir:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(log_dir, f"{date_str}_errors.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=config.logging.max_file_size_mb * 1024 * 1024,
            backupCount=config.logging.max_backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="FIU Minute Bar Generator")
    parser.add_argument("--config", required=True, help="Path to config.ini")
    parser.add_argument("--replay", metavar="YYYYMMDD", help="Replay mode: process historical data for given date")
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except (ValueError, FileNotFoundError) as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(2)

    setup_logging(config)
    logger = logging.getLogger(__name__)
    logger.info("Input dir: %s", config.input.csv_dir)
    logger.info("Output dir: %s", config.output.output_dir)

    install_stop_signal_handler()

    try:
        if args.replay:
            logger.info("Replay mode: date=%s", args.replay)
            engine = ReplayEngine(config, date=args.replay)
            engine.run()
        else:
            logger.info("Starting FIU Minute Bar Generator (live mode)")
            engine = Engine(config)
            engine.start()
    except SystemExit as e:
        logger.fatal("Engine exited with code %d", e.code)
        sys.exit(e.code)
    except Exception as e:
        logger.fatal("Unexpected error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
