from __future__ import annotations

import glob
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

MAX_SLEEP_SEC = 0.5

FILE_PATTERN = re.compile(r"^(order|snapshot|code)\.csv\.(\d{8})$")


def _extract_timestamp(line: str, file_type: str) -> int:
    """Extract the 17-digit timestamp from a CSV line based on file type.

    order/snapshot: column index 1 (the 'time' field)
    code: column index 14 (the 'rcvtime' field), fallback 0
    """
    fields = line.split(",")
    try:
        if file_type in ("order", "snapshot"):
            return int(fields[1].strip())
        # code.csv — rcvtime is column 14
        if len(fields) > 14 and fields[14].strip():
            return int(fields[14].strip())
    except (ValueError, IndexError):
        pass
    return 0


@dataclass
class _LateRecord:
    line: str
    enqueue_time: float
    file_type: str


class _LateWriter:
    """Background thread that drains delayed late records."""

    def __init__(self, stop_event: threading.Event, fsync: bool = False) -> None:
        self._stop_event = stop_event
        self._fsync = fsync
        self._lock = threading.Lock()
        self._records: List[_LateRecord] = []
        self._file_handles: dict[str, object] = {}
        self._write_locks: dict[str, threading.Lock] = {}
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="late-writer", daemon=True)
        self._thread.start()

    def register_handle(self, file_type: str, handle: object, write_lock: threading.Lock) -> None:
        self._file_handles[file_type] = handle
        self._write_locks[file_type] = write_lock

    def enqueue(self, record: _LateRecord) -> None:
        with self._lock:
            self._records.append(record)

    def _run(self) -> None:
        while not self._stop_event.is_set() or self._records:
            with self._lock:
                now = time.monotonic()
                ready = [r for r in self._records if now - r.enqueue_time >= 0]
                self._records = [r for r in self._records if now - r.enqueue_time < 0]

            for rec in ready:
                fh = self._file_handles.get(rec.file_type)
                wl = self._write_locks.get(rec.file_type)
                if fh is not None and wl is not None:
                    with wl:
                        fh.write(rec.line)
                        fh.flush()
                        if self._fsync:
                            os.fsync(fh.fileno())
                    logger.debug("Late write (%s): %s", rec.file_type, rec.line[:80])

            time.sleep(0.01)

    def join(self, timeout: float = 10.0) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)


class _BatchWriter:
    """Buffered writer that flushes on batch-size or flush-interval boundary."""

    def __init__(
        self,
        handle: object,
        batch_size: int,
        flush_interval_ms: int,
        fsync: bool = False,
        write_lock: Optional[threading.Lock] = None,
    ) -> None:
        self._handle = handle
        self._batch_size = batch_size
        self._flush_interval = flush_interval_ms / 1000.0
        self._fsync = fsync
        self._write_lock = write_lock
        self._count = 0
        self._last_flush = time.monotonic()

    def write(self, line: str) -> None:
        self._handle.write(line)
        self._count += 1
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        now = time.monotonic()
        if self._count >= self._batch_size or (now - self._last_flush) >= self._flush_interval:
            self.flush()

    def flush(self) -> None:
        if self._count == 0:
            return
        if self._write_lock:
            with self._write_lock:
                self._handle.flush()
                if self._fsync:
                    os.fsync(self._handle.fileno())
        else:
            self._handle.flush()
            if self._fsync:
                os.fsync(self._handle.fileno())
        self._count = 0
        self._last_flush = time.monotonic()

    @property
    def count(self) -> int:
        return self._count


def _scan_files(source_dir: str, file_types: list[str], date: str | None) -> dict[str, str]:
    """Find source files for each type. Returns {type: source_path}."""
    result = {}
    for ft in file_types:
        if date:
            pattern = os.path.join(source_dir, f"{ft}.csv.{date}")
            if os.path.exists(pattern):
                result[ft] = pattern
            else:
                logger.warning("Source file not found: %s", pattern)
        else:
            matches = sorted(glob.glob(os.path.join(source_dir, f"{ft}.csv.*")))
            if matches:
                result[ft] = matches[-1]
                logger.info("Auto-detected %s file: %s", ft, result[ft])
    return result


def _detect_date(source_files: dict[str, str]) -> str | None:
    """Extract date from source file paths."""
    for path in source_files.values():
        basename = os.path.basename(path)
        m = FILE_PATTERN.match(basename)
        if m:
            return m.group(2)
    return None


def _clean_target(output_dir: str, file_types: list[str], date: str | None) -> None:
    """Remove matching target CSV files, printing each one."""
    if not os.path.isdir(output_dir):
        return
    for ft in file_types:
        if date:
            target = os.path.join(output_dir, f"{ft}.csv.{date}")
            if os.path.exists(target):
                logger.info("Cleaning: %s", target)
                os.remove(target)
        else:
            for f in glob.glob(os.path.join(output_dir, f"{ft}.csv.*")):
                logger.info("Cleaning: %s", f)
                os.remove(f)


class Simulator:
    def __init__(
        self,
        source_dir: str,
        output_dir: str,
        speed: int = 100,
        date: str | None = None,
        file_types: list[str] | None = None,
        order_mode: str = "original",
        code_mode: str = "preload",
        split_line_prob: float = 0.0,
        split_line_delay_ms: int = 50,
        late_prob: float = 0.0,
        late_delay_sec: float = 10.0,
        batch_size: int = 1000,
        flush_interval_ms: int = 100,
        fsync: bool = False,
        clean: bool = True,
        speed_map: dict[str, int] | None = None,
    ) -> None:
        self._source_dir = source_dir
        self._output_dir = output_dir
        self._speed = speed
        self._speed_map: dict[str, int] = speed_map or {}  # per-file-type speed overrides
        self._date = date
        self._file_types = file_types or ["order", "snapshot", "code"]
        self._order_mode = order_mode
        self._code_mode = code_mode
        self._split_line_prob = split_line_prob
        self._split_line_delay_ms = split_line_delay_ms
        self._late_prob = late_prob
        self._late_delay_sec = late_delay_sec
        self._batch_size = batch_size
        self._flush_interval_ms = flush_interval_ms
        self._fsync = fsync
        self._clean = clean

        self._stop_event = threading.Event()
        self._late_writer = _LateWriter(self._stop_event, fsync=fsync)
        self._write_locks: dict[str, threading.Lock] = {}
        self._threads: list[threading.Thread] = []
        self._handles: dict[str, object] = {}
        self._global_min_time: int = 0
        self._real_start: float = 0.0

    def run(self) -> None:
        source_files = _scan_files(self._source_dir, self._file_types, self._date)
        if not source_files:
            logger.error("No source files found in %s", self._source_dir)
            return

        detected_date = _detect_date(source_files)
        if detected_date:
            self._date = detected_date
            logger.info("Replay date: %s", self._date)

        os.makedirs(self._output_dir, exist_ok=True)

        if self._clean:
            _clean_target(self._output_dir, self._file_types, self._date)

        # Compute global_min_time from order/snapshot only
        self._global_min_time = self._compute_global_min_time(source_files)
        logger.info("Global min time: %d", self._global_min_time)

        self._real_start = time.monotonic()
        self._late_writer.start()

        # Handle code preload
        if "code" in source_files and self._code_mode == "preload":
            self._preload_code(source_files["code"])
            stream_types = [ft for ft in self._file_types if ft != "code" and ft in source_files]
        else:
            stream_types = [ft for ft in self._file_types if ft in source_files]

        if not stream_types:
            logger.info("No streaming file types to replay")
            return

        # Open output file handles (newline="" prevents CRLF conversion on Windows)
        for ft in stream_types:
            target = os.path.join(self._output_dir, f"{ft}.csv.{self._date}")
            self._handles[ft] = open(target, "a", encoding="utf-8", newline="")
            self._write_locks[ft] = threading.Lock()
            self._late_writer.register_handle(ft, self._handles[ft], self._write_locks[ft])

        # Launch threads
        for ft in stream_types:
            t = threading.Thread(
                target=self._stream_file,
                args=(source_files[ft], ft, self._handles[ft]),
                name=f"replay-{ft}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()

        logger.info(
            "Simulator running: speed=%dx, order-mode=%s, %d threads",
            self._speed, self._order_mode, len(self._threads),
        )

        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1.0)
        except KeyboardInterrupt:
            pass

        self.stop()

    def stop(self) -> None:
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        self._late_writer.join()
        for t in self._threads:
            t.join(timeout=10)
        for fh in self._handles.values():
            fh.close()
        self._handles.clear()
        logger.info("Simulator stopped")

    def _compute_global_min_time(self, source_files: dict[str, str]) -> int:
        min_ts = float("inf")
        for ft in ("order", "snapshot"):
            path = source_files.get(ft)
            if not path:
                continue
            try:
                with open(path, "r", encoding="utf-8", newline="") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("symbol,"):
                            continue
                        ts = _extract_timestamp(line, ft)
                        if ts > 0:
                            min_ts = min(min_ts, ts)
                            break
            except OSError:
                pass
        return int(min_ts) if min_ts != float("inf") else 0

    def _preload_code(self, source_path: str) -> None:
        target = os.path.join(self._output_dir, f"code.csv.{self._date}")
        count = 0
        with open(source_path, "r", encoding="utf-8", newline="") as src, \
             open(target, "w", encoding="utf-8", newline="") as dst:
            for line in src:
                if line.startswith("symbol,"):
                    continue
                dst.write(line)
                count += 1
            dst.flush()
            if self._fsync:
                os.fsync(dst.fileno())
        logger.info("Preloaded code: %d lines -> %s", count, target)

    def _stream_file(self, source_path: str, file_type: str, handle: object) -> None:
        logger.info("Streaming %s from %s", file_type, source_path)

        lock = self._write_locks[file_type]
        writer = _BatchWriter(handle, self._batch_size, self._flush_interval_ms, self._fsync, lock)
        written = 0

        if self._order_mode == "time":
            # Load all lines for sorting (only viable for smaller files)
            lines: list[tuple[int, str]] = []
            with open(source_path, "r", encoding="utf-8", newline="") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw or raw.startswith("symbol,"):
                        continue
                    ts = _extract_timestamp(raw, file_type)
                    lines.append((ts, raw))
            lines.sort(key=lambda x: x[0])

            for ts, line in lines:
                if self._stop_event.is_set():
                    break
                self._wait_for_ts(ts, file_type)
                written += self._write_line(writer, handle, line, file_type, written, len(lines))
        else:
            # Stream line by line — no memory accumulation
            total = 0
            with open(source_path, "r", encoding="utf-8", newline="") as f:
                for raw in f:
                    if self._stop_event.is_set():
                        break
                    raw = raw.strip()
                    if not raw or raw.startswith("symbol,"):
                        continue
                    total += 1
                    ts = _extract_timestamp(raw, file_type)
                    self._wait_for_ts(ts, file_type)
                    written += self._write_line(writer, handle, raw, file_type, written, total)

        writer.flush()
        logger.info("%s: done, %d lines written", file_type, written)

    def _write_line(
        self,
        writer: _BatchWriter,
        handle: object,
        line: str,
        file_type: str,
        written: int,
        total: int,
    ) -> int:
        """Write a single line, handling late/split injection. Returns 1 if written."""
        # Late record injection
        if self._late_prob > 0 and random.random() < self._late_prob:
            record = _LateRecord(
                line=line + "\n",
                enqueue_time=time.monotonic() + self._late_delay_sec,
                file_type=file_type,
            )
            self._late_writer.enqueue(record)
            if written % 10000 == 0:
                logger.info("%s: %d lines processed (enqueued late)", file_type, written)
            return 1

        # Split line injection
        if self._split_line_prob > 0 and random.random() < self._split_line_prob:
            lock = self._write_locks[file_type]
            writer.flush()
            split_point = random.randint(1, max(1, len(line) - 1))
            with lock:
                handle.write(line[:split_point])
                handle.flush()
                if self._fsync:
                    os.fsync(handle.fileno())
                time.sleep(self._split_line_delay_ms / 1000.0)
                handle.write(line[split_point:] + "\n")
                handle.flush()
                if self._fsync:
                    os.fsync(handle.fileno())
        else:
            writer.write(line + "\n")

        if written % 10000 == 0:
            logger.info("%s: %d/%d lines processed", file_type, written, total)
        return 1

    def _wait_for_ts(self, record_ts: int, file_type: str | None = None) -> None:
        if record_ts <= 0 or self._global_min_time == 0:
            return

        speed = self._speed_map.get(file_type, self._speed) if file_type else self._speed
        target_elapsed = (record_ts - self._global_min_time) / speed
        # Convert from sub-millisecond units (17-digit: YYYYMMDDHHMMSSMMM) to seconds
        target_elapsed_sec = target_elapsed / 1_000_000_000
        actual_elapsed = time.monotonic() - self._real_start
        sleep_time = target_elapsed_sec - actual_elapsed

        if sleep_time > 0:
            time.sleep(min(sleep_time, MAX_SLEEP_SEC))
