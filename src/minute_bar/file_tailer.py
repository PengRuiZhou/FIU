from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterator

from minute_bar.models import FileState

logger = logging.getLogger(__name__)


class FileTailer:
    def __init__(self, base_dir: str, file_type: str, chunk_size: int = 65536, encoding: str = "utf-8"):
        self._base_dir = base_dir
        self._file_type = file_type
        self._chunk_size = chunk_size
        self._encoding = encoding
        self._state = FileState()
        self._file_handle = None
        self._line_offset = 0

    @property
    def state(self) -> FileState:
        return self._state

    @state.setter
    def state(self, value: FileState) -> None:
        self._state = value

    @property
    def line_offset(self) -> int:
        """Byte offset after the last yielded line (for per-minute offset tracking)."""
        return self._line_offset

    def _file_path(self, date: str) -> str:
        return os.path.join(self._base_dir, f"{self._file_type}.csv.{date}")

    def _ensure_date(self) -> bool:
        if not self._state.date:
            return False
        path = self._file_path(self._state.date)
        if not os.path.exists(path):
            return False
        return True

    def set_date(self, date: str) -> None:
        if self._state.date != date:
            self._close()
            self._state = FileState(offset=0, pending_line=b"", date=date)

    def _open(self) -> None:
        if self._file_handle is not None:
            return
        if not self._state.date:
            return
        path = self._file_path(self._state.date)
        if not os.path.exists(path):
            return
        self._file_handle = open(path, "rb")
        self._file_handle.seek(self._state.offset)

    def _close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None

    def read_lines(self) -> Iterator[bytes]:
        self._open()
        if self._file_handle is None:
            return

        try:
            file_size = os.fstat(self._file_handle.fileno()).st_size
        except OSError:
            return

        if file_size < self._state.offset:
            logger.error(
                "File size %d < checkpoint offset %d for %s.csv.%s — possible file truncation",
                file_size, self._state.offset, self._file_type, self._state.date,
            )
            self._state.offset = 0
            self._file_handle.seek(0)

        data = self._state.pending_line
        chunk = self._file_handle.read(self._chunk_size)
        if not chunk:
            return

        data += chunk
        last_newline = data.rfind(b"\n")
        if last_newline == -1:
            self._state.pending_line = data
            return

        complete_data = data[: last_newline + 1]
        self._state.pending_line = data[last_newline + 1 :]
        self._state.offset += last_newline + 1

        chunk_start_offset = self._state.offset - len(complete_data)
        pos = 0
        for line in complete_data.split(b"\n"):
            pos += len(line) + 1
            stripped = line.strip(b"\r")
            if stripped:
                self._line_offset = chunk_start_offset + pos
                yield stripped

    def close(self) -> None:
        self._close()
