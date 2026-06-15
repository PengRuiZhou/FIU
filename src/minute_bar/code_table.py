from __future__ import annotations

import logging
import os
from typing import Dict, Optional

from minute_bar.csv_parser import ParsedCode, parse_code_line
from minute_bar.file_tailer import FileTailer
from minute_bar.models import FileState
from minute_bar.validator import validate_code

logger = logging.getLogger(__name__)


class CodeTable:
    def __init__(self, csv_dir: str, encoding: str = "utf-8", chunk_size: int = 65536):
        self._csv_dir = csv_dir
        self._encoding = encoding
        self._table: Dict[str, ParsedCode] = {}
        self._tailer = FileTailer(csv_dir, "code", chunk_size=chunk_size, encoding=encoding)
        self._last_refresh_size: int = 0

    @property
    def table(self) -> Dict[str, ParsedCode]:
        return self._table

    def load(self, date: str) -> None:
        self._tailer.set_date(date)
        self._tailer.state = FileState(offset=0, pending_line=b"", date=date)

        self._table.clear()
        count = 0
        while True:
            lines = list(self._tailer.read_lines())
            if not lines:
                break
            for line in lines:
                parsed = parse_code_line(line, self._encoding)
                if parsed and validate_code(parsed):
                    self._merge_symbol(parsed)
                    count += 1
        logger.info("Loaded %d symbols from code.csv.%s", count, date)

    def refresh(self) -> int:
        count = 0
        for line in self._tailer.read_lines():
            parsed = parse_code_line(line, self._encoding)
            if parsed and validate_code(parsed):
                self._merge_symbol(parsed)
                count += 1
        if count > 0:
            logger.info("Code table refreshed: +%d symbols, total=%d", count, len(self._table))
        return count

    def get_name(self, symbol: str) -> str:
        entry = self._table.get(symbol)
        return entry.name if entry else ""

    def _merge_symbol(self, parsed: ParsedCode) -> None:
        existing = self._table.get(parsed.symbol)
        if existing is None:
            self._table[parsed.symbol] = parsed
            return
        # Preserve non-zero limitup/limitdown/decimal from existing entry
        # when new row has zeros (placeholder rows in code.csv)
        if parsed.limitup == 0 and existing.limitup != 0:
            parsed.limitup = existing.limitup
        if parsed.limitdown == 0 and existing.limitdown != 0:
            parsed.limitdown = existing.limitdown
        if parsed.decimal == 0 and existing.decimal != 0:
            parsed.decimal = existing.decimal
        self._table[parsed.symbol] = parsed

    def set_state(self, offset: int, pending_line: bytes, date: str) -> None:
        self._tailer.state = FileState(offset=offset, pending_line=pending_line, date=date)

    def get_state(self):
        return self._tailer.state

    def close(self) -> None:
        self._tailer.close()
