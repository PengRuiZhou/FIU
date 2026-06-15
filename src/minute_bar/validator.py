from __future__ import annotations

import logging
import re
from typing import Optional

from minute_bar.csv_parser import ParsedCode, ParsedSnapshot

logger = logging.getLogger(__name__)

_TIME_17DIGIT_RE = re.compile(r"^\d{17}$")


def validate_snapshot(parsed: ParsedSnapshot) -> bool:
    valid = True

    if not _TIME_17DIGIT_RE.match(str(parsed.time)):
        logger.error("Invalid time field (not 17 digits): %d for symbol %s", parsed.time, parsed.symbol)
        return False

    if parsed.lastprice < 0:
        logger.warning("Negative lastprice %d for symbol %s, treating as 0", parsed.lastprice, parsed.symbol)

    if parsed.totalvol < 0:
        logger.warning("Negative totalvol %d for symbol %s", parsed.totalvol, parsed.symbol)

    if not (0 <= parsed.decimal <= 6):
        logger.warning("Decimal %d out of range [0,6] for symbol %s, treating as 0", parsed.decimal, parsed.symbol)
        parsed.decimal = 0

    if parsed.status not in ("T", "P"):
        logger.warning("Unexpected status '%s' for symbol %s", parsed.status, parsed.symbol)

    return valid


def validate_code(parsed: ParsedCode) -> bool:
    if not parsed.symbol:
        logger.error("Code entry missing symbol")
        return False
    return True
