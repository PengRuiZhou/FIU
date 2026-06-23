from __future__ import annotations

import json
import logging


class JsonFormatter(logging.Formatter):
    """Emit each log record as one JSON line for machine-parseable logs (monitoring/jq).

    Fields: ts (epoch), level, logger, msg (formatted), plus any keys passed via extra=...
    When ``LoggingConfig.structured = true``, main.py attaches this to the file handler.
    The console handler stays human-readable.
    """

    _STD_ATTRS = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "getMessage",
        # attrs a sibling human Formatter may stamp onto the shared record:
        "message", "asctime",
    })

    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": record.created,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, val in record.__dict__.items():
            if key in self._STD_ATTRS or key.startswith("_"):
                continue
            try:
                json.dumps(val)
                obj[key] = val
            except (TypeError, ValueError):
                obj[key] = repr(val)
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        return json.dumps(obj, ensure_ascii=False)
