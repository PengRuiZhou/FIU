import json, logging
from minute_bar.log_json import JsonFormatter


def test_json_formatter_emits_valid_json_with_fields():
    rec = logging.LogRecord(name="t", level=logging.INFO, pathname="x", lineno=1,
                            msg="Tickfile recovery: done committed=%d", args=(5,),
                            exc_info=None)
    out = JsonFormatter().format(rec)
    obj = json.loads(out)
    assert obj["level"] == "INFO"
    assert obj["logger"] == "t"
    assert obj["msg"] == "Tickfile recovery: done committed=5"
    assert "ts" in obj and isinstance(obj["ts"], float)


def test_json_formatter_merges_extras():
    rec = logging.LogRecord(name="t", level=logging.WARNING, pathname="x", lineno=1,
                            msg="anomaly", args=None, exc_info=None)
    rec.minute = "202605280931"; rec.bytes = 99
    obj = json.loads(JsonFormatter().format(rec))
    assert obj["minute"] == "202605280931" and obj["bytes"] == 99
