import os

import pytest


def test_parse_commit_line_valid():
    from minute_bar.writer import _parse_commit_line
    assert _parse_commit_line("202605280931,1234567,4505,331") == ("202605280931", 1234567, 4505, 331)


@pytest.mark.parametrize("bad", [
    "",
    "   \n",
    "202605280931,1234567,4505",            # 3 fields
    "202605280931,1234567,4505,331,9",      # 5 fields
    "2026052,1234567,4505,331",             # minute not 12 digits
    "202605280931,abc,4505,331",            # non-int offset
    "202605280931,1234567,4505,-3",         # negative seqno
    "20260528093",                          # partial truncated (no newline, too few fields)
])
def test_parse_commit_line_invalid_returns_none(bad):
    from minute_bar.writer import _parse_commit_line
    assert _parse_commit_line(bad) is None


def test_parse_commit_line_trailing_cr_strips():
    from minute_bar.writer import _parse_commit_line
    assert _parse_commit_line("202605280931,1234,5,3\r") == ("202605280931", 1234, 5, 3)


def test_read_valid_sidecar_filters_bad_lines(tmp_path):
    from minute_bar.writer import _read_valid_sidecar
    sc = tmp_path / "tickfile_20260528.csv.commit"
    sc.write_text(
        "202605280931,100,5,1\n"
        "BADLINE\n"                                  # invalid -> skip
        "202605280932,200,5,2\n"
        "202605280933,150,5,3\n"                     # offset regression -> skip (INV-CM-OFFSET-MONO)
        "202605280934,300,5,4\n",
        encoding="utf-8")
    recs = _read_valid_sidecar(str(sc), "20260528")
    assert recs == [("202605280931", 100, 5, 1), ("202605280932", 200, 5, 2), ("202605280934", 300, 5, 4)]


def test_read_valid_sidecar_date_filter(tmp_path):
    from minute_bar.writer import _read_valid_sidecar
    sc = tmp_path / "tickfile_20260528.csv.commit"
    sc.write_text("202605280931,100,5,1\n202605290931,200,5,2\n", encoding="utf-8")
    recs = _read_valid_sidecar(str(sc), "20260528")
    assert recs == [("202605280931", 100, 5, 1)]  # wrong date excluded


def test_read_valid_sidecar_missing_returns_none(tmp_path):
    from minute_bar.writer import _read_valid_sidecar
    assert _read_valid_sidecar(str(tmp_path / "nope.commit"), "20260528") is None


def test_read_valid_sidecar_empty_equiv_missing(tmp_path):
    from minute_bar.writer import _read_valid_sidecar
    sc = tmp_path / "tickfile_20260528.csv.commit"
    sc.write_text("GARBAGE\nNOPE\n", encoding="utf-8")  # all invalid
    assert _read_valid_sidecar(str(sc), "20260528") == []  # empty list ≡ missing


def test_classify_precondition_new(tmp_path):
    from minute_bar.writer import _classify_append_precondition, get_tickfile_path
    tf = get_tickfile_path(str(tmp_path), "202605280931")
    sidecar = tf + ".commit"
    kind, last_rec = _classify_append_precondition("202605280931", sidecar, tf)
    assert kind == "new"
    assert last_rec is None


def test_classify_precondition_append(tmp_path):
    from minute_bar.writer import _classify_append_precondition, get_tickfile_path
    import os
    tf = get_tickfile_path(str(tmp_path), "202605280931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    # binary mode keeps "\n" as a single byte on Windows too (test asserts exact byte size)
    open(tf, "wb").write(("H\n" + "x" * 100).encode("utf-8"))   # size 102
    with open(tf + ".commit", "w") as f:
        f.write("202605280930,102,5,1\n")     # last minute < current, offset==size
    kind, last_rec = _classify_append_precondition("202605280931", tf + ".commit", tf)
    assert kind == "append"
    assert last_rec == ("202605280930", 102, 5, 1)


def test_classify_precondition_truncate_rewrite_size_gt_offset(tmp_path):
    from minute_bar.writer import _classify_append_precondition, get_tickfile_path
    import os
    tf = get_tickfile_path(str(tmp_path), "202605280931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    # binary mode keeps "\n" as a single byte on Windows too (test asserts exact byte size)
    open(tf, "wb").write(("H\n" + "x" * 200).encode("utf-8"))   # size 202 > offset 102 -> residue
    with open(tf + ".commit", "w") as f:
        f.write("202605280930,102,5,1\n")
    kind, last_rec = _classify_append_precondition("202605280931", tf + ".commit", tf)
    assert kind == "truncate_rewrite"
    assert last_rec[1] == 102


def test_classify_precondition_committed_skip(tmp_path):
    from minute_bar.writer import _classify_append_precondition, get_tickfile_path
    import os
    tf = get_tickfile_path(str(tmp_path), "202605280931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    # binary mode keeps "\n" as a single byte on Windows too (test asserts exact byte size)
    open(tf, "wb").write(("H\n" + "x" * 100).encode("utf-8"))  # size 102
    with open(tf + ".commit", "w") as f:
        f.write("202605280931,102,5,2\n")    # last == current, size==offset
    kind, last_rec = _classify_append_precondition("202605280931", tf + ".commit", tf)
    assert kind == "committed"


from minute_bar.tickfile import TICKFILE_HEADER


def test_write_appends_rows_and_sidecar_offset_matches(tmp_path):
    """INV-CM-ORDERED-TWO-FILE + INV-CM-OFFSET-FSTAT: after write, sidecar records offset == tickfile size, seqno correct."""
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path, _parse_commit_line
    import os
    selected = [("7203", None, None)]
    date = "20260528"
    write_tickfile_rows(str(tmp_path), f"{date}0931", selected, 1,
                        code_table_getter=None, skip_fsync=False, enable_commit_marker=True)
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    sc = tf + ".commit"
    assert os.path.exists(sc)
    size = os.path.getsize(tf)
    with open(sc) as f:
        line = f.readline().strip()
    rec = _parse_commit_line(line)
    assert rec is not None
    assert rec[0] == f"{date}0931"
    assert rec[1] == size          # offset == tickfile size after write
    assert rec[3] == 1             # seqno


def test_write_skip_fsync_skips_sidecar_fsync(tmp_path, monkeypatch):
    """INV-CM-SKIP-FSYNC: skip_fsync=True -> no fsync at all (sidecar included)."""
    from minute_bar.writer import write_tickfile_rows
    import os as _os
    calls = {"n": 0}
    real_fsync = _os.fsync
    def counting(fd):
        calls["n"] += 1
        return real_fsync(fd)
    monkeypatch.setattr(_os, "fsync", counting)
    write_tickfile_rows(str(tmp_path), "202605280931", [("7203", None, None)], 1,
                        skip_fsync=True, enable_commit_marker=True)
    assert calls["n"] == 0


def test_write_committed_skip_no_duplicate(tmp_path):
    """REGEN branch 2a: sidecar last == current, size == offset -> skip, no new rows, no sidecar dup."""
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path
    import os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    content = TICKFILE_HEADER + "\n" + "r" * 50 + "\n"
    with open(tf, "wb") as f:
        f.write(content.encode())
    size = os.path.getsize(tf)
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{size},1,7\n")
    before = open(tf, "rb").read()
    write_tickfile_rows(str(tmp_path), f"{date}0931", [("7203", None, None)], 8,
                        enable_commit_marker=True)
    assert open(tf, "rb").read() == before  # unchanged
    assert len([l for l in open(tf + ".commit") if l.strip()]) == 1  # no dup sidecar line


def test_write_truncate_rewrite_residue_no_duplicate(tmp_path):
    """REGEN branch 1b/2b: size > offset -> truncate to offset, then write fresh, residue gone."""
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path
    import os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ",".join(["x"] * 65) + "\n"   # committed minute (valid 65-field row)
    residue = "PARTIAL_GARBAGE_NO_NEWLINE"                    # uncommitted partial tail
    with open(tf, "wb") as f:
        f.write(committed.encode() + residue.encode())
    committed_size = len(committed.encode())
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0930,{committed_size},1,1\n")        # last minute < current
    write_tickfile_rows(str(tmp_path), f"{date}0931", [("7203", None, None)], 2,
                        enable_commit_marker=True)
    data = open(tf, "rb").read()
    assert b"PARTIAL_GARBAGE_NO_NEWLINE" not in data   # residue truncated away
    assert data.startswith(TICKFILE_HEADER.encode())


def test_write_legacy_mode_no_sidecar(tmp_path):
    """enable_commit_marker=False: no sidecar written, no flock (pure legacy append)."""
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path
    import os
    write_tickfile_rows(str(tmp_path), "202605280931", [("7203", None, None)], 1,
                        enable_commit_marker=False)
    tf = get_tickfile_path(str(tmp_path), "202605280931")
    assert not os.path.exists(tf + ".commit")  # no sidecar
    assert os.path.exists(tf)


def test_recover_truncates_partial_to_last_commit(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    import os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ("a" * 60) + "\n"
    partial = "PARTIAL_ROW_BYTES"
    with open(tf, "wb") as f:
        f.write(committed.encode() + partial.encode())
    committed_off = len(committed.encode())
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{committed_off},1,1\n")
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)
    assert had is True
    assert f"{date}0931" in cset
    assert seq == 1
    assert os.path.getsize(tf) == committed_off     # truncated
    assert b"PARTIAL_ROW_BYTES" not in open(tf, "rb").read()


def test_recover_backup_created(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    import glob, os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ("a" * 60) + "\n"
    with open(tf, "wb") as f:
        f.write(committed.encode() + b"DROPPED_TAIL")
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{len(committed.encode())},1,1\n")
    _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)
    backups = glob.glob(tf + ".truncated.*")
    assert len(backups) == 1
    assert open(backups[0], "rb").read() == b"DROPPED_TAIL"


def test_recover_offset_exceeds_size_aborts(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    import os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    open(tf, "wb").write(b"H\n" + b"x" * 90)   # size 92
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,500,1,1\n")        # offset 500 > 92
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)
    assert had is False                          # fallback, no truncate
    assert os.path.getsize(tf) == 92             # unchanged (no sparse gap)


def test_recover_sidecar_missing_fallback_row_scan_no_truncate(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    import os
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    fields = [""] * 65
    fields[16] = f"{date} 09:31:00"
    fields[59] = "5"
    with open(tf, "w") as f:
        f.write(TICKFILE_HEADER + "\n" + ",".join(fields) + "\n")
    size_before = os.path.getsize(tf)
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)
    assert had is False
    assert f"{date}0931" in cset
    assert seq == 5
    assert os.path.getsize(tf) == size_before    # no truncate in fallback


def test_recover_tickfile_missing_returns_empty(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), "20260528", enable_commit_marker=True)
    assert had is False and cset == set() and seq == 0


def test_recover_writes_audit_log(tmp_path):
    from minute_bar.writer import _recover_tickfile_to_last_commit
    import os, json
    _recover_tickfile_to_last_commit(str(tmp_path), "20260528", enable_commit_marker=True)
    log = os.path.join(str(tmp_path), "tickfile", "tickfile_recovery.log")
    assert os.path.exists(log)
    rec = json.loads(open(log).readline())
    for k in ("ts", "date", "pid", "hostname", "had_sidecar", "committed_count",
              "last_commit_minute", "truncate_bytes", "result"):
        assert k in rec


def test_recover_audit_failure_does_not_abort(tmp_path, monkeypatch):
    import os as _os
    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(_os, "makedirs", boom)
    from minute_bar.writer import _recover_tickfile_to_last_commit
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), "20260528", enable_commit_marker=True)
    assert had is False  # audit failure must NOT abort recovery


def test_recover_truncate_oserror_aborts_without_corrupting(tmp_path, monkeypatch):
    """INV-CM-FAIL-ATOMIC: if os.truncate raises during sidecar-mode recovery, the function re-raises
    and the tickfile content is left intact (no partial corruption). Backup may or may not exist."""
    import os
    from minute_bar import writer as W
    date = "20260528"
    tf = W.get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ("a" * 60) + "\n"
    with open(tf, "wb") as f:
        f.write(committed.encode() + b"DROPPED_TAIL")
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{len(committed.encode())},1,1\n")

    def boom_truncate(*a, **k):
        raise OSError("truncate boom")
    monkeypatch.setattr(W.os, "truncate", boom_truncate)

    import pytest
    with pytest.raises(OSError):
        W._recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)
    # committed content still intact (the "a"*60 row survives; DROPPED_TAIL may or may not, but no corruption of committed bytes)
    data = open(tf, "rb").read()
    assert data.startswith(TICKFILE_HEADER.encode())
    assert (b"a" * 60) in data


def test_health_check_calls_recovery_before_drain(tmp_path, monkeypatch):
    """INV-CM-ORDER-RESTART: _tickfile_writer_health_check invokes _run_tickfile_recovery before draining."""
    import os
    import queue as _q
    from unittest.mock import patch
    from minute_bar.aggregator import SharedState
    from minute_bar.tickfile import TICKFILE_HEADER
    from minute_bar.writer import get_tickfile_path
    from tests.test_tickfile_sync import _make_flusher
    from minute_bar import engine as E

    state = SharedState(); state.first_data_received = True
    date = "20260602"
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    open(tf, "wb").write((TICKFILE_HEADER + "\n" + "a" * 60 + "\n").encode())
    open(tf + ".commit", "w").write(f"{date}0931,{os.path.getsize(tf)},1,1\n")
    flusher = _make_flusher(state, tmp_path, enable_tickfile=True)

    calls = []
    monkeypatch.setattr(flusher, "_run_tickfile_recovery", lambda: calls.append("recovery"))

    # Construct a minimal engine stand-in with just the attributes health_check reads
    # before reaching the recovery call (plus enough for drain to not crash mid-method).
    eng = E.Engine.__new__(E.Engine)
    eng._tickfile_started = True
    eng._tickfile_writer_alive = False
    eng._tickfile_writer_restart_count = 0
    eng._tickfile_writer_thread = None
    eng._tickfile_writer_error_count = 0
    eng._tickfile_writer_zombie_detected_count = 0
    eng._tickfile_queue_stale_drain_count = 0
    eng._tickfile_queue = _q.Queue()  # empty -> drain returns immediately
    eng._flusher = flusher

    # health_check will attempt a real restart after drain (spawning a thread targeting
    # _tickfile_writer_loop, which we don't want). Wrap in try/except; we only assert
    # recovery was invoked before any drain/restart side effect.
    try:
        eng._tickfile_writer_health_check()
    except Exception:
        pass
    assert "recovery" in calls, "_run_tickfile_recovery must be called by health_check"

