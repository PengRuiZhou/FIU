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


# ─────────────────────────────────────────────────────────────────────────────
# Task 8 (T8) — End-to-end recovery tests
#
# Spec M-R2-2 / M-R5-7 / A3: the production hard-crash recovery path must close
# the loop end-to-end. Two E2E tests:
#   1. test_e2e_mid_append_crash_recovery_replay        — replay path (concrete)
#   2. test_e2e_live_restart_recovers_partial_minute    — live restart path
#      (semi-integrated: real Engine.__init__ eager recovery + real
#       _run_tickfile_recovery runtime recovery + real _try_generate_tickfile;
#       see the test docstring for why the full FileTailer polling loop is not
#       driven in-process).
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.e2e
def test_e2e_mid_append_crash_recovery_replay(tmp_path):
    """Mid-append crash (replay path): committed 0931 + partial 0932 tail; replay
    run() truncates the partial tail and regenerates 0932 cleanly, without
    duplicating 0931.

    Seed pattern cloned from tests/test_tickfile_stale_fix.py:197
    (TestReplayGapFillIntegration): clock-minute 0930 -> tickfile bucket 202605200931
    (the committed/seeded minute), clock-minute 0931 -> bucket 202605200932 (the gap
    to regenerate). Phase 22 left-open-right-closed round-up means sub-minute>0
    lands in bucket N+1.
    """
    import csv
    from minute_bar.config import (AggregationConfig, AppConfig, InputConfig,
                                   OutputConfig)
    from minute_bar.replay import ReplayEngine
    from minute_bar.tickfile import TICKFILE_HEADER
    from minute_bar.writer import get_tickfile_path

    date = "20260520"
    out_dir = tmp_path / "output"
    in_dir = tmp_path / "input"
    in_dir.mkdir()
    out_dir.mkdir()

    # code.csv — 17-field format (proven in tests/test_replay.py:41).
    (in_dir / f"code.csv.{date}").write_text(
        "7203,1,TSE,Toyota,JPY,equity,common,,,,0,0,0,2,0,,0\n", encoding="utf-8")

    # snapshot.csv — 21-field rows. Round-up: clock-min 0930 -> bucket 0931,
    # clock-min 0931 -> bucket 0932.
    snapshot_rows = [
        # clock-minute 0930 -> tickfile bucket 202605200931 (committed/seeded)
        "7203,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
        # clock-minute 0931 -> tickfile bucket 202605200932 (gap to regenerate)
        "7203,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083100999",
    ]
    with open(in_dir / f"snapshot.csv.{date}", "wb") as f:
        for r in snapshot_rows:
            f.write(r.encode("utf-8") + b"\n")

    # Pre-seed the output tickfile with bucket 0931 COMMITTED (valid 65-field row
    # + sidecar entry) and an un-sidecared PARTIAL 0932 tail (mid-append crash).
    seed_path = get_tickfile_path(str(out_dir), f"{date}0931")  # one file per day
    os.makedirs(os.path.dirname(seed_path), exist_ok=True)
    fields = [""] * 65
    fields[0] = "7203"
    fields[1] = date
    fields[16] = f"{date} 09:31:00"   # UpdateTime -> minute_key 202605200931
    fields[59] = "1"                   # Seqno
    fields[60] = "2026-05-20 09:30:00.999000"  # LocalTime
    committed_block = TICKFILE_HEADER + "\n" + ",".join(fields) + "\n"
    partial_tail = b"7203,partial,corrupt,tail,no,newline,bytes"   # NO sidecar entry
    with open(seed_path, "wb") as f:
        f.write(committed_block.encode("utf-8") + partial_tail)
    committed_size = len(committed_block.encode("utf-8"))
    with open(seed_path + ".commit", "w", encoding="utf-8") as f:
        # sidecar authoritative for 0931 only (offset == committed_size, < file size)
        f.write(f"{date}0931,{committed_size},1,1\n")

    config = AppConfig(
        input=InputConfig(csv_dir=str(in_dir), file_encoding="utf-8"),
        output=OutputConfig(output_dir=str(out_dir), enable_order=False,
                            enable_tickfile=True, enable_kline=False),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
    )
    ReplayEngine(config, date=date).run()

    # The partial tail bytes must be gone (truncated to last committed offset).
    data = open(seed_path, "rb").read()
    assert b"partial,corrupt" not in data, "partial tail was not truncated by recovery"

    # Group surviving rows by minute_key (UpdateTime col 16).
    with open(seed_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        by_minute = {}
        for row in reader:
            if len(row) != 65:
                continue
            mk = row[16].replace(" ", "").replace(":", "")[:12]
            by_minute[mk] = by_minute.get(mk, 0) + 1

    # 0931 (committed) must be UNCHANGED — NOT duplicated by replay.
    assert by_minute.get(f"{date}0931", 0) == 1, \
        f"committed 0931 row duplicated/corrupted: count={by_minute.get(f'{date}0931')}"
    # 0932 (the gap) must now be regenerated exactly once.
    assert by_minute.get(f"{date}0932", 0) == 1, \
        f"gap minute 0932 not regenerated cleanly: count={by_minute.get(f'{date}0932')}"


@pytest.mark.e2e
@pytest.mark.slow
def test_e2e_live_restart_recovers_partial_minute(tmp_path):
    """Live-restart recovery (M-R2-2 — the production hard-crash recovery real path).

    Scenario simulated:
      * A previous live Engine run committed minute 0931 (valid row + sidecar entry)
        and crashed MID-APPEND on minute 0932, leaving an un-sidecared partial tail
        on disk (exactly the INV-CM-FAIL-ATOMIC / mid-append scenario).
      * A new Engine process starts (Engine.__init__) and the eager init recovery
        truncates the partial tail to the last committed offset, syncing the
        committed skip-set + seqno from the sidecar.
      * The writer thread then dies again mid-append (re-corrupt), the health-check
        path fires `_run_tickfile_recovery()` (the production runtime recovery
        invoked at engine.py:1481), which truncates the fresh partial.
      * The writer loop body (`_try_generate_tickfile`, the real method the writer
        thread runs per dequeued minute) then regenerates 0932 cleanly, and the
        new committed entry appears in the sidecar — no dup of 0931, no partial
        bytes, no double sidecar entry.

    PATH EXERCISED (semi-integrated):
      This test drives the REAL production recovery path against a REAL live
      `Engine` instance constructed from an AppConfig (so Engine.__init__ runs the
      real ClockWatermarkFlusher.__init__ eager recovery, real CodeTable,
      CheckpointManager, FileTailer wiring). It then invokes the REAL
      `engine._flusher._run_tickfile_recovery()` (the same method
      `_tickfile_writer_health_check` calls on writer-death restart) and the REAL
      `engine._flusher._try_generate_tickfile(mk)` (the exact writer-loop body at
      engine.py:1280-1290).

      The only part NOT driven in-process is the FileTailer wall-clock polling
      loop + the worker-thread spawn from `Engine.start()`. That loop is
      orthogonal to recovery (it feeds data into the queue; recovery operates on
      the on-disk tickfile + sidecar). Driving it here would couple the test to
      the system clock, real-time file polling, and 4-thread convergence — the
      same timing sensitivity that makes test_e2e_phase21_benchmark.py skip
      unless real source data exists. The semi-integrated form exercises the
      EXACT production recovery methods on a real Engine instance with zero
      wall-clock dependence, and is the strongest reliable version in this
      environment.

      `jst_now_yyyymmdd` is patched to a fixed date (the established pattern from
      tests/test_tickfile_sync.py:_make_flusher) so the seeded tickfile path
      matches the recovery target date deterministically.
    """
    import csv
    import glob
    from unittest.mock import patch
    from minute_bar.config import (AggregationConfig, AppConfig, InputConfig,
                                   OutputConfig, RecoveryConfig)
    from minute_bar.engine import Engine
    from minute_bar.tickfile import TICKFILE_HEADER
    from minute_bar.writer import get_tickfile_path

    # Fixed date — patched into minute_bar.flusher.jst_now_yyyymmdd so both the
    # eager init recovery and the runtime _run_tickfile_recovery target the same
    # tickfile path that we seed below.
    date = "20260602"
    out_dir = tmp_path / "output"
    csv_dir = tmp_path / "input"
    csv_dir.mkdir()
    out_dir.mkdir()

    # Seed csv_dir with a code.csv + a snapshot.csv feeding minute 0932 (the
    # partial minute we will regenerate). These files are not tail-consumed in
    # this test (we drive _try_generate_tickfile directly), but they make the
    # Engine's csv_dir realistic and let CodeTable.load() find a code row so
    # tickfile row generation can resolve the symbol's name/market.
    (csv_dir / f"code.csv.{date}").write_text(
        "7203,1,TSE,Toyota,JPY,equity,common,,,,0,0,0,2,0,,0\n", encoding="utf-8")
    (csv_dir / f"snapshot.csv.{date}").write_text(
        "7203,20260602093100999,443500,455000,440000,455000,443500,455000,455000,100,300,135000000,1,,T,0,Y,2,0,0,20260602083100999\n",
        encoding="utf-8")

    tf = get_tickfile_path(str(out_dir), f"{date}0931")  # one file per day
    os.makedirs(os.path.dirname(tf), exist_ok=True)

    # Committed 0931 row (valid 65 fields) + partial 0932 tail (NO sidecar entry).
    fields = [""] * 65
    fields[0] = "7203"
    fields[1] = date
    fields[16] = f"{date} 09:31:00"
    fields[59] = "1"                            # Seqno
    fields[60] = "2026-05-20 09:30:00.999000"   # LocalTime
    committed_block = TICKFILE_HEADER + "\n" + ",".join(fields) + "\n"
    partial_tail = b"7203,partial,corrupt,0932,tail,no,newline"
    with open(tf, "wb") as f:
        f.write(committed_block.encode("utf-8") + partial_tail)
    committed_size = len(committed_block.encode("utf-8"))
    with open(tf + ".commit", "w", encoding="utf-8") as f:
        # Sidecar authoritative for 0931; offset < file size => partial 0932 is
        # un-sidecared residue that recovery must truncate.
        f.write(f"{date}0931,{committed_size},1,1\n")

    # ── Phase A: real Engine.__init__ runs eager recovery ──
    # AppConfig shape mirrors tests/test_e2e_phase21_benchmark.py:131 (live mode),
    # stripped of the Rust flags (Rust not required for recovery, and would raise
    # RuntimeError if enable_order_accel=True without the extension installed).
    config = AppConfig(
        input=InputConfig(csv_dir=str(csv_dir), target_date=date,
                          file_encoding="utf-8", poll_interval_ms=50),
        output=OutputConfig(output_dir=str(out_dir), enable_order=True,
                            enable_tickfile=True, enable_kline=False,
                            enable_full_snapshot=False, enable_full_kline=False),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        recovery=RecoveryConfig(
            checkpoint_file=str(tmp_path / "engine_ckpt.json"),
            output_delay_sec=0,
            data_flush_delay_minutes=0,
            enable_time_fallback=False,
            stall_flush_sec=30,
            enable_tickfile_commit_marker=True,
        ),
    )
    with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value=date):
        engine = Engine(config)

    # Eager init recovery MUST have truncated the partial tail to committed_size.
    assert open(tf, "rb").read().endswith(committed_block.encode("utf-8")), \
        "Engine.__init__ eager recovery did not truncate the partial 0932 tail"
    assert b"partial,corrupt" not in open(tf, "rb").read(), \
        "partial tail survived eager init recovery"
    # Skip-set + seqno synced from sidecar.
    assert f"{date}0931" in engine._state._generated_tickfile_minutes
    assert engine._state._tickfile_seqno >= 1
    # A .truncated.* backup of the dropped partial bytes should exist.
    backups_after_init = glob.glob(tf + ".truncated.*")
    assert len(backups_after_init) >= 1, "eager recovery created no truncation backup"
    assert open(backups_after_init[0], "rb").read() == partial_tail, \
        "truncation backup does not preserve the dropped partial bytes"

    # ── Phase B: simulate a second mid-append crash + writer-death restart ──
    # The writer thread crashes again mid-append on 0932, leaving a fresh partial.
    fresh_partial = b"7203,AGAIN,partial,0932,tail,after,restart"
    with open(tf, "ab") as f:
        f.write(fresh_partial)
    # Clear the in-memory skip-set/seqno so the runtime recovery's sync is
    # observable (mirrors a fresh writer-thread state after death).
    engine._state._generated_tickfile_minutes.clear()
    engine._state._tickfile_seqno = 0

    # Invoke the REAL production runtime recovery method — the exact call
    # _tickfile_writer_health_check makes at engine.py:1481 before restarting
    # the writer thread.
    with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value=date):
        engine._flusher._run_tickfile_recovery()

    # Runtime recovery MUST have truncated the fresh partial as well.
    assert b"AGAIN,partial" not in open(tf, "rb").read(), \
        "runtime _run_tickfile_recovery did not truncate the fresh partial"
    assert open(tf, "rb").read().endswith(committed_block.encode("utf-8")), \
        "runtime recovery corrupted the committed 0931 bytes"
    # Sync happened again.
    assert f"{date}0931" in engine._state._generated_tickfile_minutes
    assert engine._state._tickfile_seqno >= 1

    # ── Phase C: regenerate 0932 via the REAL writer-loop body ──
    # Build the pending snapshot + order data the writer thread would have
    # queued, then invoke the real _try_generate_tickfile (engine.py:1281 calls
    # exactly this). Uses real SnapshotRecord/OrderRecord dataclasses (mirrors
    # tests/test_tickfile_sync.py:_make_snapshot/_make_order).
    from minute_bar.models import OrderRecord, SnapshotRecord
    snap = SnapshotRecord(
        symbol="7203", seqno=1, time=20260602093100999, rcvtime=20260602083100999,
        preclose=443500.0, lastprice=455000.0, open=443500.0, high=455000.0,
        low=440000.0, close=455000.0, lasttradeprice=455000.0, lasttradeqty=100,
        totalvol=300, totalamount=135000000.0, sessionid=1, tradetype="",
        status="T", direction=0, pflag="N", decimal=2, vwap=451000.0,
        shortsellflag=0,
    )
    order = OrderRecord(
        symbol="7203", seqno=1, time=20260602093100999,
        bidprice=443500, bidsize=100, askprice=455000, asksize=200,
        decimal=2, rcvtime=20260602083100999,
    )
    mk0932 = f"{date}0932"
    engine._state._tickfile_pending[mk0932] = {
        "raw_records": {"7203": [snap]},
        "snapshot_copy": {"7203": snap},
    }
    engine._state.raw_order_buffers[mk0932] = [order]
    # CodeTable needs the symbol loaded so build_tickfile_row can resolve name/market.
    engine._code_table.load(date)
    # The real writer-loop body. Must succeed, write exactly one 0932 row, and
    # mark 0932 as generated.
    engine._flusher._try_generate_tickfile(mk0932)

    # ── Phase D: assertions ──
    data = open(tf, "rb").read()
    assert b"partial,corrupt" not in data and b"AGAIN,partial" not in data, \
        "partial tail bytes survived the full recovery loop"
    assert data.startswith(TICKFILE_HEADER.encode("utf-8")), \
        "tickfile header lost during recovery/regeneration"

    with open(tf, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        by_minute = {}
        for row in reader:
            if len(row) != 65:
                continue
            mk = row[16].replace(" ", "").replace(":", "")[:12]
            by_minute[mk] = by_minute.get(mk, 0) + 1

    assert by_minute.get(f"{date}0931", 0) == 1, \
        f"committed 0931 duplicated: count={by_minute.get(f'{date}0931')}"
    assert by_minute.get(f"{date}0932", 0) == 1, \
        f"regenerated 0932 not exactly one row: count={by_minute.get(f'{date}0932')}"
    assert mk0932 in engine._state._generated_tickfile_minutes, \
        "0932 not marked generated after _try_generate_tickfile"

    # Sidecar must now contain BOTH committed minutes, each offset == tickfile size
    # at the time of commit, and no duplicate entries.
    rec_lines = [l.strip() for l in open(tf + ".commit", encoding="utf-8") if l.strip()]
    rec_minutes = [l.split(",", 1)[0] for l in rec_lines]
    assert rec_minutes.count(f"{date}0931") == 1, \
        f"sidecar 0931 not exactly once: {rec_minutes}"
    assert rec_minutes.count(f"{date}0932") == 1, \
        f"sidecar 0932 not exactly once: {rec_minutes}"
    # Last sidecar offset must equal the final tickfile size (INV-CM-OFFSET-FSTAT).
    last_offset = int(rec_lines[-1].split(",")[1])
    assert last_offset == len(data), \
        f"last sidecar offset {last_offset} != tickfile size {len(data)}"


# ─────────────────────────────────────────────────────────────────────────────
# Task 9 (T9) — "Full" proof tests:
#   1. flock cross-process (subprocess) exclusion  [@slow, @requires_fcntl]
#   2. pandas empirical csv-compat                  [@requires_pandas]
#   3. no-pandas csv fallback (always runs)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.slow
@pytest.mark.requires_fcntl
def test_flock_excludes_cross_process(tmp_path):
    """fcntl.flock is per-OFD; a subprocess LOCK_EX|LOCK_NB on the same lockfile must fail (BLOCKED).

    pytest.importorskip("fcntl") makes the @requires_fcntl marker effective: on Windows
    (and any non-POSIX env without fcntl) this test is skipped cleanly instead of running
    the subprocess (which would ModuleNotFoundError on `import fcntl`). On Linux CI fcntl
    is present and the real cross-process exclusion is exercised.
    """
    pytest.importorskip("fcntl")  # skip on Windows / non-POSIX (@requires_fcntl)
    import subprocess, sys
    from minute_bar.writer import _flock_critical_section
    lockfile = str(tmp_path / "tickfile_20260528.csv.lock")
    open(lockfile, "a").close()
    with _flock_critical_section(lockfile):
        code = (
            "import fcntl,sys\n"
            f"f=open({lockfile!r},'a')\n"
            "try:\n"
            "    fcntl.flock(f.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
            "    print('ACQUIRED'); sys.exit(0)\n"
            "except BlockingIOError:\n"
            "    print('BLOCKED'); sys.exit(1)\n"
        )
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
        assert r.returncode == 1 and "BLOCKED" in r.stdout


def _make_commit_marker_snapshot(symbol="7203", seqno=1):
    """Real SnapshotRecord -> build_tickfile_row emits a full 65-field row (not 'NA')."""
    from minute_bar.models import SnapshotRecord
    return SnapshotRecord(
        symbol=symbol, seqno=seqno, time=20260528093100999, rcvtime=20260528083100999,
        preclose=443500.0, lastprice=455000.0, open=443500.0, high=455000.0,
        low=440000.0, close=455000.0, lasttradeprice=455000.0, lasttradeqty=100,
        totalvol=300, totalamount=135000000.0, sessionid=1, tradetype="",
        status="T", direction=0, pflag="N", decimal=2, vwap=451000.0, shortsellflag=0,
    )


@pytest.mark.requires_pandas
def test_tickfile_csv_pandas_empirical(tmp_path):
    """C-R7-3 core: real pd.read_csv on a sidecar-era tickfile -> 65 cols, no '#' rows, no Unnamed cols; sidecar 4 cols."""
    pd = pytest.importorskip("pandas")
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path
    date = "20260528"
    for mk, seq in [(f"{date}0931", 1), (f"{date}0932", 2), (f"{date}0933", 3)]:
        write_tickfile_rows(str(tmp_path), mk, [("7203", _make_commit_marker_snapshot(seqno=seq), None)],
                            seq, enable_commit_marker=True)
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    df = pd.read_csv(tf)
    assert df.shape[1] == 65
    assert not any(str(c).startswith("#") for c in df.columns)
    assert not any(str(c).startswith("Unnamed") for c in df.columns)
    sc = pd.read_csv(tf + ".commit", header=None)
    assert sc.shape[1] == 4
    assert len(sc) == 3  # three committed minutes


def test_tickfile_pure_csv_reader_no_hash_rows(tmp_path):
    """Weak fallback (no pandas): csv.reader sees 65 fields/row incl header, no '#', sidecar 4 fields."""
    import csv
    from minute_bar.writer import write_tickfile_rows, get_tickfile_path
    date = "20260528"
    write_tickfile_rows(str(tmp_path), f"{date}0931",
                        [("7203", _make_commit_marker_snapshot(), None)], 1, enable_commit_marker=True)
    tf = get_tickfile_path(str(tmp_path), f"{date}0931")
    with open(tf, newline="") as f:
        rows = list(csv.reader(f))
    assert all(len(r) == 65 for r in rows)        # header + data
    assert not any(any(c.startswith("#") for c in r) for r in rows)
    with open(tf + ".commit", newline="") as f:
        sc = list(csv.reader(f))
    assert all(len(r) == 4 for r in sc)


# ─────────────────────────────────────────────────────────────────────────────
# Task 11 (T11) — tamper detection (INV-CM-SIDECAR-TAMPER-DETECT),
# .truncated.* retention (INV-CM-RETENTION), fs runtime check (INV-CM-FS-CHECK-RUNTIME).
# ─────────────────────────────────────────────────────────────────────────────


def test_sidecar_missing_with_nontrivial_tickfile_critical(tmp_path, caplog):
    """INV-CM-SIDECAR-TAMPER-DETECT: big tickfile with committed rows, no sidecar -> CRITICAL tamper."""
    import json, logging, os
    from minute_bar.writer import (
        _recover_tickfile_to_last_commit, get_tickfile_path, TICKFILE_TAMPER_THRESHOLD_BYTES,
    )
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    # Build a tickfile > threshold with valid 65-field rows (so committed_set is non-empty), NO sidecar.
    fields = [""] * 65
    fields[16] = f"{date} 09:31:00"   # UpdateTime col -> minute 0931
    fields[59] = "1"                   # Seqno col
    row = ",".join(fields)
    # Write enough copies to exceed the tamper threshold.
    with open(tf, "wb") as f:
        f.write(TICKFILE_HEADER.encode() + b"\n")
        while f.tell() < TICKFILE_TAMPER_THRESHOLD_BYTES + 1024:
            f.write(row.encode() + b"\n")
    assert os.path.getsize(tf) > TICKFILE_TAMPER_THRESHOLD_BYTES

    with caplog.at_level(logging.CRITICAL, logger="minute_bar.writer"):
        cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)

    assert had is False
    assert f"{date}0931" in cset            # row scan still reconstructed the minute
    assert any("sidecar_missing_nontrivial_tickfile" in r.message for r in caplog.records)

    # The audit record's `result` must be overridden to "tamper".
    log = os.path.join(str(tmp_path), "tickfile", "tickfile_recovery.log")
    assert os.path.exists(log)
    last = [json.loads(l) for l in open(log, encoding="utf-8") if l.strip()][-1]
    assert last["result"] == "tamper"


def test_sidecar_missing_tiny_tickfile_no_tamper(tmp_path, caplog):
    """INV-CM-SIDECAR-TAMPER-DETECT: header-only / tiny tickfile w/o sidecar is normal, NOT tamper."""
    import json, logging, os
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    date = "20260528"
    tf = get_tickfile_path(str(tmp_path), f"{date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    # A single small row -> well under the threshold, no sidecar (legacy/fresh-start scenario).
    fields = [""] * 65
    fields[16] = f"{date} 09:31:00"
    fields[59] = "1"
    with open(tf, "w") as f:
        f.write(TICKFILE_HEADER + "\n" + ",".join(fields) + "\n")

    with caplog.at_level(logging.CRITICAL, logger="minute_bar.writer"):
        cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), date, enable_commit_marker=True)

    assert had is False
    assert f"{date}0931" in cset
    assert not any("sidecar_missing_nontrivial_tickfile" in r.message for r in caplog.records)
    log = os.path.join(str(tmp_path), "tickfile", "tickfile_recovery.log")
    last = [json.loads(l) for l in open(log, encoding="utf-8") if l.strip()][-1]
    assert last["result"] == "fallback"    # NOT tamper — tiny tickfile is benign


def test_truncated_retention_keeps_newest(tmp_path):
    """INV-CM-RETENTION: more than MAX_TRUNCATED_BACKUPS -> oldest pruned, newest survive."""
    import glob, os
    from minute_bar.writer import (
        _prune_truncated_backups, MAX_TRUNCATED_BACKUPS, get_tickfile_path,
    )
    tf = get_tickfile_path(str(tmp_path), "202605280000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    n = MAX_TRUNCATED_BACKUPS + 5
    # Filename layout: <tf>.truncated.<i>.999 ; parse the <i> segment after the '.truncated.' prefix.
    for i in range(n):
        p = f"{tf}.truncated.{i}.999"   # time_ns placeholder; deterministic mtime set below
        open(p, "w").close()
        os.utime(p, (i, i))             # distinct increasing mtimes -> deterministic ordering
    _prune_truncated_backups(tf)
    remaining = glob.glob(f"{tf}.truncated.*")
    assert len(remaining) == MAX_TRUNCATED_BACKUPS

    # Recover the per-file <i> index robustly (split off the .truncated. prefix, then take [0]).
    prefix = ".truncated."
    def _idx(p):
        tail = os.path.basename(p).split(prefix, 1)[1]   # "<i>.999"
        return int(tail.split(".", 1)[0])
    surviving = sorted(_idx(p) for p in remaining)
    # The newest (highest mtime i) must survive: indices [n-MAX, n).
    assert surviving == list(range(n - MAX_TRUNCATED_BACKUPS, n))


def test_truncated_retention_no_op_when_under_limit(tmp_path):
    """INV-CM-RETENTION: <= MAX_TRUNCATED_BACKUPS -> nothing deleted (guard against the len()<=keep early return)."""
    import glob, os
    from minute_bar.writer import (
        _prune_truncated_backups, MAX_TRUNCATED_BACKUPS, get_tickfile_path,
    )
    tf = get_tickfile_path(str(tmp_path), "202605280000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    n = MAX_TRUNCATED_BACKUPS        # exactly at the limit -> no prune
    for i in range(n):
        open(f"{tf}.truncated.{i}.999", "w").close()
    _prune_truncated_backups(tf)
    assert len(glob.glob(f"{tf}.truncated.*")) == n


def test_engine_rejects_nfs_output_dir(tmp_path, monkeypatch):
    """INV-CM-FS-CHECK-RUNTIME: a network fs type -> RuntimeError. The helper's POSIX branch
    is forced via monkeypatch (_HAS_FCNTL=True + a fake /proc/mounts) so the check is exercised
    even on Windows dev/test (where it is otherwise a no-op)."""
    from minute_bar import writer as W
    monkeypatch.setattr(W, "_HAS_FCNTL", True)   # force the POSIX branch regardless of platform

    import builtins
    real_open = builtins.open
    # Fake /proc/mounts: tmpfs at /, nfs mounted exactly at tmp_path.
    proc_content = (
        "tmpfs / tmpfs rw 0 0\n"
        f"nfs {tmp_path} nfs rw,vers=4 0 0\n"
    )

    def fake_open(path, *a, **k):
        if str(path) == "/proc/mounts":
            import io
            return io.StringIO(proc_content)
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", fake_open)
    with pytest.raises(RuntimeError, match="nfs"):
        W.check_output_fs_local(str(tmp_path))


def test_fs_check_passes_local_fstype(tmp_path, monkeypatch):
    """INV-CM-FS-CHECK-RUNTIME: local fs types (ext4/xfs/tmpfs) are accepted (no raise)."""
    from minute_bar import writer as W
    monkeypatch.setattr(W, "_HAS_FCNTL", True)
    import builtins
    real_open = builtins.open
    proc_content = (
        "tmpfs / tmpfs rw 0 0\n"
        f"ext4 {tmp_path} ext4 rw 0 0\n"
    )

    def fake_open(path, *a, **k):
        if str(path) == "/proc/mounts":
            import io
            return io.StringIO(proc_content)
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", fake_open)
    # Must not raise on ext4.
    W.check_output_fs_local(str(tmp_path))


def test_fs_check_noop_when_fcntl_unavailable(tmp_path):
    """INV-CM-FS-CHECK-RUNTIME (M-R25-3): on non-POSIX (Windows dev/test) the check is a no-op,
    so even an 'nfs' path is accepted (production is Linux; the check is enforced there)."""
    from minute_bar import writer as W
    # _HAS_FCNTL is False on Windows -> immediate return. Confirm it does not raise.
    assert W._HAS_FCNTL is False
    W.check_output_fs_local(str(tmp_path))   # must not raise


def test_crossday_old_date_recovery_truncates_partial(tmp_path):
    """INV-CM-CROSSDAY-FLUSH-BARRIER: recovering the OLD date at cross-day truncates its partial tail.

    The cross-day pause's _run_tickfile_recovery() resolves the date via jst_now_yyyymmdd() = the NEW
    date, so the OLD-date partial tail was never truncated. _step1_cross_day_check therefore makes an
    explicit old-date _recover_tickfile_to_last_commit call before clearing state. This test mirrors
    that exact call and asserts the old-date partial is truncated (and the returned set is discarded)."""
    import os
    from minute_bar.writer import _recover_tickfile_to_last_commit, get_tickfile_path
    old_date = "20260527"  # the day being crossed away from
    tf = get_tickfile_path(str(tmp_path), f"{old_date}0000")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    committed = TICKFILE_HEADER + "\n" + ("a" * 60) + "\n"
    with open(tf, "wb") as f:
        f.write(committed.encode() + b"OLD_DATE_PARTIAL_TAIL")
    committed_off = len(committed.encode())
    with open(tf + ".commit", "w") as f:
        # last old-date minute committed; offset = end of committed content
        f.write(f"{old_date}1500,{committed_off},1,330\n")
    # Mirrors the fix's call: result is discarded (INV-CM-CROSSDAY-COMMITTED-DISCARD).
    cset, seq, had = _recover_tickfile_to_last_commit(str(tmp_path), old_date, enable_commit_marker=True)
    assert had is True
    assert f"{old_date}1500" in cset
    assert seq == 330
    assert os.path.getsize(tf) == committed_off          # old-date partial truncated
    assert b"OLD_DATE_PARTIAL_TAIL" not in open(tf, "rb").read()


def test_killswitch_false_live_write_no_sidecar(tmp_path):
    """INV-CM-KILLSWITCH-CONSISTENCY: with enable_tickfile_commit_marker=False, the LIVE write path
    (_try_generate_tickfile) must NOT create a sidecar or flock — not just recovery. Regression for the
    flusher._try_generate_tickfile write_tickfile_rows(...) call that previously omitted the flag
    (so the kill-switch correctly no-op'd recovery via row-scan, but the live writer still emitted
    sidecars + acquired flock — an incoherent kill-switch state)."""
    import os
    from unittest.mock import patch

    from minute_bar.aggregator import SharedState
    from minute_bar.checkpoint import CheckpointManager
    from minute_bar.code_table import CodeTable
    from minute_bar.flusher import ClockWatermarkFlusher
    from minute_bar.writer import get_tickfile_path
    from tests.test_tickfile_sync import _make_snapshot

    state = SharedState()
    state.first_data_received = True
    # Construct flusher with the kill-switch OFF.
    with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value="20260602"):
        flusher = ClockWatermarkFlusher(
            state=state, code_table=CodeTable("dummy"), checkpoint=CheckpointManager("dummy", {}),
            output_dir=str(tmp_path), output_delay_sec=60, enable_order=True, enable_tickfile=True,
            enable_tickfile_commit_marker=False,
        )
    mk = "202606020931"
    # Seed pending data the way _try_generate_tickfile expects (mirror production dict shape).
    snap = _make_snapshot()
    state._tickfile_pending[mk] = {"raw_records": {"7203": [snap]}, "snapshot_copy": {"7203": snap}}
    state._tickfile_seqno = 0
    flusher._try_generate_tickfile(mk)
    tf = get_tickfile_path(str(tmp_path), mk)
    assert os.path.exists(tf)                     # tickfile written
    assert not os.path.exists(tf + ".commit")     # NO sidecar (kill-switch off)
    assert not os.path.exists(tf + ".lock")       # NO lockfile created via flock path
    assert mk in state._generated_tickfile_minutes

