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
