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
