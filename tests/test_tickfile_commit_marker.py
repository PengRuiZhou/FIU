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
