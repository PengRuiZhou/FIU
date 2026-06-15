"""Tests for CSV parser."""
import pytest
from minute_bar.csv_parser import parse_snapshot_line, parse_code_line, ParsedSnapshot, ParsedCode


class TestParseSnapshot:
    def test_minimal_17_cols(self):
        line = b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,,T,0,Y"
        result = parse_snapshot_line(line)
        assert result is not None
        assert result.symbol == "1301"
        assert result.time == 20260520093000999
        assert result.preclose == 443500
        assert result.status == "T"
        assert result.decimal == 2  # default

    def test_full_21_cols(self):
        line = b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,X,T,0,Y,2,4450000,0,20260520083000999"
        result = parse_snapshot_line(line)
        assert result is not None
        assert result.tradetype == "X"
        assert result.decimal == 2
        assert result.vwap == 4450000
        assert result.shortsellflag == 0
        assert result.rcvtime == 20260520083000999

    def test_too_few_cols(self):
        line = b"1301,20260520093000999,443500"
        result = parse_snapshot_line(line)
        assert result is None

    def test_too_many_cols(self):
        fields = ["x"] * 22
        line = ",".join(fields).encode()
        result = parse_snapshot_line(line)
        assert result is None

    def test_empty_symbol(self):
        line = b",20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,,T,0,Y"
        result = parse_snapshot_line(line)
        assert result is None

    def test_invalid_encoding(self):
        line = b"\xff\xfe"
        result = parse_snapshot_line(line)
        assert result is None

    def test_optional_cols_defaults(self):
        line = b"1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,,T,0,Y"
        result = parse_snapshot_line(line)
        assert result.decimal == 2
        assert result.vwap == 0
        assert result.shortsellflag == 0


class TestParseCode:
    def test_minimal_7_cols(self):
        line = b"1301,1,exchange,TestStock,equity,common,marine"
        result = parse_code_line(line)
        assert result is not None
        assert result.symbol == "1301"
        assert result.market == 1
        assert result.name == "TestStock"

    def test_full_cols(self):
        line = b"1301,1,TSE,\xe6\xa5\xb5\xe6\xb4\x8b,JPY,equity,common,marine,5050,JP123456,100,500000,400000,2,20260520083000999,20260520,450000"
        result = parse_code_line(line)
        assert result is not None
        assert result.lotsize == 100
        assert result.decimal == 2

    def test_too_few_cols(self):
        line = b"1301,1"
        result = parse_code_line(line)
        assert result is None
