"""Tests for FileTailer and BinaryLineAssembler."""
import os
import tempfile
import pytest
from minute_bar.file_tailer import FileTailer
from minute_bar.models import FileState


class TestFileTailer:
    def test_read_full_lines(self, tmp_path):
        csv_file = tmp_path / "snapshot.csv.20260520"
        csv_file.write_text(b"line1\nline2\nline3\n".decode(), encoding="utf-8")

        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")

        lines = list(tailer.read_lines())
        assert len(lines) == 3
        assert lines[0] == b"line1"
        assert lines[2] == b"line3"
        tailer.close()

    def test_incomplete_line_appended(self, tmp_path):
        csv_file = tmp_path / "snapshot.csv.20260520"

        # Write first part: incomplete line
        with open(csv_file, "wb") as f:
            f.write(b"line1\nincom")

        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")

        lines1 = list(tailer.read_lines())
        assert lines1 == [b"line1"]

        # Append rest
        with open(csv_file, "ab") as f:
            f.write(b"plete\nline3\n")

        lines2 = list(tailer.read_lines())
        assert len(lines2) == 2
        assert lines2[0] == b"incomplete"
        assert lines2[1] == b"line3"
        tailer.close()

    def test_offset_tracking(self, tmp_path):
        csv_file = tmp_path / "snapshot.csv.20260520"
        content = b"line1\nline2\n"
        csv_file.write_bytes(content)

        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")
        list(tailer.read_lines())

        assert tailer.state.offset == len(content)
        tailer.close()

    def test_file_not_found(self, tmp_path):
        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")

        lines = list(tailer.read_lines())
        assert lines == []
        tailer.close()

    def test_set_date_resets_state(self, tmp_path):
        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")
        tailer.state.offset = 1000

        tailer.set_date("20260521")
        assert tailer.state.offset == 0
        assert tailer.state.pending_line == b""
        tailer.close()

    def test_empty_file(self, tmp_path):
        csv_file = tmp_path / "snapshot.csv.20260520"
        csv_file.write_bytes(b"")

        tailer = FileTailer(str(tmp_path), "snapshot")
        tailer.set_date("20260520")

        lines = list(tailer.read_lines())
        assert lines == []
        tailer.close()

    def test_chinese_characters(self, tmp_path):
        csv_file = tmp_path / "snapshot.csv.20260520"
        csv_file.write_bytes("1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,78300,350251000,1,,T,0,Y\n".encode("utf-8"))

        tailer = FileTailer(str(tmp_path), "snapshot", encoding="utf-8")
        tailer.set_date("20260520")

        lines = list(tailer.read_lines())
        assert len(lines) == 1
        assert b"1301" in lines[0]
        tailer.close()
