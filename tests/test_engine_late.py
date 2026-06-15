"""Tests for engine late order handling and flushed_minutes recovery."""
import csv
import os
import pytest

from minute_bar.engine import recover_flushed_minutes
from minute_bar.models import OrderRecord
from minute_bar.writer import get_order_file_path, write_order_file


class TestRecoverFlushedMinutes:
    def test_empty_output_dir(self, tmp_path):
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()
        assert order == set()

    def test_finds_snapshot_files(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")
        (snap_dir / "snapshot_minute_20260520_0931.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == {"202605200930", "202605200931"}
        assert order == set()

    def test_finds_order_files(self, tmp_path):
        order_dir = tmp_path / "order" / "2026" / "20260520"
        order_dir.mkdir(parents=True)
        (order_dir / "order_minute_20260520_0930.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()
        assert order == {"202605200930"}

    def test_finds_both_types(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")

        order_dir = tmp_path / "order" / "2026" / "20260520"
        order_dir.mkdir(parents=True)
        (order_dir / "order_minute_20260520_0930.csv").write_text("header\n")

        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == {"202605200930"}
        assert order == {"202605200930"}

    def test_ignores_tmp_files(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv.tmp").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260520")
        assert snap == set()

    def test_ignores_other_dates(self, tmp_path):
        snap_dir = tmp_path / "snapshot" / "2026" / "20260520"
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot_minute_20260520_0930.csv").write_text("header\n")
        snap, order = recover_flushed_minutes(str(tmp_path), "20260521")
        assert snap == set()
