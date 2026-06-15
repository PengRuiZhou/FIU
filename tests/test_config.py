"""Tests for config.py — new data-driven watermark fields."""
import pytest
import tempfile
import os

from minute_bar.config import AppConfig, RecoveryConfig, load_config


class TestRecoveryConfigDefaults:
    def test_default_data_flush_delay_minutes(self):
        cfg = RecoveryConfig()
        assert cfg.data_flush_delay_minutes == 1

    def test_default_enable_time_fallback(self):
        cfg = RecoveryConfig()
        assert cfg.enable_time_fallback is True

    def test_default_stall_flush_sec(self):
        cfg = RecoveryConfig()
        assert cfg.stall_flush_sec == 300


class TestLoadConfig:
    def _write_ini(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".ini")
        with os.fdopen(fd, "w") as f:
            f.write(content)
        return path

    def test_loads_new_fields(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
            "[recovery]\ndata_flush_delay_minutes = 2\nenable_time_fallback = false\nstall_flush_sec = 60\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 2
            assert cfg.recovery.enable_time_fallback is False
            assert cfg.recovery.stall_flush_sec == 60
        finally:
            os.unlink(path)

    def test_missing_fields_use_defaults(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
            "[recovery]\noutput_delay_sec = 5\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 1
            assert cfg.recovery.enable_time_fallback is True
            assert cfg.recovery.stall_flush_sec == 300
        finally:
            os.unlink(path)

    def test_no_recovery_section_uses_defaults(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.recovery.data_flush_delay_minutes == 1
            assert cfg.recovery.enable_time_fallback is True
        finally:
            os.unlink(path)

    def test_order_chunk_size_bytes_parsed(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\norder_chunk_size_bytes = 524288\n"
            "[output]\noutput_dir = /tmp/output\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.input.order_chunk_size_bytes == 524288
        finally:
            os.unlink(path)

    def test_order_chunk_size_bytes_default(self):
        path = self._write_ini(
            "[input]\ncsv_dir = /tmp/input\n"
            "[output]\noutput_dir = /tmp/output\n"
        )
        try:
            cfg = load_config(path)
            assert cfg.input.order_chunk_size_bytes == 65536
        finally:
            os.unlink(path)
