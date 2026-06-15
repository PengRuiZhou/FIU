"""Tests for checkpoint manager."""
import pytest
from minute_bar.checkpoint import CheckpointManager
from minute_bar.models import FileState


class TestCheckpointManager:
    def test_write_and_read(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        mgr = CheckpointManager(path, str(tmp_path))

        files = {
            "snapshot": FileState(offset=12345, pending_line=b"partial", date="20260520"),
            "code": FileState(offset=678, pending_line=b"", date="20260520"),
        }

        mgr.write(
            date="20260520",
            last_seqno=100,
            output_minutes={"202605200930", "202605200931"},
            last_output_minute="202605200931",
            current_minute="202605200932",
            last_output_date="20260520",
            first_data_received=True,
            files=files,
            last_totalvol_by_symbol={"1301": 78300, "1305": 74450},
            last_totalamount_by_symbol={"1301": 3502510.0, "1305": 3081980.4},
        )

        data = mgr.read()
        assert data is not None
        assert data["version"] == 3
        assert data["last_seqno"] == 100
        assert data["last_output_minute"] == "202605200931"
        assert len(data["output_minutes"]) == 2

    def test_restore_file_states(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        mgr = CheckpointManager(path, str(tmp_path))

        files = {
            "snapshot": FileState(offset=12345, pending_line=b"abc", date="20260520"),
        }

        mgr.write(
            date="20260520", last_seqno=1, output_minutes=set(),
            last_output_minute="", current_minute="",
            last_output_date="20260520", first_data_received=True,
            files=files,
            last_totalvol_by_symbol={}, last_totalamount_by_symbol={},
        )

        data = mgr.read()
        states = mgr.get_file_states(data)
        assert "snapshot" in states
        assert states["snapshot"].offset == 12345
        assert states["snapshot"].pending_line == b"abc"

    def test_restore_volume_baselines(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        mgr = CheckpointManager(path, str(tmp_path))

        mgr.write(
            date="20260520", last_seqno=1, output_minutes=set(),
            last_output_minute="", current_minute="",
            last_output_date="20260520", first_data_received=True,
            files={},
            last_totalvol_by_symbol={"1301": 78300},
            last_totalamount_by_symbol={"1301": 3502510.0},
        )

        data = mgr.read()
        assert mgr.get_last_totalvol(data) == {"1301": 78300}
        assert mgr.get_last_totalamount(data) == {"1301": 3502510.0}

    def test_no_checkpoint_file(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path / "nonexistent.json"), str(tmp_path))
        assert mgr.read() is None

    def test_version_mismatch(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        with open(path, "w") as f:
            f.write('{"version": 999}')

        mgr = CheckpointManager(path, str(tmp_path))
        assert mgr.read() is None

    def test_atomic_write_no_tmp_left(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        mgr = CheckpointManager(path, str(tmp_path))

        mgr.write(
            date="20260520", last_seqno=1, output_minutes=set(),
            last_output_minute="", current_minute="",
            last_output_date="", first_data_received=False,
            files={},
            last_totalvol_by_symbol={}, last_totalamount_by_symbol={},
        )

        assert not (tmp_path / "checkpoint.json.tmp").exists()
        assert (tmp_path / "checkpoint.json").exists()
