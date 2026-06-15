from __future__ import annotations

import base64
import json
import logging
import os
import time
from typing import Any, Dict, Optional

from minute_bar.clock import JST, now_jst
from minute_bar.models import FileState

logger = logging.getLogger(__name__)

CHECKPOINT_VERSION = 3


class CheckpointManager:
    def __init__(self, path: str, output_dir: str):
        self._path = path
        self._output_dir = output_dir

    def write(
        self,
        date: str,
        last_seqno: int,
        output_minutes: set[str],
        last_output_minute: str,
        current_minute: str,
        last_output_date: str,
        first_data_received: bool,
        files: Dict[str, FileState],
        last_totalvol_by_symbol: Dict[str, int],
        last_totalamount_by_symbol: Dict[str, float],
    ) -> None:
        data = {
            "version": CHECKPOINT_VERSION,
            "date": date,
            "last_seqno": last_seqno,
            "output_minutes": sorted(output_minutes),
            "last_output_minute": last_output_minute,
            "current_minute": current_minute,
            "last_output_date": last_output_date,
            "first_data_received": first_data_received,
            "last_update_time": now_jst().isoformat(),
            "files": {},
            "last_totalvol_by_symbol": dict(last_totalvol_by_symbol),
            "last_totalamount_by_symbol": {k: v for k, v in last_totalamount_by_symbol.items()},
        }
        for name, state in files.items():
            data["files"][name] = {
                "offset": state.offset,
                "pending_line_base64": base64.b64encode(state.pending_line).decode("ascii"),
            }

        content = json.dumps(data, indent=2, ensure_ascii=False)
        tmp_path = self._path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        for attempt in range(5):
            try:
                os.replace(tmp_path, self._path)
                return
            except PermissionError:
                if attempt < 4:
                    logger.warning("Checkpoint rename retry %d/5", attempt + 1)
                    time.sleep(0.1)
                else:
                    raise

    def read(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self._path):
            logger.info("No checkpoint file found at %s", self._path)
            return None

        # Only read non-tmp files
        if self._path.endswith(".tmp"):
            logger.error("Refusing to read .tmp checkpoint file: %s", self._path)
            return None

        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read checkpoint: %s", e)
            return None

        if data.get("version") != CHECKPOINT_VERSION:
            logger.error(
                "Checkpoint version mismatch: expected %d, got %s",
                CHECKPOINT_VERSION, data.get("version"),
            )
            return None

        return data

    def get_file_states(self, data: Dict[str, Any]) -> Dict[str, FileState]:
        result = {}
        for name, info in data.get("files", {}).items():
            pending = base64.b64decode(info.get("pending_line_base64", ""))
            result[name] = FileState(
                offset=info.get("offset", 0),
                pending_line=pending,
                date=data.get("date", ""),
            )
        return result

    def get_last_totalvol(self, data: Dict[str, Any]) -> Dict[str, int]:
        return data.get("last_totalvol_by_symbol", {})

    def get_last_totalamount(self, data: Dict[str, Any]) -> Dict[str, float]:
        return data.get("last_totalamount_by_symbol", {})
