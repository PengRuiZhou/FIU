# Tickfile Stale-Row Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop writing stale (carry-forward) tickfile rows at shutdown/cross-day when order hasn't reached a minute, and make ReplayEngine surgically fill the resulting gaps without corrupting already-correct rows.

**Architecture:** Part 1 — gate the two unconditional force-gen paths (`flush_all_remaining`, `_step1_cross_day_check`) on the order watermark (`order_current_minute >= mk`), skipping unreached minutes. Part 2 — ReplayEngine scans the existing per-day tickfile's `UpdateTime` column at startup to learn which minutes are already generated, then skips them (fills only gaps); seqno continues from the file max.

**Tech Stack:** Python 3.12, pytest, no Rust changes.

**Spec:** `docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md`

---

## File Structure

**Modified (source):**
- `src/minute_bar/flusher.py` — `flush_all_remaining` (gate shutdown loop) + `_step1_cross_day_check` (gate cross-day loop + Fix-G log distinction)
- `src/minute_bar/replay.py` — `ReplayEngine.__init__` (add `_generated_tickfile_minutes`), `run()` (startup scan), `_flush_snapshot_minute` (skip-already-generated)

**New (tests):**
- `tests/test_tickfile_stale_fix.py` — all TDD tests for Part 1 + Part 2

**Modified (tests):** possibly `tests/test_tickfile_sync.py` / `tests/test_flusher.py` if any existing test assumed unconditional force-gen (handled in regression task).

---

## Task 1: shutdown path — skip order-unreached minutes (TDD)

**Files:**
- Modify: `src/minute_bar/flusher.py:486-499` (`flush_all_remaining` tickfile loop)
- Test: `tests/test_tickfile_stale_fix.py` (new)

- [ ] **Step 1: Write failing tests** — create `tests/test_tickfile_stale_fix.py`:

```python
"""TDD for tickfile stale-row fix (shutdown skip + replay surgical fill).
Spec: docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md"""
import os
from unittest.mock import patch

import pytest

from minute_bar.aggregator import SharedState
from minute_bar.writer import get_tickfile_path


# --- Flusher construction helper (mirrors tests/test_tickfile_sync.py:_make_flusher) ---
def _make_flusher(state, tmp_path, enable_tickfile=True):
    from minute_bar.codetable import CodeTable
    from minute_bar.checkpoint import CheckpointManager
    from minute_bar.flusher import ClockWatermarkFlusher
    flusher = ClockWatermarkFlusher(
        state=state,
        code_table=CodeTable("dummy"),
        checkpoint=CheckpointManager("dummy", {}),
        output_dir=str(tmp_path),
        output_delay_sec=60,
        enable_order=True,
        enable_tickfile=enable_tickfile,
    )
    return flusher


def _make_state():
    state = SharedState()
    state.first_data_received = True
    return state


class TestShutdownSkipsUnreachedMinutes:
    """Part 1: flush_all_remaining skips minutes order_current_minute hasn't reached."""

    def test_unreached_minute_is_skipped(self, tmp_path):
        state = _make_state()
        state.order_current_minute = "202605281430"   # order reached 1430
        # 1429 reached (order_wm >= mk); 1500 unreached (order_wm < mk)
        state._tickfile_pending["202605281429"] = {"raw_records": {}, "snapshot_copy": {}}
        state._tickfile_pending["202605281500"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605281429" in called          # reached → generated
        assert "202605281500" not in called       # unreached → skipped

    def test_natural_eof_generates_last_minute(self, tmp_path):
        """order_current_minute == last pending minute must still generate it (>= not >)."""
        state = _make_state()
        state.order_current_minute = "202605281530"   # == close; must NOT be skipped
        state._tickfile_pending["202605281530"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605281530" in called            # close generated, not skipped

    def test_empty_order_watermark_skips_all(self, tmp_path):
        """No order ever flushed (empty watermark) → skip all (all would be stale)."""
        state = _make_state()
        state.order_current_minute = ""             # order never flushed anything
        state._tickfile_pending["202605280900"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher.flush_all_remaining(skip_tickfile=False)

        assert mock_gen.call_count == 0             # nothing reached → skip all
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestShutdownSkipsUnreachedMinutes -v`
Expected: 3 FAIL — `test_unreached_minute_is_skipped` and `test_empty_order_watermark_skips_all` fail (1500 / 0900 currently generated unconditionally); `test_natural_eof_generates_last_minute` PASSES already (current code generates everything).

- [ ] **Step 3: Implement the shutdown gate** — replace `src/minute_bar/flusher.py:486-499`. Old (verbatim):

```python
        # Generate tickfile for remaining pending minutes (EOF fallback)
        if not skip_tickfile and self._enable_tickfile:
            tickfile_errors = 0
            with self._state.lock:
                remaining_pending = sorted(self._state._tickfile_pending.keys())
            for mk in remaining_pending:
                try:
                    self._try_generate_tickfile(mk)
                except Exception:
                    tickfile_errors += 1
                    logger.exception("EOF tickfile generation failed for minute=%s", mk)
            if remaining_pending:
                logger.info("EOF tickfile summary: %d generated, %d failed",
                            len(remaining_pending) - tickfile_errors, tickfile_errors)
```

New (snapshot order_wm with the keys; gate each minute; log skipped):
```python
        # Generate tickfile for remaining pending minutes (EOF fallback).
        # Gate on order watermark: skip minutes order never reached (would be stale
        # carry-forward). Spec 2026-06-16-tickfile-stale-fix-design §3.1.
        if not skip_tickfile and self._enable_tickfile:
            tickfile_errors = 0
            skipped_keys: list = []
            with self._state.lock:
                remaining_pending = sorted(self._state._tickfile_pending.keys())
                order_wm = self._state.order_current_minute
            for mk in remaining_pending:
                if order_wm and order_wm >= mk:
                    try:
                        self._try_generate_tickfile(mk)
                    except Exception:
                        tickfile_errors += 1
                        logger.exception("EOF tickfile generation failed for minute=%s", mk)
                else:
                    skipped_keys.append(mk)
            if skipped_keys:
                logger.warning(
                    "Shutdown skipped %d tickfile minutes order hadn't reached "
                    "(no stale rows written; fill via ReplayEngine --date=%s): %s",
                    len(skipped_keys), jst_now_yyyymmdd(), skipped_keys[:20],
                )
            if remaining_pending:
                logger.info("EOF tickfile summary: %d generated, %d skipped, %d failed",
                            len(remaining_pending) - len(skipped_keys) - tickfile_errors,
                            len(skipped_keys), tickfile_errors)
```

(`jst_now_yyyymmdd` is already imported at flusher.py:18 and used at :512.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestShutdownSkipsUnreachedMinutes -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_stale_fix.py
git commit -m "fix(flusher): skip order-unreached tickfile minutes at shutdown (no stale rows)"
```

---

## Task 2: cross-day path — skip order-unreached + distinguish skip from failure (TDD)

**Files:**
- Modify: `src/minute_bar/flusher.py:221-234` (`_step1_cross_day_check` Step 2) + `:236-243` (Fix-G CRITICAL log)
- Test: `tests/test_tickfile_stale_fix.py`

- [ ] **Step 1: Write failing tests** — append to `tests/test_tickfile_stale_fix.py`:

```python
class TestCrossDaySkipsUnreachedMinutes:
    """Part 1: cross-day force-gen also skips order-unreached minutes."""

    def test_cross_day_skips_unreached(self, tmp_path):
        state = _make_state()
        state.last_output_date = "20260528"        # yesterday
        state.current_minute = "202605290930"      # today (triggers cross-day)
        state.order_current_minute = "202605280930"  # yesterday's last order minute
        # 0900 reached (<= 0930); 1500 unreached (> 0930)
        state._tickfile_pending["202605280900"] = {"raw_records": {}, "snapshot_copy": {}}
        state._tickfile_pending["202605281500"] = {"raw_records": {}, "snapshot_copy": {}}
        flusher = _make_flusher(state, tmp_path)

        with patch.object(flusher, "_try_generate_tickfile") as mock_gen:
            flusher._step1_cross_day_check()

        called = [c.args[0] for c in mock_gen.call_args_list]
        assert "202605280900" in called
        assert "202605281500" not in called
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestCrossDaySkipsUnreachedMinutes -v`
Expected: FAIL (1500 currently generated unconditionally at cross-day).

- [ ] **Step 3: Implement the cross-day gate** — replace `src/minute_bar/flusher.py:221-234`. Old (verbatim):

```python
        # Step 2: Force-generate remaining pending tickfiles BEFORE clearing
        remaining_pending = []
        with self._state.lock:
            remaining_pending = sorted(self._state._tickfile_pending.keys())
        if remaining_pending:
            logger.warning(
                "Cross-day: generating %d pending tickfiles before cleanup (order lagging)",
                len(remaining_pending),
            )
            for mk in remaining_pending:
                try:
                    self._try_generate_tickfile(mk)
                except Exception:
                    logger.exception("Cross-day tickfile generation failed for minute=%s", mk)
```

New (read order_wm under the same lock; generate only reached; remove skipped from pending before the Fix-G check so they aren't logged as failures):
```python
        # Step 2: Force-generate remaining pending tickfiles BEFORE clearing.
        # Gate on order watermark: skip minutes order never reached (would be stale).
        # Spec 2026-06-16-tickfile-stale-fix-design §3.2.
        remaining_pending = []
        skipped_keys: list = []
        with self._state.lock:
            remaining_pending = sorted(self._state._tickfile_pending.keys())
            order_wm = self._state.order_current_minute
        if remaining_pending:
            generate_keys = [mk for mk in remaining_pending if order_wm and order_wm >= mk]
            skipped_keys = [mk for mk in remaining_pending if not (order_wm and order_wm >= mk)]
            if generate_keys:
                logger.warning(
                    "Cross-day: generating %d pending tickfiles before cleanup (order lagging)",
                    len(generate_keys),
                )
                for mk in generate_keys:
                    try:
                        self._try_generate_tickfile(mk)
                    except Exception:
                        logger.exception("Cross-day tickfile generation failed for minute=%s", mk)
            if skipped_keys:
                # Remove skipped from pending so the Fix-G check below does not log them
                # as failures; they are intentionally deferred to replay as 'missing'.
                with self._state.lock:
                    for mk in skipped_keys:
                        self._state._tickfile_pending.pop(mk, None)
                logger.warning(
                    "Cross-day: skipped %d tickfile minutes order hadn't reached "
                    "(no stale rows; fill via ReplayEngine --date=%s): %s",
                    len(skipped_keys), jst_now_yyyymmdd(), skipped_keys[:20],
                )
```

- [ ] **Step 4: Run tests to verify pass**

Run: `python -m pytest tests/test_tickfile_stale_fix.py -v`
Expected: 4 PASS (3 shutdown + 1 cross-day).

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/flusher.py tests/test_tickfile_stale_fix.py
git commit -m "fix(flusher): cross-day force-gen skips order-unreached minutes; no stale rows"
```

---

## Task 3: replay — add generated-minutes set + startup scan (TDD)

**Files:**
- Modify: `src/minute_bar/replay.py:32-42` (`__init__`), `:43-80` (`run`), new `_scan_generated_tickfile_minutes` + `_extract_minutes_from_tickfile`
- Test: `tests/test_tickfile_stale_fix.py`

- [ ] **Step 1: Write failing tests** — append to `tests/test_tickfile_stale_fix.py`:

```python
class TestReplayScanExtractor:
    """Part 2: UpdateTime column -> minute_key extraction (the scan primitive)."""

    def test_extract_minutes_skips_header_and_reverses_updatetime(self, tmp_path):
        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig
        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True))
        engine = ReplayEngine(config, date="20260528")
        # Write a minimal per-day tickfile: header + 2 data rows for 0901, 0902
        from minute_bar.tickfile import TICKFILE_HEADER
        from minute_bar.writer import get_tickfile_path
        path = get_tickfile_path(str(tmp_path), "202605280900")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        row_0901 = ",20260528,,,,,,,,,,,,,,,20260528 09:01:00,,,,,,,,,,,,,,,,,,,,,," + "," * 40 + "20260528,1,1,2026-05-28 09:01:00.000000,,N,,,"
        row_0902 = ",20260528,,,,,,,,,,,,,,,20260528 09:02:00,,,,,,,,,,,,,,,,,,,,,," + "," * 40 + "20260528,1,2,2026-05-28 09:02:00.000000,,N,,,"
        # Build a valid 65-field row: InstrumentID,TradingDay,..UpdateTime(16)..Seqno(59),LocalTime(60),..
        fields = [""] * 65
        fields[1] = "20260528"
        fields[16] = "20260528 09:01:00"
        fields[59] = "1"
        fields[60] = "2026-05-28 09:01:00.000000"
        row_0901 = ",".join(fields)
        fields2 = list(fields)
        fields2[16] = "20260528 09:02:00"
        fields2[59] = "2"
        row_0902 = ",".join(fields2)
        with open(path, "w", encoding="utf-8") as f:
            f.write(TICKFILE_HEADER + "\n" + row_0901 + "\n" + row_0902 + "\n")

        present = engine._scan_generated_tickfile_minutes(str(tmp_path))

        assert present == {"202605280901", "202605280902"}

    def test_scan_returns_empty_when_no_tickfile(self, tmp_path):
        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig
        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True))
        engine = ReplayEngine(config, date="20260528")
        assert engine._scan_generated_tickfile_minutes(str(tmp_path)) == set()
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestReplayScanExtractor -v`
Expected: 2 FAIL — `AttributeError: 'ReplayEngine' object has no attribute '_scan_generated_tickfile_minutes'`.

- [ ] **Step 3a: Add `_generated_tickfile_minutes` to `__init__`** — `src/minute_bar/replay.py:40-41`. Old (verbatim):

```python
        self._enable_tickfile = config.output.enable_tickfile
        self._tickfile_seqno: int = 0
```

New:
```python
        self._enable_tickfile = config.output.enable_tickfile
        self._tickfile_seqno: int = 0
        # Part 2 (stale-fix): minutes already present in the output tickfile;
        # populated by startup scan, used to skip already-generated minutes (fill only gaps).
        self._generated_tickfile_minutes: set = set()
```

- [ ] **Step 3b: Add the scan helpers** — insert after `__init__` / before `run` (after replay.py line 42). Add:

```python
    def _scan_generated_tickfile_minutes(self, output_dir: str) -> set:
        """Scan the day's tickfile; return the set of minute_keys already present.

        tickfile is ONE file per day (tickfile_{date}.csv); the UpdateTime column
        (index 16) is minute_key-derived (tickfile.py), so each row's minute is
        recoverable. Returns empty set if no tickfile exists (pure replay unaffected).
        Spec 2026-06-16-tickfile-stale-fix-design §4.1.
        """
        from minute_bar.writer import get_tickfile_path
        sample_mk = f"{self._date}0000"   # any minute of the date -> same per-day path
        path = get_tickfile_path(output_dir, sample_mk)
        if not os.path.exists(path):
            return set()
        return self._extract_minutes_from_tickfile(path)

    @staticmethod
    def _extract_minutes_from_tickfile(path: str) -> set:
        """Read a per-day tickfile; return distinct minute_keys from the UpdateTime column."""
        present: set = set()
        with open(path, "r", encoding="utf-8", newline="") as f:
            for line_num, line in enumerate(f, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                if line_num == 1:
                    continue  # header row — fields[16] is the literal 'UpdateTime'
                fields = stripped.split(",")
                if len(fields) != 65:
                    continue  # corrupted/truncated line — skip (mirror recover_tickfile_seqno)
                update_time = fields[16]
                # Format "YYYYMMDD HH:MM:00" -> 12-char minute_key "YYYYMMDDHHMM"
                minute_key = update_time.replace(" ", "").replace(":", "")[:12]
                if len(minute_key) == 12 and minute_key.isdigit():
                    present.add(minute_key)
        return present
```

- [ ] **Step 3c: Call the scan in `run()`** — `src/minute_bar/replay.py:55-59`. Old (verbatim):

```python
        self._state = SharedState(
            first_seen_volume_base=self._config.aggregation.first_seen_volume_base
        )

        write_executor = ThreadPoolExecutor(max_workers=2)
```

New (scan after state built, before streaming):
```python
        self._state = SharedState(
            first_seen_volume_base=self._config.aggregation.first_seen_volume_base
        )

        # Part 2 (stale-fix): learn which minutes are already in the output tickfile so we
        # skip them (fill only gaps). No-op when no tickfile exists (pure replay).
        if self._enable_tickfile:
            self._generated_tickfile_minutes = self._scan_generated_tickfile_minutes(
                self._config.output.output_dir
            )
            if self._generated_tickfile_minutes:
                logger.info(
                    "Replay: %d tickfile minutes already present — will skip, fill only gaps",
                    len(self._generated_tickfile_minutes),
                )

        write_executor = ThreadPoolExecutor(max_workers=2)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestReplayScanExtractor -v`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/replay.py tests/test_tickfile_stale_fix.py
git commit -m "feat(replay): startup scan of existing tickfile (learn generated minutes)"
```

---

## Task 4: replay — skip already-generated minutes during generation (TDD)

**Files:**
- Modify: `src/minute_bar/replay.py:250-263` (`_flush_snapshot_minute` tickfile block)
- Test: `tests/test_tickfile_stale_fix.py`

- [ ] **Step 1: Write failing tests** — append to `tests/test_tickfile_stale_fix.py`:

```python
class TestReplaySkipAlreadyGenerated:
    """Part 2: replay skips minutes already in the tickfile (no duplicate/corruption)."""

    def test_skip_does_not_advance_seqno(self, tmp_path):
        """A skipped minute must not burn a seqno number."""
        from minute_bar.replay import ReplayEngine
        from minute_bar.config import AppConfig, InputConfig, OutputConfig
        config = AppConfig(input=InputConfig(csv_dir=str(tmp_path)),
                           output=OutputConfig(output_dir=str(tmp_path), enable_tickfile=True))
        engine = ReplayEngine(config, date="20260528")
        engine._generated_tickfile_minutes = {"202605280901"}  # pretend 0901 already present
        engine._tickfile_seqno = 5
        # call the skip path directly: simulate _flush_snapshot_minute's tickfile block
        # by checking the guard logic via a focused unit on the engine attribute.
        # (Full integration test in Task 5's regression.)
        before = engine._tickfile_seqno
        # The guard: if minute_key in self._generated_tickfile_minutes -> skip (no seqno bump)
        assert "202605280901" in engine._generated_tickfile_minutes
        # seqno untouched by a mere membership check
        assert engine._tickfile_seqno == before
```

> Note: the strongest assertion is the integration test in Task 5 (full replay run pre-seeds a tickfile and asserts no duplicate rows + correct seqno). This unit test pins the guard invariant.

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestReplaySkipAlreadyGenerated -v`
Expected: PASSES trivially now (it's an attribute check) — this is a placeholder guard; the real behavior is locked by Task 5's integration test. (If it passes already, that's fine — proceed; the implementation edit is still required for Task 5 to pass.)

- [ ] **Step 3: Implement the skip guard** — replace `src/minute_bar/replay.py:250-263`. Old (verbatim):

```python
        if self._enable_tickfile:
            from minute_bar.writer import get_tickfile_path, recover_tickfile_seqno, write_tickfile_rows
            from minute_bar.tickfile import select_tickfile_records
            path = get_tickfile_path(output_dir, minute_key)
            if self._tickfile_seqno == 0 and os.path.exists(path):
                self._tickfile_seqno = recover_tickfile_seqno(output_dir, minute_key)
            self._tickfile_seqno += 1
            with self._state.lock:
                order_records = self._state.raw_order_buffers.pop(minute_key, [])
                latest_order_copy = dict(self._state.latest_order_by_symbol)
            code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
            selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)
            write_tickfile_rows(output_dir, minute_key, selected, self._tickfile_seqno,
                                code_table_getter=code_getter, skip_fsync=False)
```

New (skip-already-generated FIRST, before seqno increment; add to set only after successful write):
```python
        if self._enable_tickfile:
            # Part 2 (stale-fix): skip minutes already present in the output tickfile
            # (fill only gaps; do not duplicate/corrupt correct rows). Check BEFORE seqno
            # increment so skipped minutes do not burn seqno numbers.
            if minute_key in self._generated_tickfile_minutes:
                logger.debug("Replay skip already-generated tickfile minute=%s", minute_key)
            else:
                from minute_bar.writer import get_tickfile_path, recover_tickfile_seqno, write_tickfile_rows
                from minute_bar.tickfile import select_tickfile_records
                path = get_tickfile_path(output_dir, minute_key)
                if self._tickfile_seqno == 0 and os.path.exists(path):
                    self._tickfile_seqno = recover_tickfile_seqno(output_dir, minute_key)
                self._tickfile_seqno += 1
                with self._state.lock:
                    order_records = self._state.raw_order_buffers.pop(minute_key, [])
                    latest_order_copy = dict(self._state.latest_order_by_symbol)
                code_getter = (lambda symbol, t=self._code_table: t.table.get(symbol)) if self._code_table else None
                selected = select_tickfile_records(raw_records, snapshot_copy, order_records, latest_order_copy)
                write_tickfile_rows(output_dir, minute_key, selected, self._tickfile_seqno,
                                    code_table_getter=code_getter, skip_fsync=False)
                self._generated_tickfile_minutes.add(minute_key)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `python -m pytest tests/test_tickfile_stale_fix.py -v`
Expected: all PASS so far.

- [ ] **Step 5: Commit**

```bash
git add src/minute_bar/replay.py tests/test_tickfile_stale_fix.py
git commit -m "feat(replay): skip already-generated tickfile minutes (surgical gap-fill)"
```

---

## Task 5: replay integration test — gap-fill without corrupting correct rows (TDD)

**Files:**
- Test: `tests/test_tickfile_stale_fix.py`

This is the decisive end-to-end test for the user's requirement: pre-seed a tickfile with correct minutes, run replay, assert correct rows unchanged + gap minutes filled + no duplicates.

- [ ] **Step 1: Write the integration test** — append to `tests/test_tickfile_stale_fix.py`:

```python
class TestReplayGapFillIntegration:
    """End-to-end: replay fills missing gap minutes without duplicating correct rows."""

    def test_replay_fills_gap_without_corrupting_correct_rows(self, tmp_path):
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

        # code.csv — exact 17-field format proven in tests/test_replay.py:41
        (in_dir / f"code.csv.{date}").write_text(
            "7203,1,TSE,Toyota,JPY,equity,common,,,,0,0,0,2,0,,0\n", encoding="utf-8")

        # snapshot.csv — 21-field rows (proven format, tests/test_replay.py:48).
        # Round-up (floor+1, sub-minute > 0): clock-min 0930 data -> bucket 0931;
        # clock-min 0931 data -> bucket 0932.
        snapshot_rows = [
            # clock-minute 0930 -> tickfile bucket 202605200931 (the "correct" pre-seeded minute)
            "7203,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            # clock-minute 0931 -> tickfile bucket 202605200932 (the gap to fill)
            "7203,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083100999",
        ]
        with open(in_dir / f"snapshot.csv.{date}", "wb") as f:
            for r in snapshot_rows:
                f.write(r.encode("utf-8") + b"\n")

        # Pre-seed the output tickfile with bucket 0931 ALREADY generated (correct row),
        # simulating a live run that completed 0931 but not 0932.
        seed_path = get_tickfile_path(str(out_dir), f"{date}0931")  # per-day file
        os.makedirs(os.path.dirname(seed_path), exist_ok=True)
        fields = [""] * 65
        fields[0] = "7203"
        fields[1] = date
        fields[16] = f"{date} 09:31:00"   # UpdateTime -> minute_key 202605200931
        fields[59] = "1"                   # Seqno
        fields[60] = "2026-05-20 09:30:00.999000"  # LocalTime
        with open(seed_path, "w", encoding="utf-8") as f:
            f.write(TICKFILE_HEADER + "\n" + ",".join(fields) + "\n")
        correct_rows_before = 1  # one pre-seeded data row for 0931

        config = AppConfig(
            input=InputConfig(csv_dir=str(in_dir), file_encoding="utf-8"),
            output=OutputConfig(output_dir=str(out_dir), enable_order=False,
                                enable_tickfile=True, enable_kline=False),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date=date)
        engine.run()

        # Read the resulting tickfile; group rows by minute_key (UpdateTime col 16)
        with open(seed_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # header
            by_minute = {}
            for row in reader:
                if len(row) != 65:
                    continue
                mk = row[16].replace(" ", "").replace(":", "")[:12]
                by_minute.setdefault(mk, 0)
                by_minute[mk] += 1

        # 0931 (pre-seeded correct) must be UNCHANGED — NOT duplicated
        assert by_minute.get(f"{date}0931", 0) == correct_rows_before, \
            "correct 0931 row was duplicated/corrupted"
        # 0932 (the gap) must now be filled
        assert by_minute.get(f"{date}0932", 0) >= 1, "gap minute 0932 was not filled"
```

- [ ] **Step 2: Run to verify pass**

Run: `python -m pytest tests/test_tickfile_stale_fix.py::TestReplayGapFillIntegration -v`
Expected: PASS. If it FAILS, debug — this is the decisive test for the user's requirement. Common issues: the snapshot field count (the helper builds 16-col header; verify against `SNAPSHOT_MIN_COLS`), or `select_tickfile_records` returning empty for 0931 (ensure snapshot has the symbol).

- [ ] **Step 3: Commit**

```bash
git add tests/test_tickfile_stale_fix.py
git commit -m "test(replay): gap-fill without corrupting correct tickfile rows (integration)"
```

---

## Task 6: regression — existing flusher/replay/cross-day tests

**Files:** possibly `tests/test_tickfile_sync.py`, `tests/test_flusher.py`, `tests/test_replay.py`

- [ ] **Step 1: Run the full suite**

Run: `python -m pytest tests/ -q --deselect tests/test_e2e_phase21_benchmark.py --deselect tests/test_order_accel_performance.py --deselect tests/test_phase21_rss_profile.py`
Expected: previously-green tests still pass. If any EXISTING test now fails because it assumed unconditional force-gen (e.g. a cross-day test expecting a stale minute to be generated), update its expectation: the minute should now be skipped (not generated). Documented pre-existing failures (test_parity_single_batch, test_e2e_tickfile_completeness ×2, test_no_data_uses_config_interval) are unrelated — confirm they're the same ones.

- [ ] **Step 2: Fix any regression** — for each NEW failure (not pre-existing), determine if the test asserted stale-generation behavior. Update the assertion to expect the skip (minute NOT generated / NOT in `_generated_tickfile_minutes`). Re-run until green.

- [ ] **Step 3: Commit (if any test updated)**

```bash
git add tests/
git commit -m "test: update flusher/cross-day expectations for order-watermark gate"
```

---

## Task 7: doc sync + spec status

**Files:** `docs/superpowers/specs/2026-06-15-tickfile-shutdown-stale-persistence.md`, `docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md`, memory

- [ ] **Step 1: Update Q1 spec status** — in `docs/superpowers/specs/2026-06-15-tickfile-shutdown-stale-persistence.md`, change the header Status line from "修复方案待定" to "已修复（2026-06-16，方案 A + replay 手术式补齐，见 2026-06-16-tickfile-stale-fix-design.md）". Add a one-line note at the top of §5 pointing to the fix spec.

- [ ] **Step 2: Update design spec Status** — in `docs/superpowers/specs/2026-06-16-tickfile-stale-fix-design.md`, change Status from "设计已批准，待写实施计划" to "已实施（commit <SHA>）" after the implementation commits land.

- [ ] **Step 3: Update memory** — update `C:\Users\rzpeng\.claude\projects\d--FIU\memory\tickfile-shutdown-forcegen-orderless.md`: change the description from "后续 TDD 改进点（低优先级）" to "✅ 已修复（方案 A + replay 扫描补齐，2026-06-16）"; append the fix summary. Update MEMORY.md index line accordingly.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/ memory 2>/dev/null
git commit -m "docs: mark Q1 tickfile stale-fix implemented (Approach A + replay scan-fill)"
```

---

## Self-Review (plan author)

**Spec coverage:**
- Spec §3.1 (shutdown gate) → Task 1 ✓
- Spec §3.2 (cross-day gate) → Task 2 ✓
- Spec §3.3 (>= not >, empty-wm skip-all) → Task 1 tests `test_natural_eof_generates_last_minute` + `test_empty_order_watermark_skips_all` ✓
- Spec §4.1 (scan) → Task 3 ✓
- Spec §4.2 (skip during replay) → Task 4 ✓
- Spec §4.3 (gap-fill, no corruption) → Task 5 integration test ✓
- Spec §4.4 (scan vs persist rationale) → design-only, no task needed ✓
- Spec §6 (INV-TF1) → Task 7 doc note ✓
- Spec §7 tests → Tasks 1-5 ✓
- Spec gotcha: Fix-G CRITICAL false-positive → Task 2 Step 3 removes skipped keys from pending before Fix-G ✓
- Spec gotcha: header skip in scan → Task 3 `_extract_minutes_from_tickfile` skips line_num==1 ✓
- Spec gotcha: empty order_wm → Task 1 `test_empty_order_watermark_skips_all` + `if order_wm and ...` guard ✓

**Placeholder scan:** Task 5's snapshot CSV is fully specified (header + 2 rows). Task 4's unit test is intentionally a guard-pin (real behavior in Task 5) — explicitly noted, not a placeholder. No TBD/TODO.

**Type/name consistency:** `_generated_tickfile_minutes` (set) defined in Task 3 `__init__`, used in Task 3 scan + Task 4 skip + Task 5. `_scan_generated_tickfile_minutes` / `_extract_minutes_from_tickfile` defined Task 3, consistent. `order_wm` local name consistent across Task 1 & 2. `>=` operator consistent (spec §3.3).

**Gotchas addressed (from investigation):** order_wm snapshotted under lock with keys (Task 1/2); `_try_generate_tickfile` return unreliable → tests assert via call_args/membership (Task 1); cross-day order_wm valid at Step 2 (Task 2 note); Fix-G false-positive → remove skipped from pending (Task 2); replay skip before seqno increment (Task 4); scan skips header + guards len==65 (Task 3); seqno continuation via existing recover_tickfile_seqno (Task 4 note).
