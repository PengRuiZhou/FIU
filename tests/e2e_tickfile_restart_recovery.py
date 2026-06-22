"""Real end-to-end tests for the Tickfile Commit-Marker restart-recovery feature.

Two tests, NO mocks of the engine / recovery / simulator:

1. ``test_e2e_live_then_replay_restart_recovery``
   A live ``Engine`` (real ``data_simulator.Simulator`` at 100Kx over a bounded
   source slice) processes minutes [start..mid]; stop; inject a partial corrupt
   tail into the tickfile; a ``ReplayEngine`` runs over the SAME feed directory
   for the full source. Replay startup recovery must (a) truncate the partial
   tail, (b) seed the skip-set with already-committed minutes, then replay fills
   the [mid..end] gap with no duplicate minutes.

2. ``test_e2e_live_then_live_restart_recovery``
   Live engine #1 processes [start..mid]; stop; inject partial tail; a FRESH
   live ``Engine`` (#2) is constructed (its flusher ``__init__`` runs eager
   recovery, truncating the partial + seeding the skip-set) then ``start()``s
   and resumes [mid..end]. No minute is generated twice across the restart.

The bounded-source slice (an ~10-min [0900..0910] filter of the 5.7GB input,
cached at ``D:/FIU/test/_e2e_slice``) keeps each 100Kx simulator drain in a
handful of seconds while remaining 100% non-mocked.
"""
from __future__ import annotations

import csv
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TEST_DATE = "20260528"
FULL_INPUT_DIR = Path("D:/FIU/input")
SLICE_CACHE_DIR = Path("D:/FIU/test/_e2e_slice")
PARTIAL_TAIL = b"7203,CRASHPARTIAL,corrupt,tail,not,a,valid,65field,row\n"

# Minute window: small enough to be fast at 100Kx, big enough to exercise the
# restart boundary. [0900..0906] = 7 minutes; engine #1 / replay-pre fills
# [0900..0903], restart fills [0903..0906]. Source has ~1M order rows +
# ~70K snapshot rows per minute for every minute in this window.
LIVE_START = f"{TEST_DATE}0900"
LIVE_MID = f"{TEST_DATE}0903"
LIVE_END = f"{TEST_DATE}0906"

# Slice window used to build the bounded source (must fully contain LIVE_START..LIVE_END
# with headroom so the simulator's watermark advance behaves naturally). [0900..0910]
# covers the test window 0900..0906 with margin while keeping replay fast (~10 min of
# data vs the full 5.7GB day).
SLICE_FROM_PREFIX = f"{TEST_DATE}0900"
SLICE_TO_PREFIX = f"{TEST_DATE}0910"

# How long to poll the live engine for a target minute before giving up.
LIVE_POLL_TIMEOUT_SEC = 300


# ---------------------------------------------------------------------------
# STEP 1 — bounded source (cached, 100% real data)
# ---------------------------------------------------------------------------
def ensure_bounded_source(
    slice_dir: Path = SLICE_CACHE_DIR,
    date: str = TEST_DATE,
    from_ts_prefix: str = SLICE_FROM_PREFIX,
    to_ts_prefix: str = SLICE_TO_PREFIX,
    full_input_dir: Path = FULL_INPUT_DIR,
) -> Path:
    """Filter order/snapshot rows whose col-1 timestamp falls in
    [from_ts_prefix, to_ts_prefix] (minute granularity, first 12 digits) into a
    slice dir; copy code.csv verbatim. Cached: skipped if dst exists.

    Both order.csv and snapshot.csv carry the 17-digit timestamp in column
    index 1 (the ``time`` field, confirmed by reading the headers).
    """
    slice_dir.mkdir(parents=True, exist_ok=True)
    for ft in ("order", "snapshot"):
        src = full_input_dir / f"{ft}.csv.{date}"
        dst = slice_dir / f"{ft}.csv.{date}"
        if dst.exists():
            continue
        logger.info("Slicing %s -> %s ([%s..%s])", ft, dst, from_ts_prefix, to_ts_prefix)
        written = 0
        with open(src, "r", encoding="utf-8", newline="") as fin, \
             open(dst, "w", encoding="utf-8", newline="") as fout:
            # Preserve header verbatim
            header = fin.readline()
            fout.write(header)
            for line in fin:
                # timestamp is the 2nd field (index 1); a 17-digit int.
                parts = line.split(",", 2)
                if len(parts) < 2:
                    continue
                ts = parts[1]
                if len(ts) < 12:
                    continue
                if from_ts_prefix <= ts[:12] <= to_ts_prefix:
                    fout.write(line)
                    written += 1
        logger.info("Sliced %s: %d data rows", ft, written)

    code_src = full_input_dir / f"code.csv.{date}"
    code_dst = slice_dir / f"code.csv.{date}"
    if not code_dst.exists() and code_src.exists():
        shutil.copy2(code_src, code_dst)
    return slice_dir


# ---------------------------------------------------------------------------
# STEP 2 — SimFeed (real Simulator at 100Kx against the slice)
# ---------------------------------------------------------------------------
class SimFeed:
    """Drives the real data_simulator.Simulator in a daemon thread."""

    def __init__(self, source_dir: Path, work_dir: Path):
        from data_simulator.simulator import Simulator

        self.sim_out = work_dir / "sim_out"
        self.sim_out.mkdir(parents=True, exist_ok=True)
        self.sim = Simulator(
            source_dir=str(source_dir),
            output_dir=str(self.sim_out),
            speed=100000,
            date=TEST_DATE,
            file_types=["order", "snapshot", "code"],
            clean=True,
        )
        self.thread = threading.Thread(target=self.sim.run, name="simulator", daemon=True)
        self.thread.start()

    @property
    def csv_dir(self) -> str:
        return str(self.sim_out)

    def stop_and_join(self, timeout: float = 15.0) -> None:
        try:
            self.sim.stop()
        except Exception:
            logger.exception("SimFeed: simulator.stop() raised")
        self.thread.join(timeout=timeout)


# ---------------------------------------------------------------------------
# STEP 3 — assertion helpers
# ---------------------------------------------------------------------------
def read_tickfile_and_sidecar(tf_path: str) -> dict:
    """Return {by_minute, sidecar_lines, malformed} for a tickfile.

    ``by_minute`` maps minute_key -> row count. ``sidecar_lines`` is the list of
    non-empty lines in the ``.commit`` sidecar.
    """
    rep = {"by_minute": {}, "sidecar_lines": [], "malformed": 0}
    if not os.path.exists(tf_path):
        return rep
    with open(tf_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # header
        except StopIteration:
            return rep
        for row in reader:
            if len(row) != 65:
                rep["malformed"] += 1
                continue
            mk = row[16].replace(" ", "").replace(":", "")[:12]
            rep["by_minute"][mk] = rep["by_minute"].get(mk, 0) + 1
    sc = tf_path + ".commit"
    if os.path.exists(sc):
        with open(sc, "r", encoding="utf-8") as f:
            rep["sidecar_lines"] = [l.strip() for l in f if l.strip()]
    return rep


def get_tickfile_path(output_dir: str) -> str:
    """tickfile_{date}.csv lives under tickfile/<yyyy>/<yyyymmdd>/."""
    return os.path.join(
        output_dir, "tickfile", TEST_DATE[:4], TEST_DATE, f"tickfile_{TEST_DATE}.csv"
    )


def _sidecar_rowcounts(sidecar_lines: list) -> dict:
    """Parse sidecar lines into {minute_key: rowcount}.

    A minute is duplicated across a restart if its actual row count in the
    tickfile EXCEEDS the committed rowcount recorded in the sidecar (each
    committed minute = exactly one sidecar line = one row block).
    """
    out = {}
    for line in sidecar_lines:
        parts = line.split(",")
        if len(parts) != 4:
            continue
        minute, _offset, rowcount, _seqno = parts
        try:
            out[minute] = int(rowcount)
        except ValueError:
            continue
    return out


def wait_until_minute_committed(engine, target_minute: str, timeout: float) -> None:
    """Poll the live engine until ``target_minute`` is in the tickfile generated set."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        with engine._state.lock:
            done = target_minute in engine._state._generated_tickfile_minutes
        if done:
            return
        time.sleep(0.2)
    raise TimeoutError(
        f"Engine did not commit tickfile minute {target_minute} within {timeout}s"
    )


def stop_live_engine(engine, eng_thread, grace_sec: float = 3.0) -> None:
    """Mirror stale_fix_demo's stop pattern: a short grace for in-flight records
    to drain, then flip _running, join, and swallow the RuntimeError that
    stop() raises when the order thread (mid Rust batch) misses its join timeout.
    The tickfile flush has already run by then; the daemon order thread dies at
    process exit."""
    if grace_sec > 0:
        time.sleep(grace_sec)
    engine._running = False
    try:
        engine.stop()
    except RuntimeError as e:
        logger.warning("engine.stop() join-timeout reported (flush already ran): %s", e)
    if eng_thread is not None:
        eng_thread.join(timeout=30)


# ---------------------------------------------------------------------------
# STEP 4 — shared config builder
# ---------------------------------------------------------------------------
def make_live_config(csv_dir: str, output_dir: str, ckpt: str):
    from minute_bar.config import (
        AggregationConfig,
        AppConfig,
        InputConfig,
        OutputConfig,
        RecoveryConfig,
    )

    return AppConfig(
        input=InputConfig(
            csv_dir=csv_dir,
            target_date=TEST_DATE,
            order_chunk_size_bytes=524288,
            file_encoding="utf-8",
            poll_interval_ms=50,
            # Phase 21 Rust acceleration — REQUIRED to keep the order thread ahead of
            # the 100Kx simulator at the 0900 open peak (~1M order rows/min). Without
            # these the Python parser stalls the order watermark (mirrors
            # test/phase21_benchmark/{stale_fix_demo,full_day_run}.py configs).
            enable_order_accel=True,
            enable_rust_order_full_batch=True,
            enable_rust_snapshot_batch=True,
            enable_rust_tickfile=True,
        ),
        output=OutputConfig(
            output_dir=output_dir,
            enable_order=True,
            enable_tickfile=True,
            enable_kline=False,
            enable_full_snapshot=False,
            enable_full_kline=False,
        ),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        recovery=RecoveryConfig(
            checkpoint_file=ckpt,
            output_delay_sec=0,
            data_flush_delay_minutes=0,
            enable_time_fallback=False,
            stall_flush_sec=30,
            enable_tickfile_commit_marker=True,
        ),
    )


def make_replay_config(csv_dir: str, output_dir: str):
    from minute_bar.config import (
        AggregationConfig,
        AppConfig,
        InputConfig,
        OutputConfig,
    )

    return AppConfig(
        input=InputConfig(
            csv_dir=csv_dir,
            target_date=TEST_DATE,
            file_encoding="utf-8",
        ),
        output=OutputConfig(
            output_dir=output_dir,
            enable_order=True,
            enable_tickfile=True,
            enable_kline=False,
            enable_full_snapshot=False,
            enable_full_kline=False,
        ),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        recovery=None,  # ReplayEngine does not read recovery.* except enable_tickfile_commit_marker
    )


def _make_replay_recovery_cfg(cfg):
    """ReplayEngine reads cfg.recovery.enable_tickfile_commit_marker; ensure a
    RecoveryConfig is present with the flag enabled."""
    from minute_bar.config import RecoveryConfig
    cfg.recovery = RecoveryConfig(enable_tickfile_commit_marker=True)
    return cfg


# ---------------------------------------------------------------------------
# TEST 1 — live → replay restart recovery
# ---------------------------------------------------------------------------
@pytest.mark.e2e
@pytest.mark.slow
def test_e2e_live_then_replay_restart_recovery(tmp_path):
    from minute_bar.engine import Engine
    from minute_bar.replay import ReplayEngine

    slice_dir = ensure_bounded_source()

    work = tmp_path / "t1"
    engine_out = work / "engine_out"
    engine_out.mkdir(parents=True, exist_ok=True)

    # --- live engine #1: process [LIVE_START..LIVE_MID] ---
    feed = SimFeed(slice_dir, work)
    try:
        config = make_live_config(
            csv_dir=feed.csv_dir, output_dir=str(engine_out),
            ckpt=str(work / "engine_ckpt.json"),
        )
        engine = Engine(config)
        eng_thread = threading.Thread(target=engine.start, name="engine1", daemon=True)
        eng_thread.start()

        wait_until_minute_committed(engine, LIVE_MID, LIVE_POLL_TIMEOUT_SEC)
        logger.info("T1: engine#1 committed through %s; stopping", LIVE_MID)

        stop_live_engine(engine, eng_thread)
    finally:
        feed.stop_and_join()

    tf_path = get_tickfile_path(str(engine_out))

    # Sanity: engine #1 actually produced committed minutes up to LIVE_MID
    rep_before = read_tickfile_and_sidecar(tf_path)
    pre_committed = sorted(rep_before["by_minute"].keys())
    logger.info("T1: pre-inject committed minutes = %s", pre_committed)
    logger.info("T1: pre-inject by_minute counts = %s", rep_before["by_minute"])
    logger.info("T1: pre-inject sidecar (%d lines): %s",
                len(rep_before["sidecar_lines"]), rep_before["sidecar_lines"])
    assert pre_committed, "engine #1 should have committed at least one tickfile minute"
    assert rep_before["malformed"] == 0, "no malformed rows before injection"
    committed_before_mid = [m for m in pre_committed if m <= LIVE_MID]
    assert committed_before_mid, "expected committed minutes at-or-before LIVE_MID"

    size_before_inject = os.path.getsize(tf_path)

    # --- inject partial corrupt tail (simulates a mid-append hard crash) ---
    with open(tf_path, "ab") as f:
        f.write(PARTIAL_TAIL)
    size_after_inject = os.path.getsize(tf_path)
    assert size_after_inject > size_before_inject

    # --- ReplayEngine over the full (static) source. Replay does its own
    # throttled streaming via FileTailer from static files; it must NOT read a
    # live simulator output (the simulator races ahead and replay hits EOF
    # before the data lands). Point it at the bounded slice directly, mirroring
    # stale_fix_demo.py phase_b which uses csv_dir=SOURCE_DIR (the static input).
    replay_cfg = make_replay_config(str(slice_dir), str(engine_out))
    replay_cfg = _make_replay_recovery_cfg(replay_cfg)
    replay_engine = ReplayEngine(replay_cfg, date=TEST_DATE)
    replay_engine.run()

    # --- assertions ---
    rep_after = read_tickfile_and_sidecar(tf_path)
    logger.info("T1: post-replay by_minute counts = %s", rep_after["by_minute"])
    logger.info("T1: post-replay sidecar (%d lines)", len(rep_after["sidecar_lines"]))

    # (a) partial tail truncated — zero malformed rows survive
    assert rep_after["malformed"] == 0, (
        f"partial corrupt tail was NOT truncated: {rep_after['malformed']} malformed rows"
    )

    # (b) no DUPLICATE minute-block across the restart. Each committed minute =
    # one sidecar line recording its rowcount; replay's REGEN-GUARD returns
    # "committed" and writes ZERO bytes for already-committed minutes, so a
    # minute's actual row count must never EXCEED its sidecar rowcount. A
    # duplicate (regenerated block stacked on the committed one) would push the
    # count above the sidecar value.
    sc_rowcounts = _sidecar_rowcounts(rep_after["sidecar_lines"])
    overcounts = {
        m: (rep_after["by_minute"][m], rc)
        for m, rc in sc_rowcounts.items()
        if rep_after["by_minute"].get(m, 0) > rc
    }
    assert not overcounts, (
        f"duplicate tickfile minute-blocks after replay (actual > sidecar rowcount): {overcounts}"
    )

    # (c) the minutes engine #1 committed are still present (replay skipped them,
    #     did not delete them) — pick any one pre-MID minute and assert it survived.
    survivor = committed_before_mid[-1]
    assert survivor in rep_after["by_minute"], (
        f"pre-restart committed minute {survivor} vanished after replay"
    )

    # (d) the gap after LIVE_MID was filled — at least one minute > LIVE_MID is present
    post_mid = sorted(m for m in rep_after["by_minute"] if m > LIVE_MID)
    assert post_mid, (
        f"replay did not fill any minutes after {LIVE_MID}; by_minute={rep_after['by_minute']}"
    )

    # (e) sidecar consistent: each sidecar line's minute appears in by_minute, and the
    #     last sidecar offset == current tickfile size (no dangling partial).
    sc_minutes = []
    for line in rep_after["sidecar_lines"]:
        parts = line.split(",")
        assert len(parts) == 4, f"bad sidecar line: {line}"
        sc_minutes.append(parts[0])
    for m in sc_minutes:
        assert m in rep_after["by_minute"], (
            f"sidecar lists minute {m} but it is absent from the tickfile rows"
        )
    if rep_after["sidecar_lines"]:
        last_offset = int(rep_after["sidecar_lines"][-1].split(",")[1])
        assert last_offset == os.path.getsize(tf_path), (
            f"sidecar last offset {last_offset} != tickfile size {os.path.getsize(tf_path)}"
        )

    logger.info("T1 PASS: by_minute=%s, sidecar=%d lines",
                {m: c for m, c in sorted(rep_after["by_minute"].items())},
                len(rep_after["sidecar_lines"]))


# ---------------------------------------------------------------------------
# TEST 2 — live → live restart recovery
# ---------------------------------------------------------------------------
@pytest.mark.e2e
@pytest.mark.slow
def test_e2e_live_then_live_restart_recovery(tmp_path):
    from minute_bar.engine import Engine

    slice_dir = ensure_bounded_source()

    work = tmp_path / "t2"
    engine_out = work / "engine_out"
    engine_out.mkdir(parents=True, exist_ok=True)

    # --- live engine #1: process [LIVE_START..LIVE_MID] ---
    feed = SimFeed(slice_dir, work)
    try:
        config1 = make_live_config(
            csv_dir=feed.csv_dir, output_dir=str(engine_out),
            ckpt=str(work / "engine_ckpt.json"),
        )
        engine1 = Engine(config1)
        eng_thread1 = threading.Thread(target=engine1.start, name="engine1", daemon=True)
        eng_thread1.start()

        wait_until_minute_committed(engine1, LIVE_MID, LIVE_POLL_TIMEOUT_SEC)
        logger.info("T2: engine#1 committed through %s; stopping", LIVE_MID)

        stop_live_engine(engine1, eng_thread1)
    finally:
        feed.stop_and_join()

    tf_path = get_tickfile_path(str(engine_out))

    rep_before = read_tickfile_and_sidecar(tf_path)
    pre_committed = set(rep_before["by_minute"].keys())
    logger.info("T2: pre-inject committed minutes = %s", sorted(pre_committed))
    assert pre_committed, "engine #1 should have committed at least one tickfile minute"
    assert rep_before["malformed"] == 0
    committed_before_mid = {m for m in pre_committed if m <= LIVE_MID}
    assert committed_before_mid

    # --- inject partial corrupt tail ---
    with open(tf_path, "ab") as f:
        f.write(PARTIAL_TAIL)

    # --- fresh live engine #2: __init__ eager recovery truncates + seeds skip-set ---
    feed2_dir = work / "feed2"
    feed2_dir.mkdir(parents=True, exist_ok=True)
    feed2 = SimFeed(slice_dir, feed2_dir)
    try:
        # Recovery runs in ClockWatermarkFlusher.__init__ via jst_now_yyyymmdd();
        # patch it so it resolves to our test date while engine #2 is constructed.
        with patch("minute_bar.flusher.jst_now_yyyymmdd", return_value=TEST_DATE):
            config2 = make_live_config(
                csv_dir=feed2.csv_dir, output_dir=str(engine_out),
                ckpt=str(work / "engine_ckpt2.json"),
            )
            engine2 = Engine(config2)

        # (assertion i) eager init recovery truncated the partial tail
        rep_after_init = read_tickfile_and_sidecar(tf_path)
        assert rep_after_init["malformed"] == 0, (
            f"engine#2 init did NOT truncate partial tail: {rep_after_init['malformed']} malformed"
        )

        # (assertion ii) the skip-set was seeded with the pre-restart committed minutes
        with engine2._state.lock:
            seeded = set(engine2._state._generated_tickfile_minutes)
        missing_in_skipset = committed_before_mid - seeded
        assert not missing_in_skipset, (
            f"engine#2 skip-set missing pre-restart committed minutes: {sorted(missing_in_skipset)}"
        )

        # (assertion iii) a backup of the truncated tail exists (.truncated.*).
        # The backup file is named ``tickfile_<date>.csv.truncated.<time_ns>.<pid>``.
        backup_prefix = os.path.basename(tf_path) + ".truncated."  # tickfile_<date>.csv.truncated.
        tf_dir = os.path.dirname(tf_path)
        backups = [f for f in os.listdir(tf_dir) if f.startswith(backup_prefix)]
        assert backups, (
            f"expected a .truncated.* backup of the injected tail in {tf_dir}, found none"
        )

        # --- start engine #2 and let it resume [LIVE_MID..LIVE_END] ---
        eng_thread2 = threading.Thread(target=engine2.start, name="engine2", daemon=True)
        eng_thread2.start()

        wait_until_minute_committed(engine2, LIVE_END, LIVE_POLL_TIMEOUT_SEC)
        logger.info("T2: engine#2 committed through %s; stopping", LIVE_END)

        stop_live_engine(engine2, eng_thread2)
    finally:
        feed2.stop_and_join()

    # --- final assertions ---
    rep_final = read_tickfile_and_sidecar(tf_path)

    # (a) partial gone
    assert rep_final["malformed"] == 0, (
        f"partial corrupt tail present after engine#2: {rep_final['malformed']} malformed"
    )

    # (b) no duplicate minute-block across the restart (the core skip-set
    # guarantee). Each minute that engine #2 re-encountered was in the seeded
    # skip-set, so the REGEN-GUARD must have returned "committed" (zero bytes).
    # A duplicate = a minute's actual row count EXCEEDS its sidecar rowcount.
    sc_rowcounts = _sidecar_rowcounts(rep_final["sidecar_lines"])
    overcounts = {
        m: (rep_final["by_minute"][m], rc)
        for m, rc in sc_rowcounts.items()
        if rep_final["by_minute"].get(m, 0) > rc
    }
    assert not overcounts, (
        f"duplicate tickfile minute-blocks across restart (actual > sidecar rowcount): {overcounts}"
    )

    # (c) every minute that engine #1 committed still has exactly one block of rows
    #     (engine #2 did not regenerate them).
    for m in committed_before_mid:
        assert m in rep_final["by_minute"], (
            f"pre-restart minute {m} vanished after engine#2"
        )

    # (d) engine #2 advanced past LIVE_MID
    post_mid = sorted(m for m in rep_final["by_minute"] if m > LIVE_MID)
    assert post_mid, (
        f"engine#2 did not commit any minutes after {LIVE_MID}; by_minute={rep_final['by_minute']}"
    )

    # (e) sidecar consistent
    for line in rep_final["sidecar_lines"]:
        parts = line.split(",")
        assert len(parts) == 4, f"bad sidecar line: {line}"
        assert parts[0] in rep_final["by_minute"], (
            f"sidecar minute {parts[0]} absent from tickfile rows"
        )
    if rep_final["sidecar_lines"]:
        last_offset = int(rep_final["sidecar_lines"][-1].split(",")[1])
        assert last_offset == os.path.getsize(tf_path), (
            f"sidecar last offset {last_offset} != tickfile size {os.path.getsize(tf_path)}"
        )

    logger.info("T2 PASS: by_minute=%s, sidecar=%d lines",
                {m: c for m, c in sorted(rep_final["by_minute"].items())},
                len(rep_final["sidecar_lines"]))
