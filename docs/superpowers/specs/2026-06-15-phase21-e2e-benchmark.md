# Phase 21 E2E Benchmark Specification

> **Date**: 2026-06-15
> **Status**: Ready to Execute
> **Parent**: `docs/superpowers/specs/2026-06-11-phase21-a-plus-b-design.md`

---

## 1. Overview

E2E benchmark validates Phase 21 Rust acceleration achieves target wall-clock performance for the 0900 peak minute in two modes:

1. **Replay mode**: Historical data replay with `ReplayEngine` — measures batch processing throughput
2. **Live mode**: `data_simulator` writes continuously, `Engine` tails and processes — measures continuous load

**Target**: 0900 wall-clock < 60s SLA (goal: 3–5s)

---

## 2. Architecture

```
┌─────────────────┐      ┌─────────────────┐
│ data_simulator  │      │     Engine     │
│  (live mode)    │ ───▶ │  (live mode)  │
│  tail -f CSV    │      │ tail + process │
└─────────────────┘      └────────┬────────┘
                                          │
                          ┌───────────────┴───────────┐
                          │  output/                 │
                          │  snapshot/ order/ tickfile │
                          └───────────────────────────┘
```

## 3. Test Modes

### 3.1 Replay Mode (Quick Sanity)

Uses `ReplayEngine` to replay historical data. Fast, deterministic.

```bash
# Run replay benchmark
python -m pytest tests/test_e2e_phase21_benchmark.py::TestPhase21E2E::test_replay_benchmark -v -s
```

**Measures**: Full replay wall-clock time for target date.

### 3.2 Live Mode (Production Simulation)

Starts `data_simulator` in background thread + `Engine` in live mode. Simulates continuous production load.

```bash
# Run live benchmark
python -m pytest tests/test_e2e_phase21_benchmark.py::TestPhase21E2E::test_live_benchmark -v -s
```

**Measures**:
- 0900 peak minute wall-clock
- Continuous throughput (records/second)
- Memory RSS delta
- GIL contention

#### Data flow

```
D:/FIU/input/               data_simulator            Engine (live mode)
  order.csv.20260528   ──▶  tmp/sim_out/        ──▶  tmp/engine_out/
  snapshot.csv.20260528     order.csv.20260528       snapshot/ order/ tickfile/
  code.csv.20260528         snapshot.csv.20260528    (tailed by FileTailer)
                            code.csv.20260528
                            (written at 100Kx speed,
                             tailed by Engine)
```

The simulator replays the raw historical CSVs to a temp dir at high speed
(`speed=100000`). The Engine's `csv_dir` points at the simulator's output dir,
so its `FileTailer` reads the files as they grow — exactly like production tailing.

#### 0900 peak measurement

The test does NOT run to end-of-day (the 5.4 GB order file takes too long). Instead
it times a single peak minute:

1. Poll `engine._state.order_current_minute` every 50 ms.
2. Record `start_0900` when `order_current_minute >= "202605280900"`.
3. Stop when `order_current_minute >= "202605280901"` (0900 fully processed).
4. `elapsed_0900 = now - start_0900`.

Hard cap: 300 s wall-clock for the whole test (covers simulator warmup + 0900).

#### Shutdown

```python
engine._running = False
engine.stop()        # joins worker threads, final flush
sim.stop()           # stops simulator threads, closes handles
```

---

## 4. SLA Thresholds

| Result | Wall-clock | Action |
|--------|------------|--------|
| GREEN | < 6s | Target achieved |
| YELLOW | 6–60s | SLA met, continue |
| RED | > 60s | **FAIL — rollback required** |

---

## 5. Test Data

### 5.1 Source Data

Historical CSV files in `D:/FIU/test/output`:
- `order.csv.YYYYMMDD` — order ticks
- `snapshot.csv.YYYYMMDD` — snapshot updates
- `code.csv.YYYYMMDD` — code table

### 5.2 Simulator Config

```ini
[speed] replay_speed = 100000  # 100K records/second

[output] clean = true  # Start fresh each run
```

---

## 6. Test Implementation

### 6.1 Live Benchmark Test

```python
def test_live_benchmark(self, rust_available, tmp_path):
    """Run live mode benchmark with data_simulator + Engine."""
    import threading, time
    from data_simulator import Simulator
    from minute_bar.engine import Engine

    # Setup
    simulator_out = tmp_path / "simulator_output"
    simulator_out.mkdir()
    engine_out = tmp_path / "engine_output"
    engine_out.mkdir()

    # Start simulator in background thread
    sim = Simulator(
        source_dir="D:/FIU/test/output",
        output_dir=str(simulator_out),
        speed=100000,
        file_types=["order", "snapshot", "code"],
        clean=True,
    )
    sim_thread = threading.Thread(target=sim.run, daemon=True)
    sim_thread.start()
    time.sleep(2)  # Let simulator warm up

    # Start engine
    config = make_config(str(engine_out), enable_rust=True)
    engine = Engine(config)
    engine.start()

    # Monitor 0900 processing
    start_0900 = None
    while engine.is_alive():
        with engine._state.lock:
            current = engine._state.current_minute
        if current and current >= "202605280900" and start_0900 is None:
            start_0900 = time.monotonic()
        if current and current > "202605281500":
            break
        time.sleep(0.1)

    engine.stop()
    sim.stop()

    elapsed_0900 = time.monotonic() - start_0900
    assert elapsed_0900 < 60, f"0900 wall-clock {elapsed_0900:.1f}s > 60s SLA"
```

### 6.2 Replay Benchmark Test

```python
def test_replay_benchmark(self, rust_available, tmp_path):
    """Run replay benchmark with ReplayEngine."""
    from minute_bar.replay import ReplayEngine
    import time

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    config = make_config(str(output_dir), enable_rust=True)
    engine = ReplayEngine(config, date=TEST_DATE)

    start = time.monotonic()
    engine.run()
    elapsed = time.monotonic() - start

    assert elapsed < 60, f"Replay wall-clock {elapsed:.1f}s > 60s SLA"
```

---

## 7. Monitoring

### 7.1 RSS Memory

```python
import psutil, os
p = psutil.Process(os.getpid())
rss_before = p.memory_info().rss
# ... run benchmark ...
rss_after = p.memory_info().rss
rss_delta = rss_after - rss_before
assert rss_delta < 300 * 1024 * 1024  # 300MB
```

### 7.2 GIL Contention

Profile GIL hold time during 0900 peak:
```python
# Use phase4 GIL profiler output
# Expected: Order thread ~0% GIL time (Rust releases GIL)
#           Snapshot thread ~0% GIL time (Rust handles parsing)
#           Tickfile ~50ms GIL time (CSV generation)
```

---

## 8. Execution

```bash
# Replay mode (fast, ~2 min)
python -m pytest tests/test_e2e_phase21_benchmark.py::TestPhase21E2E::test_replay_benchmark -v -s

# Live mode (slow, ~5 min)
python -m pytest tests/test_e2e_phase21_benchmark.py::TestPhase21E2E::test_live_benchmark -v -s

# Both modes
python -m pytest tests/test_e2e_phase21_benchmark.py -v -s
```

---

## 9. Success Criteria

| Metric | Target | Method |
|--------|--------|--------|
| Replay wall-clock | < 60s | `time.monotonic()` |
| Live 0900 wall-clock | < 60s | Engine state monitoring |
| RSS delta | < 300MB | `psutil` |
| Output parity | 100% | `diff` against baseline |
| Regression tests | 0 failures | `pytest` suite |
