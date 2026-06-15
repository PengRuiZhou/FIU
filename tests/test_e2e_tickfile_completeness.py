"""E2E automated verification: tickfile completeness + order completeness.

Run after E2E live test:
  E2E_OUTPUT_DIR=test/tickfile_live_output E2E_DATE=20260528 E2E_SOURCE_ORDER_COUNT=87521294 \
    python -m pytest tests/test_e2e_tickfile_completeness.py -v
"""
import csv
import os
import pytest
from pathlib import Path


@pytest.mark.e2e
def test_tickfile_minutes_match_order_and_snapshot():
    """P0 verification: tickfile minutes == minutes with order + snapshot.

    Only compare minutes with BOTH order AND snapshot data (excludes pre/post-market).
    """
    output_dir = Path(os.environ.get("E2E_OUTPUT_DIR", "test/tickfile_live_output"))
    date_str = os.environ.get("E2E_DATE", "20260528")
    year = date_str[:4]

    snap_dir = output_dir / "snapshot" / year / date_str
    tf_path = output_dir / "tickfile" / year / date_str / f"tickfile_{date_str}.csv"
    order_dir = output_dir / "order" / year / date_str

    assert snap_dir.exists(), f"Snapshot directory not found: {snap_dir}"
    assert tf_path.exists(), f"Tickfile not found: {tf_path}"

    snap_mins = {f.stem.split('_')[-1] for f in snap_dir.glob("*.csv")}

    order_mins = set()
    if order_dir.exists():
        order_mins = {f.stem.split('_')[-1] for f in order_dir.glob("*.csv")}
    comparison_mins = snap_mins & order_mins  # minutes with BOTH order and snapshot

    tf_mins = set()
    with open(tf_path, encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        time_idx = header.index("UpdateTime")
        for row in reader:
            if len(row) > time_idx:
                tf_mins.add(row[time_idx].split(' ')[1].replace(':', '')[:4])

    missing = sorted(comparison_mins - tf_mins)
    assert not missing, (
        f"Missing {len(missing)} tickfile minutes "
        f"(snapshot={len(snap_mins)}, order={len(order_mins)}, "
        f"comparison={len(comparison_mins)}, tickfile={len(tf_mins)}): {missing}"
    )


@pytest.mark.e2e
def test_order_output_count_matches_source():
    """P1 verification: order output record count == source record count."""
    output_dir = Path(os.environ.get("E2E_OUTPUT_DIR", "test/tickfile_live_output"))
    source_count = int(os.environ.get("E2E_SOURCE_ORDER_COUNT", "87521294"))

    total_lines = 0
    for f in sorted((output_dir / "order").rglob("*.csv")):
        with open(f, encoding='utf-8') as fh:
            first_line = True
            for line in fh:
                if first_line:
                    first_line = False
                else:
                    total_lines += 1

    delta = abs(total_lines - source_count)
    assert delta == 0, f"Order count mismatch: output={total_lines}, source={source_count}, delta={delta}"
