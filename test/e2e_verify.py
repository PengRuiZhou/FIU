#!/usr/bin/env python3
"""E2E Verification Script — Order, Snapshot, Tickfile completeness check."""
from __future__ import annotations
import csv
import io
import os
import sys
from collections import defaultdict

# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE = "D:/FIU"
OUTPUT_DIR = f"{BASE}/test/tickfile_live_output"
SOURCE_DIR = f"{BASE}/input"
DATE = "20260528"

# ── 1. Source Data Analysis ──────────────────────────────────────────
print("=" * 70)
print("1. SOURCE DATA ANALYSIS")
print("=" * 70)

# Parse source snapshot to get per-minute record counts
snap_minutes = defaultdict(int)
snap_header = None
with open(f"{SOURCE_DIR}/snapshot.csv.{DATE}", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    snap_header = next(reader)
    time_col = snap_header.index("time")
    for row in reader:
        ts = row[time_col]
        minute_key = ts[:11]  # YYYYMMDDHHM → need YYYYMMDDHHMM
        minute_key = ts[:12]  # YYYYMMDDHHMM
        snap_minutes[minute_key] += 1

print(f"Snapshot source: {sum(snap_minutes.values())} records across {len(snap_minutes)} minutes")
print(f"Snapshot time range: {min(snap_minutes.keys())} - {max(snap_minutes.keys())}")

# Parse source order to get per-minute record counts (sample approach - count headers only)
order_minutes = defaultdict(int)
order_header = None
with open(f"{SOURCE_DIR}/order.csv.{DATE}", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    order_header = next(reader)
    time_col = order_header.index("time")
    for row in reader:
        ts = row[time_col]
        minute_key = ts[:12]
        order_minutes[minute_key] += 1

print(f"Order source: {sum(order_minutes.values())} records across {len(order_minutes)} minutes")
print(f"Order time range: {min(order_minutes.keys())} - {max(order_minutes.keys())}")

# ── 2. Output Minute Files Analysis ──────────────────────────────────
print("\n" + "=" * 70)
print("2. OUTPUT MINUTE FILES ANALYSIS")
print("=" * 70)

def get_output_minutes(data_type):
    """Get set of minute HHMM values from output files."""
    path = f"{OUTPUT_DIR}/{data_type}/2026/{DATE}"
    if not os.path.exists(path):
        return set()
    minutes = set()
    for fn in os.listdir(path):
        if fn.startswith(f"{data_type}_minute_{DATE}_") and fn.endswith(".csv"):
            hhmm = fn.replace(f"{data_type}_minute_{DATE}_", "").replace(".csv", "")
            minutes.add(hhmm)
    return minutes

order_output = get_output_minutes("order")
snapshot_output = get_output_minutes("snapshot")

print(f"Order output: {len(order_output)} minute files")
print(f"  Range: {min(order_output)} - {max(order_output)}")

print(f"Snapshot output: {len(snapshot_output)} minute files")
print(f"  Range: {min(snapshot_output)} - {max(snapshot_output)}")

# ── 3. Source vs Output Minute Coverage ──────────────────────────────
print("\n" + "=" * 70)
print("3. SOURCE vs OUTPUT MINUTE COVERAGE")
print("=" * 70)

# Extract HHMM from source minutes (format: YYYYMMDDHHMM)
def to_hhmm(minute_key):
    return minute_key[8:12]

source_snap_hhmm = set(to_hhmm(k) for k in snap_minutes.keys())
source_order_hhmm = set(to_hhmm(k) for k in order_minutes.keys())

# Source minutes not in output
missing_snap = sorted(source_snap_hhmm - snapshot_output)
missing_order = sorted(source_order_hhmm - order_output)

# Output minutes not in source (late records or carry-forward)
extra_snap = sorted(snapshot_output - source_snap_hhmm)
extra_order = sorted(order_output - source_order_hhmm)

print(f"\nSnapshot: {len(source_snap_hhmm)} source minutes, {len(snapshot_output)} output files")
if missing_snap:
    print(f"  ⚠ MISSING from output ({len(missing_snap)}): {missing_snap[:20]}{'...' if len(missing_snap) > 20 else ''}")
else:
    print(f"  ✅ All source minutes have output files")
if extra_snap:
    print(f"  Extra in output (carry-forward/late) ({len(extra_snap)}): {extra_snap[:20]}")

print(f"\nOrder: {len(source_order_hhmm)} source minutes, {len(order_output)} output files")
if missing_order:
    print(f"  ⚠ MISSING from output ({len(missing_order)}): {missing_order[:20]}{'...' if len(missing_order) > 20 else ''}")
else:
    print(f"  ✅ All source minutes have output files")
if extra_order:
    print(f"  Extra in output (carry-forward/late) ({len(extra_order)}): {extra_order[:20]}")

# ── 4. Record Count Verification ────────────────────────────────────
print("\n" + "=" * 70)
print("4. RECORD COUNT VERIFICATION (source vs output per minute)")
print("=" * 70)

# Count output records per minute for snapshots
def count_output_records(data_type):
    path = f"{OUTPUT_DIR}/{data_type}/2026/{DATE}"
    counts = {}
    if not os.path.exists(path):
        return counts
    for fn in sorted(os.listdir(path)):
        if not fn.endswith(".csv"):
            continue
        fp = os.path.join(path, fn)
        with open(fp, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f) - 1  # subtract header
            hhmm = fn.replace(f"{data_type}_minute_{DATE}_", "").replace(".csv", "")
            counts[hhmm] = line_count
    return counts

snap_out_counts = count_output_records("snapshot")
order_out_counts = count_output_records("order")

# Compare snapshot counts
print("\nSnapshot per-minute comparison (sample of mismatches):")
snap_mismatches = 0
for hhmm in sorted(source_snap_hhmm & snapshot_output):
    source_date_key = f"{DATE}{hhmm}"
    src_count = snap_minutes.get(source_date_key, 0)
    out_count = snap_out_counts.get(hhmm, 0)
    if src_count != out_count:
        snap_mismatches += 1
        if snap_mismatches <= 10:
            print(f"  {hhmm}: source={src_count}, output={out_count}, diff={out_count-src_count}")
if snap_mismatches > 10:
    print(f"  ... and {snap_mismatches - 10} more mismatches")
if snap_mismatches == 0:
    print("  ✅ All snapshot minutes match source record counts")

# Compare order counts
print("\nOrder per-minute comparison (sample of mismatches):")
order_mismatches = 0
for hhmm in sorted(source_order_hhmm & order_output):
    source_date_key = f"{DATE}{hhmm}"
    src_count = order_minutes.get(source_date_key, 0)
    out_count = order_out_counts.get(hhmm, 0)
    if src_count != out_count:
        order_mismatches += 1
        if order_mismatches <= 10:
            print(f"  {hhmm}: source={src_count}, output={out_count}, diff={out_count-src_count}")
if order_mismatches > 10:
    print(f"  ... and {order_mismatches - 10} more mismatches")
if order_mismatches == 0:
    print("  ✅ All order minutes match source record counts")

# Totals
print(f"\nSnapshot total records: source={sum(snap_minutes.values())}, output={sum(snap_out_counts.values())}")
print(f"Order total records: source={sum(order_minutes.values())}, output={sum(order_out_counts.values())}")

# ── 5. Tickfile Verification ────────────────────────────────────────
print("\n" + "=" * 70)
print("5. TICKFILE VERIFICATION")
print("=" * 70)

tickfile_path = f"{OUTPUT_DIR}/tickfile/2026/{DATE}/tickfile_{DATE}.csv"
if os.path.exists(tickfile_path):
    with open(tickfile_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        print(f"Tickfile columns: {len(header)}")
        print(f"Header: {','.join(header[:10])}...")

        # Check seqno monotonicity
        seqno_col = header.index("seqno")
        symbol_col = header.index("symbol")
        minute_col = header.index("minute") if "minute" in header else None

        prev_seqno = -1
        total_rows = 0
        symbol_set = set()
        minute_set = set()
        seqno_errors = 0

        for row in reader:
            total_rows += 1
            seqno = int(row[seqno_col])
            symbol_set.add(row[symbol_col])
            if minute_col is not None:
                minute_set.add(row[minute_col])
            if seqno <= prev_seqno:
                seqno_errors += 1
                if seqno_errors <= 5:
                    print(f"  ⚠ Seqno non-monotonic: row {total_rows}, seqno {seqno} <= prev {prev_seqno}")
            prev_seqno = seqno

    print(f"\nTickfile total rows: {total_rows}")
    print(f"Tickfile unique symbols: {len(symbol_set)}")
    print(f"Tickfile max seqno: {prev_seqno}")
    print(f"Tickfile seqno errors: {seqno_errors}")
    if seqno_errors == 0:
        print("  ✅ Seqno is monotonically increasing")

    # Check which minutes are covered
    if minute_set:
        print(f"Tickfile minute coverage: {len(minute_set)} minutes")
        tf_missing_from_snap = sorted(snapshot_output - minute_set)
        tf_missing_from_order = sorted(order_output - minute_set)
        if tf_missing_from_snap:
            print(f"  Minutes in snapshot but NOT in tickfile ({len(tf_missing_from_snap)}): {tf_missing_from_snap[:10]}")
        if tf_missing_from_order:
            print(f"  Minutes in order but NOT in tickfile ({len(tf_missing_from_order)}): {tf_missing_from_order[:10]}")
        # Minutes in tickfile but not in either
        tf_extra = sorted(minute_set - snapshot_output - order_output)
        if tf_extra:
            print(f"  Minutes in tickfile but not in snap/order ({len(tf_extra)}): {tf_extra[:10]}")
        # Total overlap check
        all_output = snapshot_output | order_output
        if minute_set == all_output:
            print("  ✅ Tickfile covers exactly the same minutes as snapshot + order combined")
else:
    print(f"  ⚠ Tickfile not found at {tickfile_path}")

# ── 6. Summary ──────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("6. SUMMARY")
print("=" * 70)
print(f"Source data: {sum(snap_minutes.values())} snapshots, {sum(order_minutes.values())} orders")
print(f"Output: {len(snapshot_output)} snapshot files, {len(order_output)} order files")
print(f"Tickfile: {'OK' if os.path.exists(tickfile_path) else 'MISSING'}")
issues = []
if missing_snap:
    issues.append(f"Snapshot missing {len(missing_snap)} minutes: {missing_snap[:5]}")
if missing_order:
    issues.append(f"Order missing {len(missing_order)} minutes: {missing_order[:5]}")
if snap_mismatches:
    issues.append(f"Snapshot record count mismatches: {snap_mismatches}")
if order_mismatches:
    issues.append(f"Order record count mismatches: {order_mismatches}")
if seqno_errors:
    issues.append(f"Tickfile seqno errors: {seqno_errors}")
if issues:
    print("\n⚠ ISSUES FOUND:")
    for i in issues:
        print(f"  - {i}")
else:
    print("\n✅ ALL CHECKS PASSED")
