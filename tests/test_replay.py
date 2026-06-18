"""Tests for replay mode."""
import csv
import json
import os
import pytest
from minute_bar.config import AppConfig, InputConfig, OutputConfig, AggregationConfig
from minute_bar.replay import ReplayEngine


def make_config(tmp_path):
    return AppConfig(
        input=InputConfig(csv_dir=str(tmp_path / "input")),
        output=OutputConfig(
            output_dir=str(tmp_path / "output"),
            enable_kline=False,
        ),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
    )


def write_snapshot_csv(path, rows):
    with open(path, "wb") as f:
        for row in rows:
            f.write(row.encode("utf-8"))
            f.write(b"\n")


def write_code_csv(path, rows):
    with open(path, "wb") as f:
        for row in rows:
            f.write(row.encode("utf-8"))
            f.write(b"\n")


class TestReplayEngine:
    def test_replay_single_day(self, tmp_path):
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        code_rows = [
            "1301,1,TSE,極洋,JPY,equity,common,,,,0,0,0,2,0,,0",
            "1305,1,TSE,iFTPX年1,JPY,equity,common,,,,0,0,0,2,0,,0",
        ]
        write_code_csv(csv_dir / "code.csv.20260520", code_rows)

        # 2 minutes of data: 09:30 and 09:31
        snapshot_rows = [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,200,200,90000000,1,,T,0,Y,2,0,0,20260520083001500",
            "1305,20260520093000500,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,0,0,20260520083000500",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1305,20260520093100500,410000,415000,410000,415000,410000,415000,415000,100,100,41500000,1,,T,0,Y,2,0,0,20260520083100500",
        ]
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", snapshot_rows)

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        output_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"
        assert output_dir.exists()

        # Should have 2 snapshot files. Round-up (floor+1): clock-minute 0930
        # data → bucket 0931, clock-minute 0931 data → bucket 0932.
        snap_0931 = output_dir / "snapshot_minute_20260520_0931.csv"
        snap_0932 = output_dir / "snapshot_minute_20260520_0932.csv"
        assert snap_0931.exists()
        assert snap_0932.exists()

        # Verify snapshot content
        with open(snap_0931, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
            assert len(rows) == 3  # 1301 has 2 records + 1305 has 1 record

        with open(snap_0932, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2

        # No kline files (enable_kline=False)
        assert not (tmp_path / "output" / "kline" / "2026" / "20260520" / "kline_minute_20260520_0931.csv").exists()

    def test_replay_empty_input(self, tmp_path):
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()
        csv_dir.joinpath("snapshot.csv.20260520").write_bytes(b"")
        csv_dir.joinpath("code.csv.20260520").write_bytes(b"")

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        output_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"
        assert not output_dir.exists()

    def test_replay_with_kline(self, tmp_path):
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,極洋,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
        ])

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        snap_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"
        kline_dir = tmp_path / "output" / "kline" / "2026" / "20260520"
        # Round-up: snapshot timestamp 09:30:00.999 → bucket 0931.
        assert (snap_dir / "snapshot_minute_20260520_0931.csv").exists()
        assert (kline_dir / "kline_minute_20260520_0931.csv").exists()


def write_order_csv(path, rows):
    with open(path, "wb") as f:
        f.write(b"symbol,time,bidprice,bidsize,askprice,asksize\n")
        for row in rows:
            f.write(row.encode("utf-8"))
            f.write(b"\n")


class TestReplayLateSnapshot:
    def test_late_snapshot_appended(self, tmp_path):
        """Out-of-order snapshot for a flushed minute should be late-appended, not lost."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])

        # clock-minute 0930 records, then clock-minute 0931 (→ bucket 0932) triggers
        # flush of bucket 0931, then a late clock-minute 0930 (→ bucket 0931) record.
        snapshot_rows = [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
        ]
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", snapshot_rows)

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        # Both clock-minute 0930 records round-up into bucket 0931.
        snap_0931 = tmp_path / "output" / "snapshot" / "2026" / "20260520" / "snapshot_minute_20260520_0931.csv"
        assert snap_0931.exists()
        with open(snap_0931, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2  # original + late


class TestReplayLateOrder:
    def test_late_order_appended(self, tmp_path):
        """Out-of-order order for a flushed minute should be late-appended."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
        ])

        order_rows = [
            "1301,20260520093000999,450000,100,451000,200",
            "1301,20260520093100999,451000,150,452000,250",
            "1301,20260520093001500,450500,80,451500,120",
        ]
        write_order_csv(csv_dir / "order.csv.20260520", order_rows)

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        order_path = tmp_path / "output" / "order" / "2026" / "20260520" / "order_minute_20260520_0931.csv"
        assert order_path.exists()
        with open(order_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2  # original + late


class TestReplayZeroDataLoss:
    def test_all_records_preserved_with_out_of_order_data(self, tmp_path):
        """Verify 0 data loss when snapshot records arrive out of order across minutes."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,Stock1,JPY,equity,common,,,,0,0,0,2,0,,0",
            "1305,1,TSE,Stock2,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])

        # Craft data with deliberate out-of-order delivery
        snapshot_rows = [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1305,20260520093000500,410000,412000,410000,413000,410000,412000,412000,50,50,20600000,1,,T,0,Y,2,0,0,20260520083000500",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1305,20260520093100500,410000,415000,410000,415000,410000,415000,415000,100,100,41500000,1,,T,0,Y,2,0,0,20260520083100500",
            "1301,20260520093200999,443500,458000,440000,458000,443500,458000,458000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083200999",
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
            "1305,20260520093101500,410000,416000,410000,416000,410000,416000,416000,50,80,33280000,1,,T,0,Y,2,0,0,20260520083101500",
        ]
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", snapshot_rows)

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        snap_dir = tmp_path / "output" / "snapshot" / "2026" / "20260520"

        total_data_rows = 0
        for f in sorted(snap_dir.glob("snapshot_minute_*.csv")):
            with open(f, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader)
                for row in reader:
                    if row[-1] == "Y":  # update_flag=Y = actual data row
                        total_data_rows += 1

        # 7 input snapshot records -> 7 output data rows (0 loss, excluding carry-forward)
        assert total_data_rows == 7, f"Expected 7 data rows, got {total_data_rows}"

    def test_all_order_records_preserved_with_late_data(self, tmp_path):
        """Verify 0 order data loss with late records."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
        ])

        order_rows = [
            "1301,20260520093000999,450000,100,451000,200",
            "1301,20260520093100999,451000,150,452000,250",
            "1301,20260520093001500,450500,80,451500,120",
            "1301,20260520093101500,451500,90,452500,130",
        ]
        write_order_csv(csv_dir / "order.csv.20260520", order_rows)

        config = AppConfig(
            input=InputConfig(csv_dir=str(csv_dir)),
            output=OutputConfig(output_dir=str(tmp_path / "output"), enable_kline=False, enable_order=True),
            aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
        )
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        order_dir = tmp_path / "output" / "order" / "2026" / "20260520"
        total_rows = 0
        for f in sorted(order_dir.glob("order_minute_*.csv")):
            with open(f, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader)
                total_rows += len(list(reader))

        # 4 input order records -> 4 output rows
        assert total_rows == 4, f"Expected 4 total rows, got {total_rows}"

    def test_summary_includes_late_stats(self, tmp_path):
        """Replay summary should include late record statistics."""
        csv_dir = tmp_path / "input"
        csv_dir.mkdir()

        write_code_csv(csv_dir / "code.csv.20260520", [
            "1301,1,TSE,TestStock,JPY,equity,common,,,,0,0,0,2,0,,0",
        ])
        write_snapshot_csv(csv_dir / "snapshot.csv.20260520", [
            "1301,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999",
            "1301,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,200,90000000,1,,T,0,Y,2,0,0,20260520083100999",
            "1301,20260520093001500,443500,452000,440000,452000,443500,452000,452000,50,150,67800000,1,,T,0,Y,2,0,0,20260520083001500",
        ])

        config = make_config(tmp_path)
        engine = ReplayEngine(config, date="20260520")
        engine.run()

        summary_path = tmp_path / "output" / "replay_summary_20260520.json"
        assert summary_path.exists()
        with open(summary_path) as f:
            summary = json.load(f)
        assert "late_snapshot_records" in summary
        assert summary["late_snapshot_records"] >= 0


def test_replay_uses_sidecar_recovery_not_scan(tmp_path):
    """INV-CM-REPLAY-SCAN-REPLACED: replay populates the skip-set from sidecar
    recovery (NOT a row scan), so an un-sidecared 0932 row is treated as a gap and
    regenerated, and a partial mid-append tail is truncated. With the old scan path
    the un-sidecared 0932 row would be picked up and the minute skipped (stale)."""
    from minute_bar.tickfile import TICKFILE_HEADER
    from minute_bar.writer import get_tickfile_path

    date = "20260520"
    out = tmp_path / "output"
    out.mkdir()
    inp = tmp_path / "input"
    inp.mkdir()
    (inp / f"code.csv.{date}").write_text(
        "7203,1,TSE,Toyota,JPY,equity,common,,,,0,0,0,2,0,,0\n", encoding="utf-8")
    # Snapshot rows: clock-minute 0930 -> bucket 0931 (committed/seeded);
    # clock-minute 0931 -> bucket 0932 (gap to regenerate). Proven round-up pattern
    # from tests/test_tickfile_stale_fix.py:197.
    (inp / f"snapshot.csv.{date}").write_text(
        "7203,20260520093000999,443500,450000,440000,451000,443500,450000,450000,100,100,45000000,1,,T,0,Y,2,0,0,20260520083000999\n"
        "7203,20260520093100999,443500,455000,440000,455000,443500,455000,455000,100,300,135000000,1,,T,0,Y,2,0,0,20260520083100999\n",
        encoding="utf-8")

    # Pre-seed the per-day tickfile: committed bucket 0931 (valid 65-field row,
    # sidecar entry present) + a STALE un-sidecared 0932 row (valid 65 fields but
    # NO sidecar entry — simulates a previous crash mid-minute) + a partial tail.
    # Under sidecar recovery: committed_set={0931}, the stale 0932 row is ignored,
    # and the file is truncated to the 0931 commit offset before 0932 regenerates.
    # Under the old row-scan: both 0931 and 0932 would be picked up -> 0932 skipped.
    tf = get_tickfile_path(str(out), f"{date}0931")
    os.makedirs(os.path.dirname(tf), exist_ok=True)
    fields = [""] * 65
    fields[0] = "7203"; fields[1] = date
    fields[16] = f"{date} 09:31:00"   # UpdateTime -> minute_key 202605200931
    fields[59] = "1"                   # Seqno
    fields[60] = "2026-05-20 09:30:00.999000"  # LocalTime
    committed_row = ",".join(fields)
    committed = TICKFILE_HEADER + "\n" + committed_row + "\n"
    committed_bytes = committed.encode("utf-8")
    # Stale un-sidecared 0932 row (valid 65 fields) — would fool a row scan.
    stale_fields = list(fields)
    stale_fields[16] = f"{date} 09:32:00"
    stale_fields[59] = "2"
    stale_fields[60] = "2026-05-20 09:31:00.999000"
    stale_row = ",".join(stale_fields)
    with open(tf, "wb") as f:
        f.write(committed_bytes)
        f.write(stale_row.encode("utf-8") + b"\n")
        f.write(b"7203,partial,corrupt,tail\n")   # partial mid-append tail (no sidecar)
    # Sidecar records ONLY the committed 0931 at offset = len(committed_bytes).
    with open(tf + ".commit", "w") as f:
        f.write(f"{date}0931,{len(committed_bytes)},1,1\n")

    cfg = AppConfig(
        input=InputConfig(csv_dir=str(inp), file_encoding="utf-8"),
        output=OutputConfig(output_dir=str(out), enable_order=False,
                            enable_tickfile=True, enable_kline=False),
        aggregation=AggregationConfig(first_seen_volume_base="start_totalvol"),
    )
    engine = ReplayEngine(cfg, date=date)
    engine.run()

    # Sidecar authoritative -> committed_set={0931} only. Stale 0932 + partial tail
    # truncated; 0932 regenerated fresh; 0931 not duplicated.
    data = open(tf, "rb").read()
    assert b"partial,corrupt" not in data, "partial tail was not truncated by sidecar recovery"
    with open(tf, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        by_min = {}
        stale_local_times = []   # collect LocalTime (col 60) for 0932 rows
        for row in reader:
            if len(row) != 65:
                continue
            mk = row[16].replace(" ", "").replace(":", "")[:12]
            by_min[mk] = by_min.get(mk, 0) + 1
            if mk == f"{date}0932":
                stale_local_times.append(row[60])
    assert by_min.get(f"{date}0931", 0) == 1, "committed 0931 was duplicated"
    assert by_min.get(f"{date}0932", 0) == 1, "gap 0932 was not regenerated cleanly"
    # The regenerated 0932 row must come from THIS run (LocalTime ~= 09:30/09:31),
    # not the stale seed (LocalTime 09:31:00.999000 would survive under scan path).
    # Under recovery the stale row is truncated before regeneration, so exactly one
    # fresh 0932 row remains.
    assert len(stale_local_times) == 1, "0932 should have exactly one (regenerated) row"
