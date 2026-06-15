from __future__ import annotations

import argparse
import os
import sys

from data_simulator.simulator import Simulator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="FIU data simulator — replay historical CSV files to test minute_bar live mode",
    )
    p.add_argument("--source-dir", default="input", help="Directory containing historical CSV files (default: input)")
    p.add_argument("--output-dir", default="test/output", help="Directory to write simulated output (default: test/output)")
    p.add_argument("--speed", type=int, default=100, help="Replay speed multiplier (default: 100)")
    p.add_argument("--order-speed", type=int, default=None, help="Order-specific speed multiplier (default: same as --speed)")
    p.add_argument("--snapshot-speed", type=int, default=None, help="Snapshot-specific speed multiplier (default: same as --speed)")
    p.add_argument("--date", default=None, help="Date to replay in YYYYMMDD format (default: auto-detect from source files)")
    p.add_argument("--file-types", default="order,snapshot,code", help="Comma-separated file types to replay (default: order,snapshot,code)")
    p.add_argument("--order-mode", choices=["original", "time"], default="original", help="Row order: original=source order, time=sort by timestamp (default: original)")
    p.add_argument("--code-mode", choices=["preload", "stream"], default="preload", help="Code file mode: preload=write all at start, stream=append line by line (default: preload)")
    p.add_argument("--split-line-prob", type=float, default=0.0, help="Probability of splitting a line into two writes (default: 0.0)")
    p.add_argument("--split-line-delay-ms", type=int, default=50, help="Delay in ms between split-line halves (default: 50)")
    p.add_argument("--late-prob", type=float, default=0.0, help="Probability of delaying a record as late (default: 0.0)")
    p.add_argument("--late-delay-sec", type=float, default=10.0, help="Real-time seconds to delay late records (default: 10.0, not affected by speed)")
    p.add_argument("--batch-size", type=int, default=1000, help="Number of lines before batch flush (default: 1000)")
    p.add_argument("--flush-interval-ms", type=int, default=100, help="Max ms between flushes (default: 100)")
    p.add_argument("--fsync", action="store_true", default=False, help="Call os.fsync after each flush (slow)")
    p.add_argument("--clean", action="store_true", default=True, help="Clean target CSV files before starting (default: True)")
    p.add_argument("--no-clean", dest="clean", action="store_false", help="Do not clean target files before starting")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    source_dir = os.path.abspath(args.source_dir)
    output_dir = os.path.abspath(args.output_dir)
    file_types = [t.strip() for t in args.file_types.split(",")]

    sim = Simulator(
        source_dir=source_dir,
        output_dir=output_dir,
        speed=args.speed,
        date=args.date,
        file_types=file_types,
        order_mode=args.order_mode,
        code_mode=args.code_mode,
        split_line_prob=args.split_line_prob,
        split_line_delay_ms=args.split_line_delay_ms,
        late_prob=args.late_prob,
        late_delay_sec=args.late_delay_sec,
        batch_size=args.batch_size,
        flush_interval_ms=args.flush_interval_ms,
        fsync=args.fsync,
        clean=args.clean,
        speed_map={
            k: v for k, v in [("order", args.order_speed), ("snapshot", args.snapshot_speed)]
            if v is not None
        },
    )

    try:
        sim.run()
    except KeyboardInterrupt:
        print("\nStopping simulator...")
        sim.stop()


if __name__ == "__main__":
    main()
