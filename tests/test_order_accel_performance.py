"""Phase 4: Performance benchmarks for Rust order acceleration."""

import pytest
import threading
import time

from minute_bar.csv_parser import use_rust_accel


@pytest.mark.slow
@pytest.mark.skipif(not use_rust_accel(), reason="Rust extension not available")
def test_concurrent_parse_sustained():
    """Simulate sustained concurrent order + snapshot parsing for 10 seconds.

    Verifies that PyO3 GIL release allows real concurrency — the order thread
    should achieve >100K records/s sustained even while a snapshot thread
    is also parsing concurrently.
    """
    from minute_bar._order_accel import parse_order_batch

    # Generate 100K lines (representing ~1 second of peak data)
    lines = []
    for i in range(100_000):
        sym = ['7203', '6501', '9984', '6758', '8306', '7974'][i % 6]
        time_val = 20260528090000000 + (i * 100) % 1000000
        lines.append(f'{sym},{time_val},{4500000+i},{100+i},{4600000+i},{200+i},2,{time_val-23}'.encode())

    # Simulate sustained parsing: both threads parse concurrently for 10 seconds
    results = {'order': 0, 'snapshot': 0}
    stop_event = threading.Event()

    def order_thread():
        count = 0
        while not stop_event.is_set():
            batch, _ = parse_order_batch(lines, 'utf-8')
            count += len(batch)
        results['order'] = count

    def snapshot_thread():
        # Snapshot has ~8x fewer records — use a smaller batch
        snap_lines = lines[:1000]
        count = 0
        while not stop_event.is_set():
            batch, _ = parse_order_batch(snap_lines, 'utf-8')
            count += len(batch)
        results['snapshot'] = count

    t1 = threading.Thread(target=order_thread)
    t2 = threading.Thread(target=snapshot_thread)
    t1.start()
    t2.start()

    time.sleep(10)
    stop_event.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    # Order thread should achieve >100K records/s sustained
    order_throughput = results['order'] / 10.0
    snapshot_throughput = results['snapshot'] / 10.0
    print(f"\nOrder throughput:   {order_throughput:>12,.0f} rec/s")
    print(f"Snapshot throughput: {snapshot_throughput:>10,.0f} rec/s")

    assert order_throughput > 100_000, (
        f"Order throughput too low: {order_throughput:,.0f} rec/s (need >100,000)"
    )
