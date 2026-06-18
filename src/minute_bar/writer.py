from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import threading
from typing import Dict, List, Optional, Tuple

try:
    import fcntl as _fcntl  # POSIX only (production = Linux per M-R25-3)
    _HAS_FCNTL = True
except ImportError:
    _fcntl = None
    _HAS_FCNTL = False

from minute_bar.code_table import CodeTable
from minute_bar.clock import extract_date_from_minute_key
from minute_bar.models import OHLCVAggregate, OrderRecord, SnapshotRecord
from minute_bar.tickfile import TICKFILE_HEADER, build_tickfile_row

logger = logging.getLogger(__name__)

# Tickfile IO constants (Phase 18, spec N41)
TICKFILE_MAX_ROW_BYTES = 640   # v11: Increased from 512. Pathological float repr() can reach ~562 bytes.
                                # Measured typical max ~423 bytes. 640 provides >13% margin.
TICKFILE_TAIL_READ_SIZE = 4096  # >6x TICKFILE_MAX_ROW_BYTES, covers truncated lines safely
assert TICKFILE_TAIL_READ_SIZE >= TICKFILE_MAX_ROW_BYTES * 6, (
    "TAIL_READ_SIZE must be >= 6x MAX_ROW_BYTES for seek safety")

# Commit-marker sidecar constants (Phase 23, commit-marker recovery)
SIDECAR_TAIL_READ_SIZE = 65536   # full-day sidecar ~16KB; cap a tail read at 64KB (INV-CM-SIDECAR-MAXSIZE)
MAX_SIDECAR_SIZE = 1 * 1024 * 1024  # 1MB sanity cap (normal ~16KB)


def _fmt(value: float, decimal: int) -> str:
    return f"{value:.{decimal}f}"


# File-level write locks to serialize atomic_write and append operations
_write_locks: Dict[str, threading.RLock] = {}  # RLock prevents crash-induced deadlock (N27)
_write_lock_mutex = threading.Lock()


def _get_write_lock(path: str) -> threading.RLock:
    with _write_lock_mutex:
        if path not in _write_locks:
            _write_locks[path] = threading.RLock()
        return _write_locks[path]


def _prune_write_locks(current_date: str) -> None:
    """Remove _write_locks entries for dates other than current_date.
    Called at cross-day after writer is paused and before resume.
    Prevents unbounded growth of module-level dict in long-running processes.

    Only prunes TICKFILE paths (uses pathlib path component check, N39).

    PRECONDITION: All writer threads must be paused before calling this function.
    Deleting a lock that is currently held by another thread would allow a new lock
    to be created for the same path, violating the sole-writer invariant.
    """
    import pathlib
    with _write_lock_mutex:
        stale_keys = [k for k in _write_locks
                      if any(p == "tickfile" for p in pathlib.PurePath(k).parts)
                      and current_date not in k]
        for k in stale_keys:
            del _write_locks[k]
        if stale_keys:
            logger.debug("Pruned %d stale tickfile _write_locks entries", len(stale_keys))


def get_snapshot_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "snapshot", date_str[:4], date_str,
                        f"snapshot_minute_{date_str}_{hhmm}.csv")


def get_order_file_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    return os.path.join(output_dir, "order", date_str[:4], date_str,
                        f"order_minute_{date_str}_{hhmm}.csv")


def atomic_write(path: str, content: str) -> None:
    with _get_write_lock(path):
        tmp_path = path + ".tmp"
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)


def _format_order_row(rec: OrderRecord) -> str:
    return (
        f"{rec.seqno},{rec.symbol},{rec.time},"
        f"{rec.bidprice},{rec.bidsize},{rec.askprice},{rec.asksize},"
        f"{rec.decimal},{rec.rcvtime}"
    )


def append_order_records(path: str, records: List[OrderRecord]) -> None:
    """Append order rows to existing file without writing header."""
    try:
        with _get_write_lock(path):
            with open(path, "a", encoding="utf-8", newline="") as f:
                for rec in records:
                    f.write(_format_order_row(rec) + "\n")
    except IOError as e:
        logger.fatal("Late append failed for %s: %s", path, e)
        raise


def append_snapshot_records(path: str, records: List[SnapshotRecord], code_table: CodeTable) -> None:
    """Append snapshot rows to existing file without writing header. update_flag=Y."""
    try:
        with _get_write_lock(path):
            with open(path, "a", encoding="utf-8", newline="") as f:
                for rec in records:
                    name = code_table.get_name(rec.symbol)
                    f.write(_format_snapshot_row(rec, name, "Y") + "\n")
    except IOError as e:
        logger.fatal("Late append failed for %s: %s", path, e)
        raise


def compute_trade_flag(agg: OHLCVAggregate) -> str:
    return "Y" if (agg.volume > 0 or agg.any_lasttradeqty_positive) else "N"


def compute_carry_trade_flag() -> str:
    return "N"


def _format_snapshot_row(rec: SnapshotRecord, name: str, update_flag: str) -> str:
    return (
        f"{rec.seqno},{rec.symbol},{name},{rec.time},"
        f"{rec.preclose},{rec.lastprice},"
        f"{rec.open},{rec.high},{rec.low},{rec.close},"
        f"{rec.lasttradeprice},{rec.lasttradeqty},"
        f"{rec.totalvol},{rec.totalamount},"
        f"{rec.sessionid},{rec.tradetype},{rec.status},{rec.direction},{rec.pflag},"
        f"{rec.decimal},{rec.vwap},{rec.shortsellflag},{rec.rcvtime},{update_flag}"
    )


SNAPSHOT_HEADER = ("seqno,symbol,name,time,preclose,lastprice,open,high,low,close,"
                   "lasttradeprice,lasttradeqty,totalvol,totalamount,"
                   "sessionid,tradetype,status,direction,pflag,decimal,vwap,shortsellflag,rcvtime,update_flag")


def _minute_end_threshold(minute_key: str) -> int:
    """Return next minute start as 17-digit time integer for carry-forward filtering.

    NOTE (round-up): bar M covers [(M-1):00, M:00); its true right boundary is M:00.
    This returns M+1:00, which is 1 minute loose. That is harmless: the carry-forward
    "N" branch only fires for symbols absent from ohlcv_data[M], whose records all have
    time < (M-1):00 (well below either threshold). Kept loose to avoid a behavior change
    in the round-up migration; could be tightened to M:00 in a follow-up.
    """
    yyyymmdd = int(minute_key[:8])
    hh = int(minute_key[8:10])
    mm = int(minute_key[10:12])
    mm += 1
    if mm >= 60:
        mm -= 60
        hh += 1
        if hh >= 24:
            hh = 0
            yyyymmdd += 1
    return int(f"{yyyymmdd:08d}{hh:02d}{mm:02d}00000")


def write_snapshot_file(
    output_dir: str,
    minute_key: str,
    snapshot_copy: Dict[str, SnapshotRecord],
    ohlcv_data: Dict[str, OHLCVAggregate],
    code_table: CodeTable,
    full: bool = True,
    raw_records: Dict[str, List[SnapshotRecord]] = None,
) -> None:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    out_dir = os.path.join(output_dir, "snapshot", date_str[:4], date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"snapshot_minute_{date_str}_{hhmm}.csv")

    lines = [SNAPSHOT_HEADER]
    total_rows = 0
    skipped_carry_forward = 0

    symbols_to_output = snapshot_copy if full else {s: snapshot_copy[s] for s in ohlcv_data if s in snapshot_copy}
    minute_end = _minute_end_threshold(minute_key)

    for symbol in sorted(symbols_to_output):
        name = code_table.get_name(symbol)

        if raw_records and symbol in raw_records:
            for rec in raw_records[symbol]:
                lines.append(_format_snapshot_row(rec, name, "Y"))
                total_rows += 1
        elif symbol in ohlcv_data:
            rec = snapshot_copy[symbol]
            lines.append(_format_snapshot_row(rec, name, "Y"))
            total_rows += 1
        else:
            rec = snapshot_copy[symbol]
            if rec.time >= minute_end:
                skipped_carry_forward += 1
                continue
            lines.append(_format_snapshot_row(rec, name, "N"))
            total_rows += 1

    if skipped_carry_forward:
        logger.warning(
            "Skipped %d carry-forward rows with future timestamps in %s (minute_end=%d)",
            skipped_carry_forward, minute_key, minute_end,
        )

    atomic_write(path, "\n".join(lines) + "\n")
    logger.info("Wrote snapshot file: %s (%d rows, %d symbols)", path, total_rows, len(symbols_to_output))


def write_kline_file(
    output_dir: str,
    minute_key: str,
    snapshot_copy: Dict[str, SnapshotRecord],
    ohlcv_data: Dict[str, OHLCVAggregate],
    code_table: CodeTable,
    full: bool = True,
) -> None:
    date_str = minute_key[:8]
    hhmm = minute_key[8:12]
    out_dir = os.path.join(output_dir, "kline", date_str[:4], date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"kline_minute_{date_str}_{hhmm}.csv")

    lines = ["seqno,symbol,name,open,high,low,close,volume,amount,count,trade_flag,update_flag"]

    symbols_to_output = snapshot_copy if full else {s: snapshot_copy[s] for s in ohlcv_data if s in snapshot_copy}
    minute_end = _minute_end_threshold(minute_key)

    for symbol in sorted(symbols_to_output):
        rec = snapshot_copy[symbol]
        name = code_table.get_name(symbol)

        if symbol in ohlcv_data:
            agg = ohlcv_data[symbol]
            update_flag = "Y"
            trade_flag = compute_trade_flag(agg)
            d = agg.decimal
            lines.append(
                f"{agg.seqno},{symbol},{name},"
                f"{_fmt(agg.open, d)},{_fmt(agg.high, d)},{_fmt(agg.low, d)},{_fmt(agg.close, d)},"
                f"{agg.volume},{_fmt(agg.amount, d)},{agg.count},{trade_flag},{update_flag}"
            )
        else:
            if rec.time >= minute_end:
                continue
            update_flag = "N"
            trade_flag = compute_carry_trade_flag()
            d = rec.decimal
            lines.append(
                f"{rec.seqno},{symbol},{name},"
                f"{_fmt(rec.lastprice, d)},{_fmt(rec.lastprice, d)},{_fmt(rec.lastprice, d)},{_fmt(rec.lastprice, d)},"
                f"0,{_fmt(0.0, d)},0,{trade_flag},{update_flag}"
            )

    atomic_write(path, "\n".join(lines) + "\n")
    logger.info("Wrote kline file: %s (%d symbols)", path, len(symbols_to_output))


ORDER_HEADER = "seqno,symbol,time,bidprice,bidsize,askprice,asksize,decimal,rcvtime"


def write_order_file(
    output_dir: str,
    minute_key: str,
    order_records: List[OrderRecord],
) -> None:
    if not order_records:
        return

    path = get_order_file_path(output_dir, minute_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"

    with _get_write_lock(path):
        with open(tmp_path, "w", encoding="utf-8", newline="",
                  buffering=1_048_576) as f:
            f.write(ORDER_HEADER)
            f.write("\n")
            for rec in order_records:
                f.write(_format_order_row(rec))
                f.write("\n")
        os.replace(tmp_path, path)

    logger.info("Wrote order file: %s (%d records)", path, len(order_records))


def get_tickfile_path(output_dir: str, minute_key: str) -> str:
    date_str = minute_key[:8]
    return os.path.join(output_dir, "tickfile", date_str[:4], date_str,
                        f"tickfile_{date_str}.csv")


@contextlib.contextmanager
def _nullctx():
    yield None


def _fstat_size_after(path: str) -> int:
    """Read post-write size via fstat on a fresh fd (INV-CM-OFFSET-FSTAT)."""
    with open(path, "rb") as f:
        return os.fstat(f.fileno()).st_size


def write_tickfile_rows(
    output_dir: str,
    minute_key: str,
    selected: list,
    seqno: int,
    code_table_getter=None,
    skip_fsync: bool = False,
    enable_commit_marker: bool = True,
) -> None:
    if not selected:
        return

    path = get_tickfile_path(output_dir, minute_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    sidecar_path = path + ".commit"
    lockfile_path = path + ".lock"

    rows = []
    skipped = 0
    for symbol, snap, order in selected:
        try:
            row = build_tickfile_row(snap, order, seqno, code_table_getter, minute_key)
            rows.append(row)
        except Exception:
            logger.error("Tickfile row build failed for symbol %s seqno=%d", symbol, seqno, exc_info=True)
            skipped += 1

    if not rows:
        logger.warning("Tickfile: skipped %d/%d symbols for minute=%s", skipped, len(selected), minute_key)
        raise IOError(f"All tickfile rows failed to build for minute={minute_key} ({skipped}/{len(selected)})")

    with _get_write_lock(path):  # in-process RLock (INV-CM-LOCK)
        flock_cm = _flock_critical_section(lockfile_path) if enable_commit_marker else _nullctx()
        with flock_cm:  # cross-process flock (INV-CM-SIDECAR-IN-LOCK / FLOCK-WITH-NESTED);
                        # on non-POSIX dev/test flock is a no-op, so concurrency relies on _get_write_lock above
            content = TICKFILE_HEADER + "\n" + "\n".join(rows) + "\n"

            if enable_commit_marker:
                kind, last_rec = _classify_append_precondition(minute_key, sidecar_path, path)
                if kind == "committed":
                    # branch 2a: already committed -> file-level skip, zero bytes (INV-CM-REGEN-NO-SIDECAR-REWRITE).
                    logger.debug("Tickfile REGEN skip (committed): minute=%s", minute_key)
                    return
                if kind == "truncate_rewrite":
                    # branch 1b/2b: truncate path-based BEFORE append fd open (INV-CM-TRUNCATE-BEFORE-OPEN).
                    os.truncate(path, last_rec[1])
                file_existed_before = os.path.exists(path)
            else:
                kind, file_existed_before = "legacy", os.path.exists(path)

            # --- write tickfile rows (atomic-create vs append) ---
            if not file_existed_before:
                tmp_path = path + ".tmp"
                try:
                    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                        f.write(content)
                        f.flush()
                        if not skip_fsync:
                            os.fsync(f.fileno())
                    os.replace(tmp_path, path)
                except Exception:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    raise
                offset = _fstat_size_after(path)
            else:
                with open(path, "rb") as f:
                    first_line = f.readline().decode("utf-8", errors="replace").strip()
                if first_line != TICKFILE_HEADER:
                    file_size = os.path.getsize(path)
                    if file_size == 0:
                        logger.info("Tickfile header rewrite: %s", path)
                        tmp_path = path + ".tmp"
                        try:
                            with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                                f.write(content)
                                f.flush()
                                if not skip_fsync:
                                    os.fsync(f.fileno())
                            os.replace(tmp_path, path)
                        except Exception:
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                            raise
                        offset = _fstat_size_after(path)
                    else:
                        raise IOError(f"Tickfile header corrupted, cannot append: {path}")
                else:
                    need_newline_fix = False
                    file_size = os.path.getsize(path)
                    tail_size = min(file_size, TICKFILE_TAIL_READ_SIZE)
                    if tail_size > 0:
                        with open(path, "rb") as f:
                            f.seek(-tail_size, 2)
                            tail_bytes = f.read()
                        last_line = ""
                        for raw_line in reversed(tail_bytes.split(b'\n')):
                            stripped = raw_line.strip()
                            if stripped:
                                try:
                                    last_line = stripped.decode("utf-8", errors="strict")
                                except UnicodeDecodeError:
                                    need_newline_fix = True
                                break
                        if last_line and len(last_line.split(',')) != 65:
                            need_newline_fix = True
                    if kind == "truncate_rewrite" and need_newline_fix:
                        logger.critical("Tickfile newline-fix after REGEN truncate (offset mismatch) minute=%s path=%s",
                                        minute_key, path)
                        raise IOError(f"REGEN truncate offset mismatch for {minute_key}")
                    with open(path, "a", encoding="utf-8", newline="") as f:
                        if need_newline_fix:
                            f.write("\n")
                        for row in rows:
                            f.write(row + "\n")
                        f.flush()
                        if not skip_fsync:
                            os.fsync(f.fileno())
                        offset = os.fstat(f.fileno()).st_size  # INV-CM-OFFSET-FSTAT (same fd, after fsync)

            # --- sidecar commit (only when enabled + a real write happened) ---
            if enable_commit_marker:
                line = f"{minute_key},{offset},{len(rows)},{seqno}\n"
                with open(sidecar_path, "a", encoding="utf-8", newline="") as sf:
                    sf.write(line)
                    sf.flush()
                    if not skip_fsync:
                        os.fsync(sf.fileno())  # INV-CM-ORDERED-TWO-FILE: tickfile fsync already done above

    logger.info("Tickfile append: %s minute=%s (%d symbols, seqno=%d)", path, minute_key, len(rows), seqno)
    if skipped > 0:
        logger.warning("Tickfile: skipped %d/%d symbols for minute=%s", skipped, len(selected), minute_key)


def _parse_commit_line(line: str) -> Optional[Tuple[str, int, int, int]]:
    """Parse a sidecar commit line `<minute>,<offset>,<rowcount>,<seqno>`.
    Returns (minute, offset, rowcount, seqno) or None if invalid (partial/corrupt).
    minute must be 12 digits; offset/rowcount/seqno non-negative ints."""
    stripped = line.strip()
    if not stripped:
        return None
    parts = stripped.split(",")
    if len(parts) != 4:
        return None
    minute, offset_s, rowcount_s, seqno_s = parts
    if len(minute) != 12 or not minute.isdigit():
        return None
    try:
        offset = int(offset_s)
        rowcount = int(rowcount_s)
        seqno = int(seqno_s)
    except ValueError:
        return None
    if offset < 0 or rowcount < 0 or seqno < 0:
        return None
    return (minute, offset, rowcount, seqno)


def recover_tickfile_seqno(output_dir: str, minute_key: str) -> int:
    """Thin wrapper (deprecated by sidecar recovery; kept for back-compat). Returns last seqno."""
    path = get_tickfile_path(output_dir, minute_key)
    if not os.path.exists(path):
        return 0
    try:
        return _scan_tickfile_rows(path)[1]
    except OSError:
        return 0


def _scan_tickfile_rows(path: str):
    """Single-pass row scan (M-R23-2): returns (minute_set, last_seqno) from a tickfile.
    Reads UpdateTime (col 16) + Seqno (col 59); skips header + non-65-field lines."""
    minutes: set = set()
    last_seqno = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        for line_num, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped or line_num == 1:
                continue
            fields = stripped.split(",")
            if len(fields) != 65:
                continue
            mk = fields[16].replace(" ", "").replace(":", "")[:12]
            if len(mk) == 12 and mk.isdigit():
                minutes.add(mk)
            try:
                last_seqno = max(last_seqno, int(fields[59]))
            except (ValueError, IndexError):
                pass
    return minutes, last_seqno


class _FallbackSignal(Exception):
    pass


def _backup_truncated_tail(tickfile_path: str, offset: int, old_size: int) -> bool:
    """Copy [offset, old_size) to a .truncated.{time_ns}.{pid} file. Returns False on IO failure."""
    import time as _time
    try:
        with open(tickfile_path, "rb") as f:
            f.seek(offset)
            tail = f.read(old_size - offset)
        backup_path = f"{tickfile_path}.truncated.{_time.time_ns()}.{os.getpid()}"
        with open(backup_path, "wb") as bf:
            bf.write(tail)
            bf.flush()
            os.fsync(bf.fileno())
        return True
    except OSError:
        logger.exception("Backup of truncated tail failed for %s", tickfile_path)
        return False


def _tail_strip_partial_last_line(tickfile_path: str) -> int:
    """INV-CM-FALLBACK-STRIP: if the last line isn't a complete 65-field data row,
    truncate the file back to the last '\\n' boundary. Returns bytes stripped."""
    try:
        size = os.path.getsize(tickfile_path)
    except OSError:
        return 0
    if size == 0:
        return 0
    tail = min(size, TICKFILE_TAIL_READ_SIZE)
    with open(tickfile_path, "rb") as f:
        f.seek(-tail, 2)
        data = f.read()
    # find the last newline; if file ends with a complete 65-field line, strip nothing
    last_nl = data.rfind(b"\n")
    if last_nl == len(data) - 1:
        seg = data[:last_nl]
        inner_nl = seg.rfind(b"\n")
        last_line = seg[inner_nl + 1:].decode("utf-8", errors="replace").strip()
        if last_line and len(last_line.split(",")) == 65:
            return 0
    if last_nl == -1:
        keep = 0
    else:
        keep = size - len(data) + last_nl + 1  # bytes before the partial tail (incl. the newline)
    if keep < size:
        os.truncate(tickfile_path, keep)
        logger.critical("Tickfile tail-strip: removed %d partial bytes from %s", size - keep, tickfile_path)
        return size - keep
    return 0


def _write_recovery_audit(output_dir, date, *, had_sidecar, committed_count,
                          last_commit_minute, truncate_bytes, result, fallback_mode=False):
    """Best-effort persistent audit log (INV-CM-AUDIT-BESTEFFORT). Never raises."""
    try:
        import time as _time
        log_dir = os.path.join(output_dir, "tickfile")
        os.makedirs(log_dir, exist_ok=True)  # C-R25-2: self-create, don't rely on data path
        rec = {
            "ts": _time.time(),
            "date": date,
            "pid": os.getpid(),
            "hostname": socket.gethostname() or "unknown",
            "had_sidecar": had_sidecar,
            "committed_count": committed_count,
            "last_commit_minute": last_commit_minute,
            "truncate_bytes": truncate_bytes,
            "result": result,           # truncate | noop | fallback | error | tamper
            "fallback_mode": fallback_mode,
        }
        with open(os.path.join(log_dir, "tickfile_recovery.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        logger.debug("audit log write failed (best-effort)", exc_info=True)


def _fallback_recover(output_dir, tickfile_path, date, enable_commit_marker, has_sidecar_file, result):
    """Single-pass row scan + optional tail-strip. INV-CM-FAIL-ATOMIC: scan in try; mutate only after.

    INV-CM-FALLBACK-STRIP (M-R7-2): tail-strip runs ONLY on the sidecar-MISSING path
    (has_sidecar_file=False). When the sidecar exists but is untrustworthy (e.g.
    INV-CM-SIDECAR-OFFSET-BOUND violation), we do NOT mutate the file — that signals
    tamper/inconsistency, and the conservative action is read-only scan."""
    try:
        committed_set, last_seqno = _scan_tickfile_rows(tickfile_path)
    except OSError:
        logger.exception("fallback row scan failed %s; file untouched", tickfile_path)
        _write_recovery_audit(output_dir, date, had_sidecar=False, committed_count=0,
                              last_commit_minute=None, truncate_bytes=0, result="error", fallback_mode=True)
        raise
    truncate_bytes = 0
    if enable_commit_marker and not has_sidecar_file:
        truncate_bytes = _tail_strip_partial_last_line(tickfile_path)
    _write_recovery_audit(output_dir, date, had_sidecar=False,
                          committed_count=len(committed_set),
                          last_commit_minute=(max(committed_set) if committed_set else None),
                          truncate_bytes=truncate_bytes, result=result, fallback_mode=True)
    return (committed_set, last_seqno, False)


def _recover_tickfile_to_last_commit(output_dir: str, date: str, enable_commit_marker: bool = True):
    """Read sidecar, truncate tickfile to last commit. Returns (committed_set, last_seqno, had_sidecar).
    had_sidecar=True  -> sidecar authoritative; tickfile truncated to max offset.
    had_sidecar=False -> fallback single-pass row scan (minutes + seqno); tail-strip only when commit_marker on.
    INV-CM-RECOVERY-GATE: when enable_commit_marker=False, still returns row-scan fallback (had_sidecar=False)."""
    sample_mk = f"{date}0000"
    tickfile_path = get_tickfile_path(output_dir, sample_mk)
    sidecar_path = tickfile_path + ".commit"
    lockfile_path = tickfile_path + ".lock"

    if not os.path.exists(tickfile_path):
        _write_recovery_audit(output_dir, date, had_sidecar=False, committed_count=0,
                              last_commit_minute=None, truncate_bytes=0, result="noop")
        return (set(), 0, False)

    sidecar_records = _read_valid_sidecar(sidecar_path, date)
    has_sidecar_file = sidecar_records is not None

    # --- sidecar mode (enabled + non-empty valid records) ---
    if enable_commit_marker and sidecar_records:
        with _get_write_lock(tickfile_path):
            with _flock_critical_section(lockfile_path):
                try:
                    records = _read_valid_sidecar(sidecar_path, date) or []
                    if not records:
                        raise _FallbackSignal()
                    current_size = os.path.getsize(tickfile_path)
                    max_rec = max(records, key=lambda r: r[1])      # INV-CM-OFFSET-MAX
                    max_offset = max_rec[1]
                    last_seqno = max_rec[3]
                    committed_set = {r[0] for r in records}
                    if max_offset > current_size:                   # INV-CM-SIDECAR-OFFSET-BOUND
                        logger.critical("sidecar offset %d > tickfile size %d (%s); fallback",
                                        max_offset, current_size, tickfile_path)
                        raise _FallbackSignal()
                    truncate_bytes = 0
                    result = "noop"
                    if current_size > max_offset:
                        if not _backup_truncated_tail(tickfile_path, max_offset, current_size):
                            logger.critical("backup failed; aborting truncate (degraded) %s", tickfile_path)
                            raise _FallbackSignal()
                        os.truncate(tickfile_path, max_offset)      # path-based, atomic; FAIL-ATOMIC-safe
                        truncate_bytes = current_size - max_offset
                        result = "truncate"
                except _FallbackSignal:
                    return _fallback_recover(output_dir, tickfile_path, date, enable_commit_marker,
                                             has_sidecar_file, "fallback")
                except OSError:
                    logger.exception("recovery truncate failed (INV-CM-FAIL-ATOMIC) %s", tickfile_path)
                    _write_recovery_audit(output_dir, date, had_sidecar=False, committed_count=0,
                                          last_commit_minute=None, truncate_bytes=0, result="error",
                                          fallback_mode=True)
                    raise
                _write_recovery_audit(output_dir, date, had_sidecar=True,
                                      committed_count=len(committed_set),
                                      last_commit_minute=max_rec[0],
                                      truncate_bytes=truncate_bytes, result=result)
                return (committed_set, last_seqno, True)

    # --- fallback path ---
    return _fallback_recover(output_dir, tickfile_path, date, enable_commit_marker, has_sidecar_file, "fallback")


def extract_minutes_from_tickfile(path: str) -> set:
    """Read a per-day tickfile; return distinct minute_keys from the UpdateTime column (index 16).
    Module-level (promoted from ReplayEngine) so writer.recovery + flusher fallback can call it
    without importing replay (circular). Does NOT re-round the minute (m-R19-1)."""
    present: set = set()
    with open(path, "r", encoding="utf-8", newline="") as f:
        for line_num, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped or line_num == 1:
                continue
            fields = stripped.split(",")
            if len(fields) != 65:
                continue
            minute_key = fields[16].replace(" ", "").replace(":", "")[:12]
            if len(minute_key) == 12 and minute_key.isdigit():
                present.add(minute_key)
    return present


def _read_valid_sidecar(sidecar_path: str, date: str):
    """Read & validate sidecar lines for `date`. Returns:
      None  -> file missing (caller: treat as no-sidecar).
      []    -> file present but zero valid lines (≡ missing, INV-CM-SIDECAR-EMPTY-EQUIV-MISSING).
      [records] -> list of (minute, offset, rowcount, seqno), offset-strictly-increasing (INV-CM-OFFSET-MONO),
                   date-filtered (INV-CM-DATE-FILTER)."""
    if not os.path.exists(sidecar_path):
        return None
    records = []
    last_offset = -1
    try:
        with open(sidecar_path, "r", encoding="utf-8", newline="") as f:
            for line in f:
                rec = _parse_commit_line(line)
                if rec is None:
                    continue
                minute, offset, rowcount, seqno = rec
                if not minute.startswith(date):
                    logger.warning("Sidecar cross-date record skipped: %s (expected date %s)", minute, date)
                    continue
                if offset <= last_offset:
                    logger.warning("Sidecar non-monotonic offset skipped: %d after %d", offset, last_offset)
                    continue
                last_offset = offset
                records.append(rec)
    except OSError:
        logger.warning("Sidecar read failed: %s", sidecar_path, exc_info=True)
        return []
    return records


@contextlib.contextmanager
def _flock_critical_section(lockfile_path: str):
    """Cross-process exclusive non-blocking flock for the with-block.
    POSIX fcntl.flock(LOCK_EX|LOCK_NB); Windows dev/test = best-effort no-op (M-R25-3: prod is Linux).
    Raises BlockingIOError if held by another process. fd lives for the whole block; close releases."""
    with open(lockfile_path, "a") as lockfile_f:  # "a": create-if-absent, never truncate (LOCKFILE-IMMORTAL)
        if _HAS_FCNTL:
            _fcntl.flock(lockfile_f.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        else:
            logger.debug("flock unavailable (non-POSIX); cross-process lock skipped for %s", lockfile_path)
        try:
            yield
        finally:
            pass  # fd close on with-exit releases the OFD flock


def _sidecar_tail_last_record(sidecar_path: str, date: str):
    """Tail-read the sidecar's last valid record (REGEN-GUARD precondition, ms-level).
    Returns (minute, offset, rowcount, seqno) or None."""
    try:
        size = os.path.getsize(sidecar_path)
    except OSError:
        return None
    if size == 0:
        return None
    if size > MAX_SIDECAR_SIZE:
        logger.critical("Sidecar huge (%d bytes > %d); tail-reading last %d only",
                        size, MAX_SIDECAR_SIZE, SIDECAR_TAIL_READ_SIZE)
    tail = min(size, SIDECAR_TAIL_READ_SIZE)
    try:
        with open(sidecar_path, "rb") as f:
            f.seek(-tail, 2)
            data = f.read()
    except OSError:
        logger.warning("Sidecar tail read failed: %s", sidecar_path, exc_info=True)
        return None
    last = None
    for raw in reversed(data.split(b"\n")):
        if not raw.strip():
            continue
        rec = _parse_commit_line(raw.decode("utf-8", errors="replace"))
        if rec and rec[0].startswith(date):
            last = rec
            break
    return last


def _classify_append_precondition(current_minute_key: str, sidecar_path: str, tickfile_path: str):
    """REGEN-GUARD predicate. Returns (kind, last_record):
      ("new", None)              — sidecar missing/empty -> first write of the day.
      ("committed", last_rec)    — sidecar last minute == current AND tickfile size == offset -> skip.
      ("append", last_rec)       — last minute <= current AND size <= offset -> clean append.
      ("truncate_rewrite", last_rec) — tickfile size > last offset -> uncommitted residue; truncate to offset then write.
    last_record = (minute, offset, rowcount, seqno) of sidecar's last valid line."""
    last_rec = _sidecar_tail_last_record(sidecar_path, extract_date_from_minute_key(current_minute_key))
    if last_rec is None:
        return ("new", None)
    last_minute, last_offset = last_rec[0], last_rec[1]
    try:
        size = os.path.getsize(tickfile_path)
    except OSError:
        size = 0
    if last_minute == current_minute_key:
        if size == last_offset:
            return ("committed", last_rec)
        if size > last_offset:
            return ("truncate_rewrite", last_rec)
        logger.warning(
            "Tickfile size %d < sidecar offset %d (anomaly; possible truncation/tamper) minute=%s; "
            "skipping to avoid double-write",
            size, last_offset, current_minute_key,
        )
        return ("committed", last_rec)
    # last_minute < current (normal) — or > current (anomaly)
    if last_minute > current_minute_key:
        logger.warning("Sidecar last minute %s > current %s (anomaly); treating as append",
                       last_minute, current_minute_key)
    if size > last_offset:
        return ("truncate_rewrite", last_rec)
    return ("append", last_rec)
