#!/usr/bin/env python3
"""Deterministic SQLite write workload for the litestream-vs-literstream bench.

Writes fixed-size random rows in timed batches over a *long-lived* connection
(as a real application would), then holds the connection open for a settle
period so the replicator can drain the final frames before the connection closes
(closing a WAL connection can reset the WAL). Runs against WAL mode with
autocheckpoint disabled — the replicator owns checkpointing.

    workload.py <db> <duration_s> <batches_per_s> <rows_per_batch> <payload_bytes> [settle_s] [seed]
"""
import random
import sqlite3
import sys
import time


def main() -> None:
    db = sys.argv[1]
    duration = float(sys.argv[2])
    bps = float(sys.argv[3])
    rows_per_batch = int(sys.argv[4])
    payload = int(sys.argv[5])
    settle = float(sys.argv[6]) if len(sys.argv) > 6 else 8.0
    seed = int(sys.argv[7]) if len(sys.argv) > 7 else 1234

    rng = random.Random(seed)
    con = sqlite3.connect(db, isolation_level=None)  # manual BEGIN/COMMIT
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA wal_autocheckpoint=0")  # replicator owns checkpoints
    con.execute("PRAGMA busy_timeout=5000")
    con.execute(
        "CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY AUTOINCREMENT, val BLOB)"
    )

    interval = 1.0 / bps
    deadline = time.monotonic() + duration
    rows = 0
    while time.monotonic() < deadline:
        start = time.monotonic()
        batch = [(rng.randbytes(payload),) for _ in range(rows_per_batch)]
        con.execute("BEGIN")
        con.executemany("INSERT INTO kv(val) VALUES (?)", batch)
        con.execute("COMMIT")
        rows += rows_per_batch
        elapsed = time.monotonic() - start
        if elapsed < interval:
            time.sleep(interval - elapsed)

    total = con.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
    print(f"wrote {rows} rows this run; table now has {total} rows", flush=True)
    # Keep the connection open so the replicator drains before the WAL can reset.
    time.sleep(settle)
    con.close()


if __name__ == "__main__":
    main()
