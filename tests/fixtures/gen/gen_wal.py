#!/usr/bin/env python3
"""Generate a SQLite database with a populated, un-checkpointed WAL.

    python3 gen_wal.py OUT.db

Writes OUT.db (the base image, as of the last checkpoint = empty) plus
OUT.db-wal (all committed frames). We disable autocheckpoint and hard-exit with
os._exit so SQLite's close-time checkpoint never runs — leaving the WAL on disk
exactly as literstream would observe it on a live database.

The workload spans multiple transactions (so there are multiple commit frames)
and rewrites early pages (so the page map must keep the *latest* offset).
"""
import os
import sqlite3
import sys


def main() -> None:
    db = sys.argv[1]
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db + suffix)
        except FileNotFoundError:
            pass

    con = sqlite3.connect(db, isolation_level=None)  # explicit BEGIN/COMMIT
    con.execute("PRAGMA page_size=4096")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA wal_autocheckpoint=0")
    # NORMAL is the idiomatic WAL-mode setting: the WAL is fsync'd only at
    # checkpoint, not on every commit. It doesn't change what lands in the -wal
    # file here (we os._exit, and frames are already write()n to the OS), but it
    # mirrors how a real application would run.
    con.execute("PRAGMA synchronous=NORMAL")

    con.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, note TEXT)")

    con.execute("BEGIN")
    for i in range(1, 101):
        con.execute("INSERT INTO items VALUES(?,?,?)", (i, f"item-{i:04d}", "x" * 20))
    con.execute("COMMIT")

    con.execute("BEGIN")
    for i in range(101, 201):
        con.execute("INSERT INTO items VALUES(?,?,?)", (i, f"item-{i:04d}", "y" * 20))
    con.execute("COMMIT")

    # Rewrite early rows so page 1/2 get newer WAL frames (page-map dedup).
    con.execute("BEGIN")
    con.execute("UPDATE items SET note='updated' WHERE id<=10")
    con.execute("COMMIT")

    sys.stdout.write(f"wrote {db} + {db}-wal\n")
    sys.stdout.flush()
    os._exit(0)  # skip the close-time checkpoint; keep the WAL populated


if __name__ == "__main__":
    main()
