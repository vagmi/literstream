#!/usr/bin/env python3
"""Estimate object-store operation counts (and GCS cost) for a fleet of
literstream databases at production intervals.

The model is built from what each code path actually does after the local
FileIndex change (so steady-state planning does NOT list the store):

  base replication (L0)
    - Each interval in which a database has new committed frames produces one L0
      LTX file: one PUT (CAS). Idle databases produce nothing.
    - The single workload-dependent input is f0 = L0 files per database per day.

  compaction (Driver uses compact_level per level; no lists, FileIndex is local)
    - L1 (hourly): merges that hour's new L0 files into one L1 file.
        reads  = each L0 file once  -> f0 GETs/day
        writes = one L1 file per active hour -> up to (86400/L1) PUTs/day
    - L2 (daily): merges the day's L1 files into one L2 file.
        reads  = the day's L1 files (<= hours/day) GETs, writes = 1 PUT/day
    - Each input file is read exactly once per level (compaction advances a
      frontier), so there is no re-reading.

  snapshot (daily): rebuilds the min=1 base from the restore plan.
        reads ~= a few plan files (GET), writes = 1 PUT/day

  retention: deletes only. DELETE is FREE on GCS, so it costs nothing.

  open (per process start / deploy): seeds the FileIndex once.
        reads = (MAX_LEVEL+1) LISTs + 1 GET per database. One-time, not recurring.

Op classes / GCS pricing (Standard, per 1,000 ops; verify current numbers):
  Class A (PUT/insert, LIST): ~$0.005 / 1,000   <- the expensive ones
  Class B (GET/HEAD):         ~$0.0004 / 1,000
  DELETE:                     free
"""

# ----- fleet + intervals (the "standard" config from the question) -----
N_DBS = 100
CHECKPOINT_S = 60         # 1 minute: WAL fold cadence (also the finest L0 cadence)
L1_S = 3600               # 1 hour
L2_S = 86400              # 1 day
SNAPSHOT_S = 86400        # 1 day
RESTARTS_PER_DAY = 1      # deploys / process restarts (drives the one-time open lists)
MAX_LEVELS = 10           # levels scanned on open (0..=9)
SNAPSHOT_PLAN_FILES = 3   # ~files a daily snapshot reads to rebuild the base

CLASS_A_PER_1K = 0.005    # $ per 1,000 PUT/LIST
CLASS_B_PER_1K = 0.0004   # $ per 1,000 GET

SECS_PER_DAY = 86400
DAYS_PER_MONTH = 30


def per_db_day(f0):
    """Op counts for one database over one day, given f0 = L0 files/day."""
    l1_runs = SECS_PER_DAY // L1_S            # 24
    l2_runs = SECS_PER_DAY // L2_S            # 1
    snap_runs = SECS_PER_DAY // SNAPSHOT_S    # 1
    active = 1 if f0 > 0 else 0

    l1_put = min(l1_runs, f0)                 # one L1 file per active hour
    l2_put = min(l2_runs, 1) * active
    snap_put = snap_runs * active

    l0_put = f0
    open_list = MAX_LEVELS * RESTARTS_PER_DAY

    l1_get = f0                               # each L0 read once into L1
    l2_get = min(l1_put, l1_runs)             # the day's L1 files read into L2
    snap_get = SNAPSHOT_PLAN_FILES * snap_runs * active
    open_get = RESTARTS_PER_DAY

    puts = l0_put + l1_put + l2_put + snap_put
    lists = open_list
    gets = l1_get + l2_get + snap_get + open_get
    deletes = f0 + l1_put                     # aged-out L0 + old L1 (free)

    return {
        "L0 put": l0_put, "L1 put": l1_put, "L2 put": l2_put, "snapshot put": snap_put,
        "open list": lists,
        "L1 get": l1_get, "L2 get": l2_get, "snapshot get": snap_get, "open get": open_get,
        "class_a": puts + lists,
        "class_b": gets,
        "deletes(free)": deletes,
    }


def report(f0, label):
    d = per_db_day(f0)
    a_day = d["class_a"] * N_DBS
    b_day = d["class_b"] * N_DBS
    a_mo = a_day * DAYS_PER_MONTH
    b_mo = b_day * DAYS_PER_MONTH
    cost_mo = a_mo / 1000 * CLASS_A_PER_1K + b_mo / 1000 * CLASS_B_PER_1K
    print(f"\n=== {label}: f0 = {f0} L0 files/db/day ===")
    print(f"  per db/day   : {d['class_a']:>6} Class A (put+list), {d['class_b']:>6} Class B (get), "
          f"{d['deletes(free)']:>6} deletes(free)")
    print(f"    breakdown  : L0 put {d['L0 put']}, L1 put {d['L1 put']}, L2 put {d['L2 put']}, "
          f"snap put {d['snapshot put']}, open list {d['open list']}")
    print(f"  fleet ({N_DBS}) : {a_day:>8,} Class A/day, {b_day:>8,} Class B/day")
    print(f"  fleet/month  : {a_mo:>10,} Class A, {b_mo:>10,} Class B")
    print(f"  GCS cost/mo  : ${cost_mo:,.2f}  (Class A ${a_mo/1000*CLASS_A_PER_1K:,.2f} + "
          f"Class B ${b_mo/1000*CLASS_B_PER_1K:,.2f}; deletes free)")


if __name__ == "__main__":
    print(f"literstream op estimate | {N_DBS} databases | checkpoint {CHECKPOINT_S}s, "
          f"L1 {L1_S//60}m, L2 {L2_S//3600}h, snapshot {SNAPSHOT_S//3600}h | "
          f"{RESTARTS_PER_DAY} restart/day")
    print("(steady state does NOT list the store; the only lists are the per-restart open() seed.)")

    # f0 = distinct minutes/day a database is actually written (one L0 file each).
    # A written-then-idle database now goes quiet (the seq-bump loop is fixed), so
    # f0 tracks real writes rather than being pinned at 1440 by bookkeeping.
    scenarios = [
        (24,   "hourly activity"),
        (144,  "every ~10 min"),
        (1440, "continuously written all day"),
    ]
    for f0, label in scenarios:
        report(f0, label)
