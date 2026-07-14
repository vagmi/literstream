# Bench: literstream (Rust) vs litestream (Go)

Measures CPU and resident memory (RSS) of each tool while it **continuously
replicates an identical SQLite write workload** to a local-directory replica.

## Why local directory (not S3/Garage)

We started against Garage but Garage v1.0.1 doesn't enforce conditional PUT
(if-none-match), which the Rust CAS path relies on. A local directory removes the
object store and the network from the loop entirely, so the numbers reflect each
tool's *own* overhead — and litestream has a native `file` replica type, so it's
apples-to-apples.

## Method

- **Same workload, deterministic:** `workload.py` writes fixed-size random rows in
  timed batches over a **long-lived** connection (as a real app would), seeded so
  both runs are byte-identical. WAL mode, autocheckpoint disabled (the replicator
  owns checkpointing).
- **Long-lived writer:** the writer holds its connection open for a settle period
  after the last write; the replicator is signalled to drain *while the writer is
  still connected*. (Closing a WAL connection can reset the WAL — see the bug note
  below.)
- **Same cadence:** both replicate on a 1s monitor interval, with the same tiered
  compaction defaults (L1@30s, L2@5m, L3@1h).
- **Sequential runs** (no cross-tool contention); RSS + cumulative CPU sampled once
  a second via `ps`; correctness verified by restoring each replica with its own
  tool and comparing row counts.

## Run it

```sh
cargo build --release --features cli --bin literstream
./scripts/bench/run.sh [duration_s] [batches_per_s] [rows_per_batch] [payload_bytes]
# e.g. light:  ./scripts/bench/run.sh 60 10 30  200   (~300 rows/s)
#      heavy:  ./scripts/bench/run.sh 60 20 100 200   (~2000 rows/s)
```

## Representative results (Apple Silicon, macOS; 60s runs)

After the fixes below. Both tools restore to the exact source row count with
`PRAGMA integrity_check = ok`.

| Metric | literstream (Rust) | litestream (Go) |
|---|---|---|
| **light** (~300 rows/s) — avg RSS | **32.7 MB** | 37.2 MB |
| light — peak RSS | 53 MB | **39 MB** |
| light — CPU | **0.17 s** | 0.22 s |
| **heavy** (~2000 rows/s) — avg RSS | 68.9 MB | **38.4 MB** |
| heavy — peak RSS | 136 MB | **40 MB** |
| heavy — CPU | **0.45 s** | 0.62 s |
| binary size | **9.5 MB** | 51 MB |

**Rust wins CPU in both cases.** For the light / small-database case (the intended
use), **Rust also wins memory**. Under heavy write load against a large (~22 MB)
database, Go's RSS stays flat while Rust's rises (peak ~136 MB) — see below.

For context, before the perf work the heavy case was **909 MB peak / 3.5 s CPU**
for Rust; it is now 136 MB / 0.45 s and correct.

## What this shows (and the remaining gap)

- **CPU**: Rust is faster (compiled, no GC). Expected.
- **Memory, small/light DB**: Rust is now *flat and lower than Go*.
- **Memory, heavy/large DB**: Go stays flat (~40 MB) because it streams WAL frames
  into a shadow WAL and uploads via `io.Pipe`, never holding a whole file. The port
  still does whole-file work in two spots: every sync `fs::read`s the entire `-wal`,
  and a **re-snapshot** (rare, but reads the whole DB) fires whenever a non-blocking
  checkpoint races the writer. On a big DB those whole-DB reads set the RSS
  high-water. Closing this fully means streaming the WAL *tail* and the snapshot/
  upload path (e.g. a pipe) — the obvious next step, tracked separately.

## Bugs found and fixed via this bench

1. **Read-mark released between syncs → data loss.** The syncer acquired/released
   the WAL read-mark inside each `sync()`, so an external writer's checkpoint (e.g.
   on connection close) could reset the WAL and recycle frames before they were
   replicated. Fixed by holding the pinned read transaction for the syncer's
   lifetime (like litestream), releasing only around the port's own checkpoints.

2. **Re-snapshot storm → huge memory/CPU.** `wal_frame_count()` reads the `-wal`
   file's high-water size, which a PASSIVE checkpoint doesn't shrink — so the
   checkpoint threshold tripped *every tick*, each checkpoint restarted the WAL
   (new salt), and the salt change forced a full re-snapshot (the 909 MB spike).
   Fixed by gating PASSIVE on WAL **growth since the last checkpoint**.

3. **Checkpoint gap → corrupt restore.** Fixing (2) exposed a latent gap: a
   non-blocking PASSIVE checkpoint can move frames into the DB that we hadn't
   synced, and the following incremental (new WAL generation) silently dropped
   them → malformed image. Fixed by *detecting* it — if a checkpoint moved more
   frames than we'd synced, the next sync re-snapshots from the DB; otherwise it
   takes the cheap incremental path. The driver also drains fully before
   checkpointing, so the common case is gap-free.

4. **Replica key layout.** Aligned to litestream's `<prefix>/<level:04x>/…` remote
   layout so the port can restore litestream's S3/GCS replicas (verified) and vice
   versa.
