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
| **light** (~300 rows/s) — avg RSS | **15.5 MB** | 37.8 MB |
| light — peak RSS | **26 MB** | 40 MB |
| light — CPU | **0.12 s** | 0.23 s |
| **heavy** (~2000 rows/s) — avg RSS | **39.0 MB** | 39.3 MB |
| heavy — peak RSS | 65 MB | **41 MB** |
| heavy — CPU | **0.32 s** | 0.63 s |
| binary size | **9.5 MB** | 51 MB |

**Rust wins CPU everywhere (~2×) and memory on light/small databases; on the heavy
large-DB case it now ties Go on average RSS (39 vs 39).** The only remaining gap is
heavy *peak* RSS (65 vs 41), a transient spike during the final shutdown flush.

For context, before the perf work the heavy case was **909 MB peak / 3.5 s CPU**;
it is now 65 MB / 0.32 s and correct — a ~14× memory and ~11× CPU improvement.

## How the memory got flat

Three coupled changes brought Rust's RSS from climbing-with-DB-size to flat:

1. **Checkpoint under a write lock** (`Db::acquire_write_lock`, a second
   connection's `BEGIN IMMEDIATE`). Freezing writes around a final sync +
   checkpoint means the checkpoint never folds an un-replicated frame into the
   DB, so a WAL reset needs *no* re-snapshot (the earlier fix re-snapshotted the
   whole DB on every racing checkpoint — the memory climb). PASSIVE-under-lock
   also makes TRUNCATE unnecessary.
2. **Bounded WAL tail reads.** Each incremental sync reads only `[offset..EOF]`
   of the `-wal` (the new frames), not the whole file — `WalReader::from_tail`
   parses a partial buffer with a base offset. Per-sync memory is now O(new
   frames) regardless of WAL size.
3. **Single-threaded runtime** for the CLI (`current_thread`) — the replicator is
   one I/O-bound task.

**Known limitation:** under *pathological continuous* writes the WAL can grow on
disk (our long-lived read-mark blocks SQLite's WAL restart), because a true
non-blocking shadow WAL isn't implemented. Memory stays flat regardless (tail
reads), and real/bursty workloads reset the WAL during idle windows. A full
shadow WAL (litestream's design) would bound the WAL on disk under all loads.

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
   them → malformed image. Fixed by checkpointing **under a write lock** (a second
   connection's `BEGIN IMMEDIATE`) around a final sync, so no frame commits between
   our last upload and the checkpoint — the incremental-after-reset path is then
   always safe, and no re-snapshot is needed.

4. **Replica key layout.** Aligned to litestream's `<prefix>/<level:04x>/…` remote
   layout so the port can restore litestream's S3/GCS replicas (verified) and vice
   versa.
