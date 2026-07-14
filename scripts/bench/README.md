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
| **light** (~300 rows/s) — avg RSS | **16 MB** | 38 MB |
| light — peak RSS | **28 MB** | 41 MB |
| light — CPU | **0.12 s** | 0.26 s |
| **heavy** (~2000 rows/s) — avg RSS | **40.6 MB** | 38.6 MB |
| heavy — peak RSS | 63 MB | **42 MB** |
| heavy — CPU | **0.34 s** | 0.57 s |
| binary size | **9.5 MB** | 51 MB |

**Rust wins CPU everywhere (~2×) and memory on light/small databases; on the heavy
large-DB case it ties Go on average RSS (40.6 vs 38.6).** The only remaining gap is
heavy *peak* RSS (63 vs 42), an occasional re-snapshot spike. Both restore exactly
with a clean integrity check.

For context, before the perf work the heavy case was **909 MB peak / 3.5 s CPU**;
it is now 63 MB / 0.34 s and correct — a ~14× memory and ~10× CPU improvement.

## The checkpoint model (matches Litestream's philosophy)

Like Litestream, literstream **never stalls the application writer**. The
checkpoint is non-blocking PASSIVE; correctness against the checkpoint/replication
race comes from *noticing*, not from freezing writes:

1. **Capture first, upload later (an in-memory shadow).** `checkpoint_if_needed`
   *builds* the pending LTX from the WAL (fast, local) and holds it, then
   checkpoints, then uploads. Because the captured bytes outlive the WAL reset, the
   race window shrinks from a network upload to a local build — so re-snapshots
   become *rare*, not one per checkpoint. This dropped heavy avg RSS from ~49 MB to
   ~40 MB.
2. **Detection.** If the checkpoint still folds more frames into the DB than we
   captured (a write in that tiny window), the next sync **re-snapshots** from the
   DB. Correctness by noticing — never a write stall. These rare re-snapshots
   (whole-DB reads) are the source of the heavy-case peak.
3. **Seq-bump reset.** After a PASSIVE checkpoint that fully drained the WAL,
   literstream writes one `_literstream_seq` row (while the read-mark is released)
   to force SQLite to restart the WAL into a fresh generation — the same trick
   Litestream uses to keep the WAL bounded.
4. **Bounded WAL tail reads** (`WalReader::from_tail`, O(new frames)) and a
   **single-threaded runtime** for the CLI (`current_thread`).

**Known limitation (shared with Litestream):** the shadow is *in-memory* (covers a
single checkpoint, not a process restart), and under *sustained* writes PASSIVE
keeps returning `SQLITE_BUSY` and can't fully drain, so the seq-bump reset only
fires in idle windows and the `-wal` can grow on disk (monitor it). A *persistent*
shadow WAL would also survive restarts — a further step this port doesn't take.

## Bugs found and fixed via this bench

1. **Read-mark released between syncs → data loss.** The syncer acquired/released
   the WAL read-mark inside each `sync()`, so an external writer's checkpoint (e.g.
   on connection close) could reset the WAL and recycle frames before they were
   replicated. Fixed by holding the pinned read transaction for the syncer's
   lifetime (like litestream), releasing only around the port's own checkpoints.

2. **Re-snapshot storm → huge memory/CPU.** `wal_frame_count()` reads the `-wal`
   file's high-water size, which a PASSIVE checkpoint doesn't shrink — so the
   checkpoint threshold tripped *every tick*, forcing a full re-snapshot each time
   (the 909 MB spike). Fixed by gating on WAL **growth since the last checkpoint**.

3. **Checkpoint gap → corrupt restore.** A non-blocking PASSIVE checkpoint can
   fold frames into the DB that we hadn't synced; the following incremental
   silently dropped them → malformed image. Fixed by **detecting** it (checkpoint
   moved more frames than we'd synced → next sync re-snapshots) — Litestream's
   never-block approach, not a write freeze.

4. **Replica key layout.** Aligned to litestream's `<prefix>/<level:04x>/…` remote
   layout so the port can restore litestream's S3/GCS replicas (verified) and vice
   versa.
