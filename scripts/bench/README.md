# Bench: literstream (Rust) vs litestream (Go)

Measures CPU and resident memory (RSS) of each tool while it **continuously
replicates an identical SQLite write workload** to a local-directory replica.

## Why local directory (not S3/Garage)

We started against Garage but Garage v1.0.1 doesn't enforce conditional PUT
(if-none-match), which the Rust CAS path relies on. A local directory removes the
object store and the network from the loop entirely, so the numbers reflect each
tool's *own* overhead, and litestream has a native `file` replica type, so it's
apples-to-apples.

## Method

- **Same workload, deterministic:** `workload.py` writes fixed-size random rows in
  timed batches over a **long-lived** connection (as a real app would), seeded so
  both runs are byte-identical. WAL mode, autocheckpoint disabled (the replicator
  owns checkpointing).
- **Long-lived writer:** the writer holds its connection open for a settle period
  after the last write; the replicator is signalled to drain *while the writer is
  still connected*. (Closing a WAL connection can reset the WAL, see the bug note
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

Measured after the staging tier landed (see below). Both tools restore to the
exact source row count with `PRAGMA integrity_check = ok`.

| Scenario | Metric | literstream (Rust) | litestream (Go) |
|---|---|---|---|
| light (~300 rows/s) | avg RSS | **18.5 MB** | 38.0 MB |
| light (~300 rows/s) | peak RSS | **29.0 MB** | 39.3 MB |
| light (~300 rows/s) | CPU | **0.52 s** | 0.54 s |
| heavy (~2000 rows/s) | avg RSS | 41.7 MB | **38.5 MB** |
| heavy (~2000 rows/s) | peak RSS | 68.8 MB | **43.0 MB** |
| heavy (~2000 rows/s) | CPU | **0.95 s** | 1.42 s |
| binary size | | **9.5 MB** | 51 MB |

On the light workload literstream uses roughly half the memory of litestream,
18.5 MB versus 38 MB on average and 29 MB versus 39 MB at peak, and it matches
litestream on CPU. On the heavy workload literstream wins CPU, 0.95 s versus
1.42 s, and ties on average RSS, 41.7 MB versus 38.5 MB, while litestream wins
heavy peak RSS, 43 MB versus 68.8 MB. That peak gap is the occasional catch-up
snapshot that the never-block checkpoint takes when a write races the checkpoint.
Both tools restore exactly with a clean integrity check.

The staging tier that landed after the first round of perf work trades some CPU
for durability. Every sync now fsyncs its LTX file to a local staging directory
before the checkpoint, and against a local directory replica it writes those
bytes twice, once to staging and once to the replica. That raised literstream's
CPU relative to the earlier in-memory design, though it still wins or ties
litestream here. Against a real object store the second write is a network
upload rather than a local disk write, so the production CPU cost is lower than
this local directory benchmark suggests.

## The checkpoint model (matches Litestream's philosophy)

Like Litestream, literstream **never stalls the application writer**. The
checkpoint is non-blocking PASSIVE, and correctness against the checkpoint and
replication race now comes from making the frames durable before the checkpoint
runs, not from freezing writes:

1. **Stage first, upload later, through a local staging directory.**
   `checkpoint_if_needed` encodes the pending LTX from the WAL to a file under
   `<db>-litestream/ltx/` and fsyncs it before it checkpoints. Because the staged
   file is durable before the checkpoint merges its frames into the main database,
   neither a WAL reset nor a process crash can lose those frames. On the next start
   literstream re-uploads any staged file it finds, which recovers the exact frames
   with no re-snapshot. It then uploads from the staged file and removes it. Sync
   is local and fast, upload is remote and slow, and the two are now decoupled, so
   an object store outage lets replication keep staging and checkpointing and catch
   up later.
2. **Detection.** If a write races the checkpoint and it merges more frames into
   the database than literstream captured, those frames now live only in the
   database file. literstream notices this and immediately stages a catch-up
   snapshot from the database, which is itself durable. This is the never-block
   fallback, and the `Driver` reports it as `resnapshot_fired`. These rare
   whole-database snapshots are the source of the heavy-case peak.
3. **Seq-bump reset.** After a PASSIVE checkpoint that fully drained the WAL,
   literstream writes one `_literstream_seq` row while the read-mark is released,
   which forces SQLite to restart the WAL into a fresh generation. This is the same
   trick Litestream uses to keep the WAL bounded.
4. **Checkpoint policy.** The threshold is gated on the logical WAL offset rather
   than the `-wal` file size, which a PASSIVE checkpoint leaves stale (Litestream
   issue #997). A large blocking TRUNCATE checkpoint acts as an emergency brake
   under sustained writes, which is safe now that durability does not depend on the
   checkpoint. A time-based checkpoint merges a low-write database's WAL on a fixed
   interval so its restore stays cheap. Reads stay bounded through
   `WalReader::from_tail` (O(new frames)), and the CLI uses a single-threaded
   runtime (`current_thread`).

This staging tier resolves the earlier design's main limitation. The previous
approach held the pending LTX in memory, which covered a single checkpoint but not
a process restart. The staged files are on disk and survive a restart, so the
crash window between a checkpoint and its upload is now closed. Under sustained
writes PASSIVE can still return `SQLITE_BUSY` and fail to drain, so the WAL can
grow on disk until an idle window or until the TRUNCATE brake fires. Monitor disk
usage in that case.

## Bugs found and fixed via this bench

1. **Read-mark released between syncs → data loss.** The syncer acquired/released
   the WAL read-mark inside each `sync()`, so an external writer's checkpoint (e.g.
   on connection close) could reset the WAL and recycle frames before they were
   replicated. Fixed by holding the pinned read transaction for the syncer's
   lifetime (like litestream), releasing only around the port's own checkpoints.

2. **Re-snapshot storm → huge memory/CPU.** `wal_frame_count()` reads the `-wal`
   file's high-water size, which a PASSIVE checkpoint doesn't shrink, so the
   checkpoint threshold tripped *every tick*, forcing a full re-snapshot each time
   (the 909 MB spike). Fixed by gating on WAL growth since the last checkpoint, and
   later refined to gate on the logical WAL offset (issue #997).

3. **Checkpoint gap → corrupt restore.** A non-blocking PASSIVE checkpoint can
   merge frames into the DB that we hadn't synced; the following incremental
   silently dropped them → malformed image. Fixed by **detecting** it (checkpoint
   moved more frames than we'd synced → next sync re-snapshots). The staging tier
   later made that recovery durable by staging a catch-up snapshot on the spot,
   rather than deferring it to the next sync.

4. **Replica key layout.** Aligned to litestream's `<prefix>/<level:04x>/…` remote
   layout so the port can restore litestream's S3/GCS replicas (verified) and vice
   versa.
