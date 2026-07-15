# literstream

**Continuously back up a live SQLite database to object storage — as a Rust library.**

literstream watches a running SQLite database and streams every change to an
object store (S3, GCS, Azure, local disk, or in-memory) as a chain of small,
immutable files. If the machine dies, you rebuild the database from the replica.
It can restore the latest state or **any earlier point in time**.

It is a **library**, not a daemon: you call `sync()` (or run the built-in
`Driver`) from inside your own application, so there's no sidecar process to
operate.

> ### Standing on the shoulders of a giant
>
> literstream is a Rust port of **[Litestream](https://litestream.io)** by
> **[Ben Johnson](https://github.com/benbjohnson)** — a genuinely brilliant piece
> of engineering that makes SQLite a first-class production database. This project
> exists to learn from and build on that work. The replication file format is
> **byte-compatible with Litestream's LTX format** (via
> [`superfly/ltx`](https://github.com/superfly/ltx)), so **Litestream can restore
> literstream's backups and vice-versa**. All the hard, clever ideas here are
> Litestream's; any bugs are ours. Thank you, Ben. 🙏

---

## What it does

- **Continuous replication.** Turns newly-committed WAL frames into immutable
  LTX files and uploads them as they happen.
- **Any object store.** Built on the [`object_store`](https://docs.rs/object_store)
  crate: S3, GCS, Azure, local filesystem, and memory all work with a one-line
  change.
- **Point-in-time recovery.** Restore the latest state, or rewind to a specific
  transaction ID or wall-clock timestamp.
- **Tiered compaction + retention.** Old files are merged into coarser levels on
  a schedule and pruned by age, keeping storage bounded. It is the same model as
  Litestream (raw L0 → time-based levels → full snapshots).
- **Bounded memory, any size.** Snapshots pread page by page, restores stream to
  disk, compaction is a streaming k-way merge, and large uploads go out in parts,
  so replicating or restoring a multi-gigabyte database uses kilobytes of memory,
  not gigabytes.
- **Durable by staging.** Each LTX file is written and fsync'd to a local staging
  directory before the checkpoint that could merge its frames into the database,
  so a crash between checkpoint and upload loses nothing: the staged file is
  shipped on the next start.
- **Built for many databases.** One process can replicate hundreds of small
  per-tenant databases at once. Memory scales linearly (a fraction of a megabyte
  each), and steady-state replication never lists the object store (each
  replicator keeps a local file index), so idle databases cost almost nothing.
- **Safe by construction.** A single-writer lock plus compare-and-swap uploads
  prevent two processes from corrupting a replica.

> **Status:** a learning-oriented project, built feature-by-feature. The core
> replicate/restore path is validated both directions against the real Litestream
> binary. Treat it as experimental.

## How it works

literstream opens SQLite in **WAL mode**, disables SQLite's
automatic checkpointing so *it* decides when checkpoints happen, and holds a
long-lived **read transaction** that pins a spot in the WAL so a checkpoint can
never overwrite frames before they've been copied out. It reads the new WAL
frames, encodes them as **LTX** files, **stages them to a local directory and
fsyncs them** for durability, then uploads them to the object store (a small file
goes up with an **atomic, if-not-exists** write; a large one streams in parts).
Restoring just replays that file chain back into a database image.

For the full story, including how one process cheaply replicates a fleet of small
databases, see **[docs/how-it-works.md](docs/how-it-works.md)**.

## Quick start

```toml
# Cargo.toml
[dependencies]
literstream = "0.1"
object_store = { version = "0.14", features = ["aws", "gcp"] }
rusqlite = { version = "0.35", features = ["bundled"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

```rust
use std::sync::Arc;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{Syncer, restore};
use object_store::local::LocalFileSystem;
use rusqlite::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open the database the way literstream needs it (WAL mode, manual
    //    checkpoints). This does NOT lock out your application.
    let db = Db::open("app.db")?;

    // 2. Point at a replica. Any object_store backend works — swap this one line
    //    for S3 or GCS. Here we use local disk.
    std::fs::create_dir_all("./replica")?;
    let store = LocalFileSystem::new_with_prefix("./replica")?;
    let client = ReplicaClient::new(Arc::new(store), "app");

    // 3. The syncer ties them together and takes a single-writer lock.
    let mut syncer = Syncer::open(db, client).await?;

    // 4. Your application writes through its OWN connection — literstream only
    //    reads the WAL, it never writes your data for you.
    let app = Connection::open("app.db")?;
    app.execute_batch(
        "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT);
         INSERT INTO users(name) VALUES ('alice'), ('bob');",
    )?;

    // 5. Replicate the newly-committed WAL frames. Call this periodically (or use
    //    the `Driver`, below, which also checkpoints, compacts, and prunes).
    syncer.sync().await?;

    // 6. Disaster recovery: rebuild the database from the replica alone.
    let image = restore(syncer.client()).await?;
    std::fs::write("restored.db", image)?;

    Ok(())
}
```

### Continuous replication with the `Driver`

For a long-running app, drive the whole loop (sync, checkpoint, tiered
compaction, snapshots, and retention) from a single `tick`:

```rust
use std::time::{Duration, SystemTime};
use literstream::sync::{CompactionLevels, Driver};

let mut driver = Driver::new(syncer, CompactionLevels::default_levels());

loop {
    driver.tick(SystemTime::now()).await?;      // one scheduler step
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

`CompactionLevels::default_levels()` mirrors Litestream: merge L0→L1 every 30s,
L1→L2 every 5m, L2→L3 every 1h, with full snapshots and time-based retention.

### Point-in-time recovery

```rust
use literstream::sync::{restore_to_txid, restore_to_timestamp};

// Rebuild the database as of transaction 1042...
let at_txid = restore_to_txid(&client, 1042).await?;

// ...or as of a wall-clock time (milliseconds since the Unix epoch).
let at_time = restore_to_timestamp(&client, 1_752_000_000_000).await?;
```

## Examples

Runnable examples live in [`examples/`](examples):

- `00_simple_usage.rs`: the five-step replicate-and-restore path above.
- `01_complete.rs`: a full `Driver` loop: random traffic, tiered compaction,
  retention, and point-in-time recovery, narrated as it runs.
- `fanout_bench.rs`: replicates N databases at once to a real or local object
  store and reports memory, correctness, and a per-operation request count. It
  wraps the store to tally every operation, so the output is a request budget.

```sh
cargo run --example 00_simple_usage
LITERSTREAM_DEMO_SECS=90 cargo run --example 01_complete
FANOUT_LOCAL=/tmp/replica FANOUT_N=100 cargo run --release --example fanout_bench
```

To estimate the object-store operations and monthly cost for a fleet at your own
intervals, edit the constants in and run
[`scripts/ops-estimate.py`](scripts/ops-estimate.py).

## Acknowledgements

- **[Litestream](https://litestream.io)** and **[Ben Johnson](https://github.com/benbjohnson)**
  : the original, the behavior model, and the reason this exists.
- **[superfly/ltx](https://github.com/superfly/ltx)** : the LTX file format this
  is byte-compatible with.
- **[`object_store`](https://docs.rs/object_store)** : the storage abstraction
  that gives us every backend for free.

## License

See [LICENSE](LICENSE).
