//! `literstream` — a minimal continuous-replication daemon that mirrors
//! litestream's `replicate` behaviour, for benchmarking against the real Go
//! litestream binary.
//!
//! It replicates to a **local directory** (an `object_store` filesystem backend,
//! equivalent to litestream's `file` replica type), so the benchmark measures
//! the two tools' own CPU/memory without a network or object-store CAS in the
//! loop. It opens the database literstream's way and runs the [`Driver`] once
//! per second — sync, checkpoint, tiered compaction, snapshots, retention —
//! until Ctrl-C, then drains.
//!
//!     literstream replicate <db-path>  <replica-dir>
//!     literstream restore   <out-path> <replica-dir>

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{CompactionLevels, Driver, Syncer, restore};
use object_store::local::LocalFileSystem;

// A single-threaded runtime: the replicator is one I/O-bound task, so worker
// threads would only add baseline memory.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("replicate") if args.len() >= 4 => replicate(&args).await,
        Some("restore") if args.len() >= 4 => restore_cmd(&args).await,
        _ => {
            eprintln!("usage:");
            eprintln!("  {} replicate <db-path>  <replica-dir>", args[0]);
            eprintln!("  {} restore   <out-path> <replica-dir>", args[0]);
            std::process::exit(2);
        }
    }
}

/// A replica client over a local directory (created if missing).
fn local_client(replica_dir: &str) -> ReplicaClient {
    std::fs::create_dir_all(replica_dir).expect("create replica dir");
    let store = LocalFileSystem::new_with_prefix(replica_dir).expect("open local store");
    ReplicaClient::new(Arc::new(store), "")
}

/// Rebuilds the database from the replica and writes it to `out-path`.
async fn restore_cmd(args: &[String]) {
    let out_path = &args[2];
    let client = local_client(&args[3]);
    let image = restore(&client).await.expect("restore");
    std::fs::write(out_path, &image).expect("write image");
    eprintln!("restored {} bytes -> {out_path}", image.len());
}

async fn replicate(args: &[String]) {
    let db_path = args[2].clone();
    let replica_dir = args[3].clone();

    let client = local_client(&replica_dir);
    let db = Db::open(&db_path).expect("open db");
    // Litestream's default cascade: L1@30s, L2@5m, L3@1h + snapshots + retention.
    let mut driver = Driver::new(syncer(db, client).await, CompactionLevels::default_levels());

    eprintln!(
        "literstream replicating {db_path} -> {replica_dir} (pid {})",
        std::process::id(),
    );

    // Monitor once per second (litestream's MonitorInterval), draining on Ctrl-C.
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = driver.tick(SystemTime::now()).await {
                    eprintln!("tick error: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("shutting down, draining WAL...");
                if let Err(e) = driver.flush().await {
                    eprintln!("flush error: {e}");
                }
                break;
            }
        }
    }
}

async fn syncer(db: Db, client: ReplicaClient) -> Syncer {
    Syncer::open(db, client).await.expect("open syncer")
}
