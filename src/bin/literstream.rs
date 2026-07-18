//! `literstream` — a minimal continuous-replication daemon that mirrors
//! litestream's `replicate` behaviour, for benchmarking against the real Go
//! litestream binary.
//!
//! The replica is a **local directory** (an `object_store` filesystem backend,
//! equivalent to litestream's `file` replica type), an S3 bucket addressed as
//! `s3://<prefix>` (endpoint/bucket/credentials from the `LITESTREAM_S3_*`
//! environment — Garage-compatible), or a GCS bucket addressed as
//! `gs://<bucket>/<prefix>` (credentials from `GOOGLE_APPLICATION_CREDENTIALS`
//! or `GOOGLE_SERVICE_ACCOUNT`). It opens the database literstream's way and
//! runs the [`Driver`] once per second — sync, checkpoint, tiered compaction,
//! snapshots, retention — until Ctrl-C, then drains.
//!
//!     literstream replicate <db-path>  <replica>
//!     literstream restore   [--txid N | --timestamp T] <replica> <out-path>
//!
//! where `<replica>` is `<dir> | s3://<prefix> | gs://<bucket>/<prefix>`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::{Parser, Subcommand};
use jiff::Timestamp;
use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{
    CompactionLevels, Driver, Syncer, restore_to_path, restore_to_timestamp, restore_to_txid,
};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;

/// Continuous SQLite replication to object storage, and restore.
#[derive(Parser)]
#[command(name = "literstream", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Continuously replicate a live database to a replica until Ctrl-C.
    Replicate {
        /// Path to the SQLite database to replicate.
        db_path: String,
        /// Replica destination: a directory, `s3://<prefix>`, or `gs://<bucket>/<prefix>`.
        replica: String,
    },
    /// Rebuild a database from a replica (optionally at a point in time).
    Restore {
        /// Restore the state as of this transaction ID (point-in-time recovery).
        #[arg(long, conflicts_with = "timestamp")]
        txid: Option<u64>,
        /// Restore the state as of this time: an RFC 3339 datetime
        /// (e.g. `2026-07-16T10:30:00Z`) or Unix epoch milliseconds.
        #[arg(long)]
        timestamp: Option<String>,
        /// Replica source: a directory, `s3://<prefix>`, or `gs://<bucket>/<prefix>`.
        replica: String,
        /// Path to write the reconstructed database to.
        out_path: String,
    },
}

// A single-threaded runtime: the replicator is one I/O-bound task, so worker
// threads would only add baseline memory.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    match Cli::parse().command {
        Command::Replicate { db_path, replica } => replicate(&db_path, &replica).await,
        Command::Restore {
            txid,
            timestamp,
            replica,
            out_path,
        } => restore_cmd(txid, timestamp.as_deref(), &replica, &out_path).await,
    }
}

/// A replica client over a local directory, an S3 bucket (`s3://<prefix>`, with
/// endpoint/bucket/credentials from `LITESTREAM_S3_*`), or a GCS bucket
/// (`gs://<bucket>/<prefix>`, credentials from the `GOOGLE_*` environment).
fn replica_client(replica: &str) -> ReplicaClient {
    if let Some(rest) = replica.strip_prefix("gs://") {
        // Unlike the s3:// form (bucket from the env), the bucket rides in the
        // URL, so one env applies to any bucket: gs://<bucket>[/<prefix>].
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        assert!(!bucket.is_empty(), "gs:// replica needs a bucket name");
        // Reads GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SERVICE_ACCOUNT.
        let gcs = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("build gcs client");
        ReplicaClient::new(Arc::new(gcs), prefix.to_string())
    } else if let Some(prefix) = replica.strip_prefix("s3://") {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("missing env {k}"));
        let s3 = AmazonS3Builder::new()
            .with_endpoint(env("LITESTREAM_S3_ENDPOINT"))
            .with_region(env("LITESTREAM_S3_REGION"))
            .with_bucket_name(env("LITESTREAM_S3_BUCKET"))
            .with_access_key_id(env("LITESTREAM_S3_ACCESS_KEY"))
            .with_secret_access_key(env("LITESTREAM_S3_SECRET"))
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false) // path-style (Garage)
            .build()
            .expect("build s3 client");
        ReplicaClient::new(Arc::new(s3), prefix.to_string())
    } else {
        std::fs::create_dir_all(replica).expect("create replica dir");
        let store = LocalFileSystem::new_with_prefix(replica).expect("open local store");
        ReplicaClient::new(Arc::new(store), "")
    }
}

/// Parses `--timestamp`: an RFC 3339 datetime, or a bare integer read as Unix
/// epoch milliseconds. Returns milliseconds since the Unix epoch.
fn parse_timestamp_ms(s: &str) -> i64 {
    if let Ok(ms) = s.parse::<i64>() {
        return ms;
    }
    s.parse::<Timestamp>()
        .unwrap_or_else(|e| panic!("invalid --timestamp {s:?}: {e} (want RFC 3339 or epoch ms)"))
        .as_millisecond()
}

/// Rebuilds the database from the replica and writes it to `out_path`. With
/// `--txid`/`--timestamp` it reconstructs an earlier point in time; otherwise it
/// streams the latest state straight to disk with bounded memory.
async fn restore_cmd(txid: Option<u64>, timestamp: Option<&str>, replica: &str, out_path: &str) {
    let client = replica_client(replica);
    match (txid, timestamp) {
        (Some(txid), _) => {
            let result = restore_to_txid(&client, txid).await.expect("restore");
            std::fs::write(out_path, &result.image).expect("write image");
            eprintln!(
                "restored {} bytes as of txid {} -> {out_path}",
                result.image.len(),
                result.txid,
            );
        }
        (None, Some(ts)) => {
            let ms = parse_timestamp_ms(ts);
            let result = restore_to_timestamp(&client, ms).await.expect("restore");
            std::fs::write(out_path, &result.image).expect("write image");
            eprintln!(
                "restored {} bytes as of txid {} (<= {ts}) -> {out_path}",
                result.image.len(),
                result.txid,
            );
        }
        (None, None) => {
            let path = std::path::Path::new(out_path);
            let txid = restore_to_path(&client, path).await.expect("restore");
            eprintln!("restored latest (txid {txid}) -> {out_path}");
        }
    }
}

async fn replicate(db_path: &str, replica: &str) {
    let client = replica_client(replica);
    let db = Db::open(db_path).expect("open db");
    // Litestream's default cascade: L1@30s, L2@5m, L3@1h + snapshots + retention.
    let mut driver = Driver::new(syncer(db, client).await, CompactionLevels::default_levels());

    eprintln!(
        "literstream replicating {db_path} -> {replica} (pid {})",
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
