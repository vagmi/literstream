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
//!     literstream replicate <db-path>  <replica-dir | s3://prefix | gs://bucket/prefix>
//!     literstream restore   <out-path> <replica-dir | s3://prefix | gs://bucket/prefix>

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{CompactionLevels, Driver, Syncer, restore};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
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
            eprintln!(
                "  {} replicate <db-path>  <replica-dir | s3://prefix | gs://bucket/prefix>",
                args[0]
            );
            eprintln!(
                "  {} restore   <out-path> <replica-dir | s3://prefix | gs://bucket/prefix>",
                args[0]
            );
            std::process::exit(2);
        }
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

/// Rebuilds the database from the replica and writes it to `out-path`.
async fn restore_cmd(args: &[String]) {
    let out_path = &args[2];
    let client = replica_client(&args[3]);
    let image = restore(&client).await.expect("restore");
    std::fs::write(out_path, &image).expect("write image");
    eprintln!("restored {} bytes -> {out_path}", image.len());
}

async fn replicate(args: &[String]) {
    let db_path = args[2].clone();
    let replica = args[3].clone();

    let client = replica_client(&replica);
    let db = Db::open(&db_path).expect("open db");
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
