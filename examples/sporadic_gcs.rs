//! Sporadic-write correctness probe against a real GCS backend, single database.
//!
//! It writes one row at a time with idle ticks in between (so the time-based
//! checkpoint fires and folds the WAL without a seq bump, exactly the low-write
//! path), then drains, restores the replica to a SECOND local file, and diffs it
//! against the live database row-by-row. If any write, especially the last one,
//! failed to replicate, the diff prints it.
//!
//! Run:
//!   GCS_BUCKET=literstream-test-bucket cargo run --release --example sporadic_gcs
//! Tunables: SPORADIC_WRITES, SPORADIC_CHECKPOINT_S, SPORADIC_IDLE_TICKS,
//!   SPORADIC_SNAPSHOT_S, SPORADIC_TAIL_TICKS (ticks after the LAST write before draining).

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{CompactionLevels, Driver, Syncer, restore_to_path};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as OsPath};
use futures::StreamExt;
use rusqlite::Connection;

fn envu(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn values(path: &std::path::Path) -> Vec<String> {
    let c = Connection::open(path).unwrap();
    let mut stmt = c.prepare("SELECT v FROM t ORDER BY id").unwrap();
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

#[tokio::main]
async fn main() {
    let writes = envu("SPORADIC_WRITES", 6);
    let checkpoint_s = envu("SPORADIC_CHECKPOINT_S", 2);
    let idle_ticks = envu("SPORADIC_IDLE_TICKS", 3);
    let snapshot_s = envu("SPORADIC_SNAPSHOT_S", 0); // 0 = disabled
    let tail_ticks = envu("SPORADIC_TAIL_TICKS", 1); // ticks after the last write
    let bucket = std::env::var("GCS_BUCKET").expect("set GCS_BUCKET");
    let prefix = format!("sporadic-{}", std::process::id());

    let store: Arc<dyn ObjectStore> =
        Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket).build().unwrap());
    let client = ReplicaClient::new(store.clone(), prefix.clone());

    let dir = std::env::temp_dir().join(format!("literstream-sporadic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    // Long-lived application writer, as in production.
    let w = Connection::open(&db_path).unwrap();
    w.busy_timeout(Duration::from_secs(5)).unwrap();
    w.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = w.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).unwrap();
    w.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    let db = Db::open(&db_path).unwrap();
    let syncer = Syncer::open(db, client.clone()).await.unwrap();
    let mut driver = Driver::new(syncer, CompactionLevels::default())
        .with_checkpoint_interval(Duration::from_secs(checkpoint_s))
        .with_snapshot_interval(Duration::from_secs(snapshot_s));

    println!(
        "gs://{bucket}/{prefix} | {writes} sporadic writes | checkpoint {checkpoint_s}s | \
         {idle_ticks} idle ticks between writes | {tail_ticks} tail ticks after the last\n"
    );

    let base = UNIX_EPOCH + Duration::from_secs(100_000);
    let mut clock: u64 = 0;

    for i in 0..writes {
        w.execute("INSERT INTO t(v) VALUES (?1)", rusqlite::params![format!("write-{i}")]).unwrap();
        // For the LAST write, tick only `tail_ticks` times (stress the tail);
        // otherwise idle for `idle_ticks` to let the time-based checkpoint fold.
        let ticks = if i == writes - 1 { tail_ticks } else { idle_ticks };
        let mut outcomes = Vec::new();
        for _ in 0..ticks {
            clock += checkpoint_s + 1;
            let r = driver.tick(base + Duration::from_secs(clock)).await.unwrap();
            outcomes.push(format!("{:?}{}", r.synced, if r.checkpoint.is_some() { "+ckpt" } else { "" }));
        }
        let l0 = client.list_ltx(0).await.unwrap();
        println!(
            "write-{i}: ticks [{}] | replica L0 = {:?}",
            outcomes.join(", "),
            l0.iter().map(|f| (f.min_txid, f.max_txid)).collect::<Vec<_>>()
        );
    }

    // Drain everything still pending.
    let flushed = driver.flush().await.unwrap();
    println!("\nflush() shipped {flushed} more file(s)");

    // Restore the replica to a SECOND file and diff against the live database.
    let out = dir.join("restored.db");
    let txid = restore_to_path(&client, &out).await.unwrap();
    let src = values(&db_path);
    let got = values(&out);
    println!("\nrestored to txid {txid}");
    println!("source   ({:>2} rows): {:?}", src.len(), src);
    println!("restored ({:>2} rows): {:?}", got.len(), got);
    if src == got {
        println!("\nRESULT: OK — every write, including the last, replicated.");
    } else {
        let missing: Vec<_> = src.iter().filter(|v| !got.contains(v)).collect();
        println!("\nRESULT: MISMATCH — missing from the replica: {missing:?}");
    }

    // Cleanup GCS + local.
    let mut s = store.list(Some(&OsPath::from(prefix.as_str())));
    let mut n = 0;
    while let Some(m) = s.next().await {
        if let Ok(m) = m {
            let _ = store.delete(&m.location).await;
            n += 1;
        }
    }
    println!("cleanup: removed {n} objects");
    let _ = std::fs::remove_dir_all(&dir);
}
