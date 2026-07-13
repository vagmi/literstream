//! Phase 4 integration test against a real S3-compatible object store (Garage).
//!
//! Gated: bring up Garage and source its env first, then run explicitly:
//!
//!   ./scripts/garage-up.sh
//!   source docker/garage/.garage.env
//!   cargo test --test s3_garage -- --ignored --nocapture
//!
//! Without the `LITESTREAM_S3_*` env it prints a skip note and returns.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{SyncOutcome, Syncer, restore};
use object_store::aws::AmazonS3Builder;
use rusqlite::Connection;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn s3_client_from_env() -> Option<ReplicaClient> {
    let endpoint = std::env::var("LITESTREAM_S3_ENDPOINT").ok()?;
    let region = std::env::var("LITESTREAM_S3_REGION").ok()?;
    let bucket = std::env::var("LITESTREAM_S3_BUCKET").ok()?;
    let access = std::env::var("LITESTREAM_S3_ACCESS_KEY").ok()?;
    let secret = std::env::var("LITESTREAM_S3_SECRET").ok()?;

    let s3 = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(region)
        .with_bucket_name(bucket)
        .with_access_key_id(access)
        .with_secret_access_key(secret)
        .with_allow_http(true) // Garage speaks plain HTTP locally
        .with_virtual_hosted_style_request(false) // path-style
        .build()
        .ok()?;

    // Unique prefix per run so repeats start from a clean chain.
    let prefix = format!(
        "itest-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    Some(ReplicaClient::new(Arc::new(s3), prefix))
}

fn writer(path: &std::path::Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = c
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT, note TEXT)",
    )
    .unwrap();
    c
}

fn insert_range(c: &Connection, lo: i64, hi: i64, note: &str) {
    c.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = c
            .prepare("INSERT INTO items(id,name,note) VALUES (?1,?2,?3)")
            .unwrap();
        for i in lo..hi {
            stmt.execute(rusqlite::params![i, format!("item-{i:04}"), note])
                .unwrap();
        }
    }
    c.execute_batch("COMMIT").unwrap();
}

#[tokio::test]
#[ignore = "requires Garage: scripts/garage-up.sh + source docker/garage/.garage.env"]
async fn replicate_and_restore_over_garage_s3() {
    let Some(client) = s3_client_from_env() else {
        eprintln!("skipping: LITESTREAM_S3_* env not set (run scripts/garage-up.sh)");
        return;
    };

    let dir = std::env::temp_dir().join(format!("literstream-s3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    let db = Db::open(&db_path).unwrap();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&db_path);
    insert_range(&w, 1, 101, "first");
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Snapshot { txid: 1, .. }
    ));
    insert_range(&w, 101, 201, "second");
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Incremental { txid: 2, .. }
    ));
    w.execute_batch("BEGIN; UPDATE items SET note='updated' WHERE id<=10; COMMIT;")
        .unwrap();
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Incremental { txid: 3, .. }
    ));

    // The object store now holds three LTX objects.
    let listed = client.list_ltx(0).await.unwrap();
    assert_eq!(listed.len(), 3, "expected 3 LTX objects, got {listed:?}");

    // Restore from Garage and validate with SQLite.
    let image = restore(&client).await.unwrap();
    let restored = dir.join("restored.db");
    std::fs::write(&restored, &image).unwrap();
    let rc = Connection::open(&restored).unwrap();
    let integrity: String = rc
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    let count: i64 = rc
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    assert_eq!(count, 200);

    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "garage S3 round-trip ok: 200 rows restored from {} objects",
        listed.len()
    );
}
