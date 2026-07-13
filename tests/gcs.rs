//! Phase 4/5 integration test against Google Cloud Storage — the primary target
//! and a backend that natively enforces conditional writes (if-generation-match),
//! so the CAS equivocation guard is fully effective here.
//!
//! Gated: set credentials + bucket, then run explicitly:
//!
//!   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
//!   export LITESTREAM_GCS_BUCKET=your-bucket
//!   cargo test --test gcs -- --ignored --nocapture
//!
//! Without `LITESTREAM_GCS_BUCKET` it prints a skip note and returns.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use literstream::db::Db;
use literstream::storage::{PutOutcome, ReplicaClient};
use literstream::sync::{ReplicaReader, SyncOutcome, Syncer, restore, restore_to_txid};
use object_store::gcp::GoogleCloudStorageBuilder;
use rusqlite::Connection;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn gcs_client_from_env() -> Option<ReplicaClient> {
    let bucket = std::env::var("LITESTREAM_GCS_BUCKET").ok()?;
    // Reads GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SERVICE_ACCOUNT from the env.
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .ok()?;
    let prefix = format!(
        "itest-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    Some(ReplicaClient::new(Arc::new(gcs), prefix))
}

#[tokio::test]
#[ignore = "requires GCS: GOOGLE_APPLICATION_CREDENTIALS + LITESTREAM_GCS_BUCKET"]
async fn cas_guard_enforced_on_gcs() {
    let Some(client) = gcs_client_from_env() else {
        eprintln!("skipping: LITESTREAM_GCS_BUCKET not set");
        return;
    };

    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
            .await
            .unwrap(),
        PutOutcome::AlreadyIdentical,
        "GCS must enforce if-generation-match so re-create is caught"
    );
    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"beta"))
            .await
            .unwrap(),
        PutOutcome::Conflict
    );
    assert_eq!(
        client.get_ltx(0, 1, 1).await.unwrap(),
        Bytes::from_static(b"alpha")
    );
    let _ = client.delete_ltx(0, 1, 1).await;
    println!("gcs CAS guard ok");
}

#[tokio::test]
#[ignore = "requires GCS: GOOGLE_APPLICATION_CREDENTIALS + LITESTREAM_GCS_BUCKET"]
async fn replicate_and_restore_over_gcs() {
    let Some(client) = gcs_client_from_env() else {
        eprintln!("skipping: LITESTREAM_GCS_BUCKET not set");
        return;
    };

    let dir = std::env::temp_dir().join(format!("literstream-gcs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    let db = Db::open(&db_path).unwrap();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = Connection::open(&db_path).unwrap();
    w.busy_timeout(Duration::from_secs(5)).unwrap();
    w.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = w
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    w.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();

    for batch in 0..3 {
        w.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = w.prepare("INSERT INTO items(note) VALUES (?1)").unwrap();
            for _ in 0..50 {
                stmt.execute(rusqlite::params![format!("batch-{batch}")])
                    .unwrap();
            }
        }
        w.execute_batch("COMMIT").unwrap();
        let outcome = syncer.sync().await.unwrap();
        assert!(matches!(
            outcome,
            SyncOutcome::Snapshot { .. } | SyncOutcome::Incremental { .. }
        ));
    }

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
    assert_eq!(count, 150);

    // Point-in-time restore to the first transaction (50 rows).
    let pit = restore_to_txid(&client, 1).await.unwrap();
    assert_eq!(pit.txid, 1);

    // Direct page reads via ranged GETs (proves get_range/head work on GCS).
    let ps = 4096usize;
    let mut reader = ReplicaReader::open(&client, None).await.unwrap();
    for pgno in 1..=(image.len() / ps) as u32 {
        let page = reader.read_page(pgno).await.unwrap().expect("page exists");
        let start = (pgno as usize - 1) * ps;
        assert_eq!(
            &page[..],
            &image[start..start + ps],
            "gcs page {pgno} differs"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
    println!("gcs round-trip + PITR + direct page reads ok: 150 rows");
}
