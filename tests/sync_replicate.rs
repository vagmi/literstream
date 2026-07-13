//! Phase 3–4 end-to-end tests: replicate a live SQLite database's WAL to an
//! in-memory object store, then restore and validate against SQLite itself.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use literstream::db::{CheckpointMode, Db};
use literstream::storage::ReplicaClient;
use literstream::sync::{SyncError, SyncOutcome, Syncer, restore};
use object_store::memory::InMemory;
use rusqlite::Connection;

fn memory_client() -> ReplicaClient {
    ReplicaClient::new(Arc::new(InMemory::new()), "")
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

struct TempCase {
    dir: PathBuf,
    db_path: PathBuf,
}

impl TempCase {
    fn new(tag: &str) -> TempCase {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "literstream-sync-{}-{}-{}",
            std::process::id(),
            tag,
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempCase {
            db_path: dir.join("app.db"),
            dir,
        }
    }
}

impl Drop for TempCase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A separate application writer connection (WAL, no autocheckpoint).
fn writer(path: &PathBuf) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = c
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    c
}

fn ensure_table(c: &Connection) {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT, note TEXT)",
    )
    .unwrap();
}

fn insert_range(c: &Connection, lo: i64, hi: i64, note: &str) {
    c.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = c
            .prepare("INSERT INTO items(id, name, note) VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in lo..hi {
            stmt.execute(rusqlite::params![i, format!("item-{i:04}"), note])
                .unwrap();
        }
    }
    c.execute_batch("COMMIT").unwrap();
}

/// Reads the row count / integrity of a raw database image via a throwaway file.
fn validate_image(dir: &PathBuf, image: &[u8]) -> (String, i64) {
    let p = dir.join("restored.db");
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(dir.join("restored.db-wal"));
    std::fs::write(&p, image).unwrap();
    let c = Connection::open(&p).unwrap();
    let integrity: String = c
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    let count: i64 = c
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap();
    (integrity, count)
}

#[tokio::test]
async fn snapshot_then_incrementals_restore_to_live_state() {
    let tc = TempCase::new("chain");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);

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

    // Rewrite existing rows (touches earlier pages -> dedup keeps latest).
    w.execute_batch("BEGIN; UPDATE items SET note='updated' WHERE id<=10; COMMIT;")
        .unwrap();
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Incremental { txid: 3, .. }
    ));

    // Nothing new -> skip.
    assert_eq!(syncer.sync().await.unwrap(), SyncOutcome::Skipped);

    // Restore and validate the reconstructed image with SQLite.
    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 200);

    // And it must byte-match the live database once checkpointed into its file.
    drop(w);
    {
        let c = Connection::open(&tc.db_path).unwrap();
        let _: (i64, i64, i64) = c
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
    }
    let live = std::fs::read(&tc.db_path).unwrap();
    assert_eq!(
        image, live,
        "restored image should equal the checkpointed live database"
    );
}

#[tokio::test]
async fn survives_truncate_checkpoint_and_new_generation() {
    let tc = TempCase::new("checkpoint");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();
    // Aggressive thresholds so a checkpoint fires after the first batch.
    syncer.min_checkpoint_frames = 2;
    syncer.truncate_frames = 2;

    let w = writer(&tc.db_path);
    ensure_table(&w);

    insert_range(&w, 1, 201, "first");
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Snapshot { txid: 1, .. }
    ));

    // Sync captured everything; now truncate the WAL (new generation follows).
    let ckpt = syncer.checkpoint_if_needed().unwrap();
    assert!(matches!(ckpt, Some((CheckpointMode::Truncate, _))));
    assert_eq!(syncer.db().wal_size(), 0);

    // New writes land in a fresh WAL generation (new salts).
    insert_range(&w, 201, 301, "second");
    let outcome = syncer.sync().await.unwrap();
    assert!(
        matches!(outcome, SyncOutcome::Incremental { .. }),
        "expected incremental after checkpoint, got {outcome:?}"
    );

    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 300);
}

#[tokio::test]
async fn equivocation_is_detected_on_upload() {
    let tc = TempCase::new("equiv");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);
    insert_range(&w, 1, 51, "first");
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Snapshot { txid: 1, .. }
    ));

    // Simulate another writer that already wrote a *different* LTX at txid 2.
    client
        .put_ltx(0, 2, 2, bytes::Bytes::from_static(b"a different ltx"))
        .await
        .unwrap();

    insert_range(&w, 51, 101, "second");
    let err = syncer.sync().await.unwrap_err();
    assert!(
        matches!(err, SyncError::Equivocation { txid: 2 }),
        "expected equivocation at txid 2, got {err:?}"
    );
}

#[tokio::test]
async fn single_writer_lock_rejects_a_second_syncer() {
    let tc = TempCase::new("writerlock");

    let db1 = Db::open(&tc.db_path).unwrap();
    let _s1 = Syncer::open(db1, memory_client()).await.unwrap();

    // A second syncer on the same database cannot acquire the writer lock.
    let db2 = Db::open(&tc.db_path).unwrap();
    let result = Syncer::open(db2, memory_client()).await;
    assert!(
        matches!(result, Err(SyncError::Lock(_))),
        "expected a lock error from the second syncer"
    );
}
