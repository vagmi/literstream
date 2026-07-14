//! Phase 3–4 end-to-end tests: replicate a live SQLite database's WAL to an
//! in-memory object store, then restore and validate against SQLite itself.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use literstream::db::{CheckpointMode, Db};
use literstream::storage::ReplicaClient;
use literstream::sync::{
    ReplicaReader, SyncError, SyncOutcome, Syncer, restore, restore_to_timestamp, restore_to_txid,
};
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
    // Drop the syncer first: it holds the pinned read-mark, which (correctly)
    // blocks an external TRUNCATE checkpoint until released.
    drop(w);
    drop(syncer);
    {
        let c = Connection::open(&tc.db_path).unwrap();
        let (busy, _, _): (i64, i64, i64) = c
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(busy, 0, "checkpoint should complete once the syncer is gone");
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
    // Aggressive threshold so a checkpoint fires after the first batch.
    syncer.min_checkpoint_frames = 2;

    let w = writer(&tc.db_path);
    ensure_table(&w);

    insert_range(&w, 1, 201, "first");
    assert!(matches!(
        syncer.sync().await.unwrap(),
        SyncOutcome::Snapshot { txid: 1, .. }
    ));

    // Sync captured everything; now checkpoint (PASSIVE under a write lock, which
    // restarts the WAL into a new generation).
    let ckpt = syncer.checkpoint_if_needed().await.unwrap();
    assert!(matches!(ckpt, Some((CheckpointMode::Passive, _))));

    // New writes land in a fresh WAL generation (new salts). Because the
    // checkpoint ran under a write lock after a final sync, no frames were
    // checkpointed behind our back: the syncer takes the cheap
    // incremental-after-reset path, not a full re-snapshot.
    insert_range(&w, 201, 301, "second");
    let outcome = syncer.sync().await.unwrap();
    assert!(
        matches!(outcome, SyncOutcome::Incremental { .. }),
        "expected incremental after WAL reset, got {outcome:?}"
    );

    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 300);
}

#[tokio::test]
async fn compaction_bounds_the_chain_and_restore_still_works() {
    let tc = TempCase::new("compact");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);

    // 5 syncs -> snapshot(1) + incrementals(2..5) = 5 L0 files, 100 rows.
    for b in 0..5 {
        insert_range(&w, b * 20 + 1, b * 20 + 21, "batch");
        syncer.sync().await.unwrap();
    }
    assert_eq!(client.list_ltx(0).await.unwrap().len(), 5);

    // Compact: fold L0[1..4] into an L1 base, keep the newest L0 (txid 5).
    let info = syncer.compact().await.unwrap().unwrap();
    assert_eq!((info.min_txid, info.max_txid), (1, 4));
    assert_eq!(
        client.list_ltx(9).await.unwrap().len(),
        1,
        "one snapshot base"
    );
    assert_eq!(
        client.list_ltx(0).await.unwrap().len(),
        1,
        "only the kept head L0"
    );

    // Restore now uses the compacted base + the kept L0 and is still correct.
    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 100);

    // Syncing continues past compaction; restore stays correct.
    insert_range(&w, 101, 121, "after");
    syncer.sync().await.unwrap();
    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 120);
}

#[tokio::test]
async fn resumes_across_levels_after_compaction() {
    let tc = TempCase::new("resume");
    let client = memory_client();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    // First syncer: 4 syncs, then compact (L1[1..3] base + kept L0[4]).
    {
        let db = Db::open(&tc.db_path).unwrap();
        let mut s1 = Syncer::open(db, client.clone()).await.unwrap();
        for b in 0..4 {
            insert_range(&w, b * 20 + 1, b * 20 + 21, "x");
            s1.sync().await.unwrap();
        }
        s1.compact().await.unwrap().unwrap();
        // s1 (and its writer lock) drops here.
    }

    // A fresh syncer resumes from the kept L0 head across levels.
    let db2 = Db::open(&tc.db_path).unwrap();
    let mut s2 = Syncer::open(db2, client.clone()).await.unwrap();
    assert_eq!(s2.position_txid(), 4, "resumed at the kept L0 head");

    insert_range(&w, 81, 101, "y");
    assert!(matches!(
        s2.sync().await.unwrap(),
        SyncOutcome::Incremental { txid: 5, .. }
    ));

    let image = restore(&client).await.unwrap();
    let (integrity, count) = validate_image(&tc.dir, &image);
    assert_eq!(integrity, "ok");
    assert_eq!(count, 100);
}

#[tokio::test]
async fn direct_page_reads_match_full_restore() {
    let tc = TempCase::new("pageread");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);
    for b in 0..4 {
        insert_range(&w, b * 30 + 1, b * 30 + 31, "b");
        syncer.sync().await.unwrap();
    }
    syncer.compact().await.unwrap().unwrap();

    let full = restore(&client).await.unwrap();
    let ps = 4096usize;
    let n_pages = full.len() / ps;
    assert!(n_pages >= 2);

    // Read every page directly via the page index + ranged GETs.
    let mut reader = ReplicaReader::open(&client, None).await.unwrap();
    assert_eq!(reader.page_size(), 4096);
    for pgno in 1..=n_pages as u32 {
        let page = reader
            .read_page(pgno)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("page {pgno} should exist"));
        let start = (pgno as usize - 1) * ps;
        assert_eq!(
            &page[..],
            &full[start..start + ps],
            "page {pgno} differs from full restore"
        );
    }
    // A page past the database doesn't exist.
    assert!(
        reader
            .read_page(n_pages as u32 + 50)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn point_in_time_restore_by_txid_and_timestamp() {
    let tc = TempCase::new("pitr");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);

    // txid 1..4, cumulative 10/20/30/40 rows.
    for b in 0..4 {
        insert_range(&w, b * 10 + 1, b * 10 + 11, "b");
        syncer.sync().await.unwrap();
    }

    // Restore to an earlier transaction.
    let r = restore_to_txid(&client, 2).await.unwrap();
    assert_eq!(r.txid, 2);
    assert_eq!(validate_image(&tc.dir, &r.image), ("ok".into(), 20));

    // A future TXID snaps to the latest.
    assert_eq!(restore_to_txid(&client, 99).await.unwrap().txid, 4);

    // Timestamp bounds: far-future -> latest, epoch 0 -> nothing retained.
    assert_eq!(
        restore_to_timestamp(&client, i64::MAX).await.unwrap().txid,
        4
    );
    assert!(matches!(
        restore_to_timestamp(&client, 0).await,
        Err(SyncError::TxidTooOld { .. })
    ));

    // After compaction (L1[1..3] base), pre-3 points are gone; 3 is the base.
    syncer.compact().await.unwrap().unwrap();
    assert!(matches!(
        restore_to_txid(&client, 2).await,
        Err(SyncError::TxidTooOld { .. })
    ));
    let r = restore_to_txid(&client, 3).await.unwrap();
    assert_eq!(r.txid, 3);
    assert_eq!(validate_image(&tc.dir, &r.image), ("ok".into(), 30));
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

#[tokio::test]
async fn level_cascade_and_snapshot_restore_to_live_state() {
    let tc = TempCase::new("cascade");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);

    // 5 syncs -> 5 L0 files (snapshot(1) + incrementals 2..5), 100 rows.
    for b in 0..5 {
        insert_range(&w, b * 20 + 1, b * 20 + 21, "batch");
        syncer.sync().await.unwrap();
    }
    assert_eq!(client.list_ltx(0).await.unwrap().len(), 5);

    // Fold L0 -> L1 (a single window covering 1..5, min=1 since L0[1] is a snapshot).
    let l1 = syncer.compact_level(1).await.unwrap().unwrap();
    assert_eq!((l1.min_txid, l1.max_txid), (1, 5));
    // compact_level does not delete the source level (retention's job).
    assert_eq!(client.list_ltx(0).await.unwrap().len(), 5);
    assert_eq!(client.list_ltx(1).await.unwrap().len(), 1);

    // Fold L1 -> L2.
    let l2 = syncer.compact_level(2).await.unwrap().unwrap();
    assert_eq!((l2.min_txid, l2.max_txid), (1, 5));

    // A full snapshot at the snapshot level for the current position.
    let snap = syncer.snapshot().await.unwrap().unwrap();
    assert_eq!((snap.min_txid, snap.max_txid), (1, 5));
    assert_eq!(client.list_ltx(9).await.unwrap().len(), 1);
    // Re-snapshotting the same position is a no-op.
    assert!(syncer.snapshot().await.unwrap().is_none());

    // Restore stitches across levels and matches SQLite exactly.
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 100));

    // Compacting again with nothing new is a no-op.
    assert!(syncer.compact_level(1).await.unwrap().is_none());

    // More writes still sync and restore correctly on top of the cascade.
    insert_range(&w, 101, 121, "after");
    syncer.sync().await.unwrap();
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 120));
}

#[tokio::test]
async fn resume_reads_wal_state_from_l0_after_snapshot() {
    let tc = TempCase::new("resume-snap");
    let client = memory_client();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    // First syncer: sync a few times, cascade to L1/L2 and take a snapshot whose
    // max TXID ties the newest L0 — the case that must resolve WAL state from L0.
    {
        let db = Db::open(&tc.db_path).unwrap();
        let mut s1 = Syncer::open(db, client.clone()).await.unwrap();
        for b in 0..4 {
            insert_range(&w, b * 20 + 1, b * 20 + 21, "x");
            s1.sync().await.unwrap();
        }
        s1.compact_level(1).await.unwrap().unwrap();
        s1.snapshot().await.unwrap().unwrap();
    }

    // A fresh syncer must resume at the newest L0 head (txid 4), not the snapshot.
    let db2 = Db::open(&tc.db_path).unwrap();
    let mut s2 = Syncer::open(db2, client.clone()).await.unwrap();
    assert_eq!(s2.position_txid(), 4, "resumed at the newest L0 head");

    insert_range(&w, 81, 101, "y");
    assert!(matches!(
        s2.sync().await.unwrap(),
        SyncOutcome::Incremental { txid: 5, .. }
    ));
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 100));
}

#[tokio::test]
async fn retention_prunes_lower_levels_but_keeps_restore_correct() {
    use std::time::{Duration, SystemTime};

    let tc = TempCase::new("retention");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

    let w = writer(&tc.db_path);
    ensure_table(&w);

    // 6 syncs -> 6 L0 files, 120 rows.
    for b in 0..6 {
        insert_range(&w, b * 20 + 1, b * 20 + 21, "b");
        syncer.sync().await.unwrap();
    }
    // Cascade L0 -> L1 and take a full snapshot at the current position.
    syncer.compact_level(1).await.unwrap().unwrap();
    syncer.snapshot().await.unwrap().unwrap();
    assert_eq!(client.list_ltx(0).await.unwrap().len(), 6);
    assert_eq!(client.list_ltx(1).await.unwrap().len(), 1);
    assert_eq!(client.list_ltx(9).await.unwrap().len(), 1);

    // Use a far-future "now" so every existing file counts as expired; retention
    // still keeps the newest at each level and never breaks restore.
    let future = SystemTime::now() + Duration::from_secs(3600);

    // L0 retention: files folded into L1 and old are dropped; newest L0 kept.
    let removed = syncer
        .enforce_l0_retention(future, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(removed >= 1, "some L0 files should be pruned");
    assert_eq!(
        client.list_ltx(0).await.unwrap().len(),
        1,
        "only the newest L0 remains"
    );

    // Snapshot retention (with cascade prune of lower levels below the retained
    // snapshot boundary). One snapshot exists, so it is kept as the newest.
    let min_snap = syncer
        .enforce_snapshot_retention(future, Duration::from_secs(1), 3)
        .await
        .unwrap();
    assert_eq!(client.list_ltx(9).await.unwrap().len(), 1, "newest snapshot kept");
    assert!(min_snap == 0 || min_snap == 6);

    // Despite pruning, the latest restore is still byte-correct.
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 120));
}

#[tokio::test]
async fn driver_compacts_during_workload_and_preserves_pitr() {
    use literstream::sync::{CompactionLevel, CompactionLevels, Driver};
    use std::time::{Duration, UNIX_EPOCH};

    let tc = TempCase::new("driver");
    let db = Db::open(&tc.db_path).unwrap();
    let client = memory_client();
    let syncer = Syncer::open(db, client.clone()).await.unwrap();

    // Small intervals so a synthetic clock crosses them quickly. L0 retention is
    // disabled, so every synced txid stays individually restorable while the
    // cascade + snapshots run — the "compact during the workload, still PITR" case.
    let levels = CompactionLevels::new(vec![
        CompactionLevel { level: 0, interval: Duration::ZERO },
        CompactionLevel { level: 1, interval: Duration::from_secs(3) },
    ])
    .unwrap();
    let mut driver = Driver::new(syncer, levels)
        .with_snapshot_interval(Duration::from_secs(5))
        .with_snapshot_retention(Duration::from_secs(10_000))
        .with_l0_retention(Duration::ZERO);

    let w = writer(&tc.db_path);
    ensure_table(&w);

    let base = UNIX_EPOCH + Duration::from_secs(3600);
    let mut marks: Vec<(u64, i64)> = Vec::new(); // (txid, expected rows)
    let mut any_compaction = false;
    let mut any_snapshot = false;

    for i in 0..12u64 {
        insert_range(&w, i as i64 * 10 + 1, i as i64 * 10 + 11, "d");
        let report = driver.tick(base + Duration::from_secs(i)).await.unwrap();
        any_compaction |= !report.compactions.is_empty();
        any_snapshot |= report.snapshot.is_some();
        if i == 2 || i == 6 || i == 9 {
            marks.push((driver.syncer().position_txid(), (i as i64 + 1) * 10));
        }
    }
    driver.flush().await.unwrap();

    assert!(any_compaction, "level compaction should fire during the workload");
    assert!(any_snapshot, "a snapshot should be taken during the workload");

    // Latest restore is correct.
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 120));

    // Every mid-history mark is still restorable *exactly* — compaction ran but
    // the (disabled) L0 retention kept fine granularity.
    for (txid, rows) in &marks {
        let r = restore_to_txid(&client, *txid).await.unwrap();
        assert_eq!(r.txid, *txid);
        assert_eq!(validate_image(&tc.dir, &r.image), ("ok".into(), *rows));
    }

    // Now slide the retention window: evict L0 below the last mark. Earlier marks
    // become unrecoverable (folded into coarser levels), the latest still works —
    // the granularity/storage tradeoff, now bounded by retention rather than by
    // compaction destroying history.
    let (last_txid, _) = *marks.last().unwrap();
    driver
        .syncer()
        .enforce_retention_by_txid(0, last_txid)
        .await
        .unwrap();

    // Before eviction the early mark restored *exactly* (asserted above). Now its
    // L0 file is gone, so a restore to that TXID snaps down to a coarser level
    // boundary (or errors if it predates every retained file) — granularity is
    // lost, but the latest image stays byte-correct.
    let (early_txid, _) = marks[0];
    match restore_to_txid(&client, early_txid).await {
        Ok(r) => assert!(
            r.txid < early_txid,
            "expected coarser granularity after eviction, still got exact txid {}",
            r.txid
        ),
        Err(SyncError::TxidTooOld { .. }) => {}
        Err(e) => panic!("unexpected restore error: {e}"),
    }
    let image = restore(&client).await.unwrap();
    assert_eq!(validate_image(&tc.dir, &image), ("ok".into(), 120));
}
