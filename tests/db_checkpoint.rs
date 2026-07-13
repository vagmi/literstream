//! Phase 2 tests: the SQLite control model — WAL mode, disabled autocheckpoint,
//! manual checkpoints, and the pinned read lock that protects the WAL.
//!
//! Writes go through the managed connection (`db.connection()`), which inherits
//! `wal_autocheckpoint=0`, so these tests isolate literstream's own behavior.

use std::path::PathBuf;
use std::time::Duration;

use literstream::db::{CheckpointMode, Db};
use rusqlite::Connection;

fn temp_db(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("literstream-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}-{}.db", std::process::id(), tag));
    remove_db(&path);
    path
}

fn remove_db(path: &PathBuf) {
    for suffix in ["", "-wal", "-shm"] {
        let mut p = path.clone().into_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(p);
    }
}

/// Inserts `n` rows of 4 KB blobs through `conn` in a single transaction,
/// creating the table on first use.
fn write_rows(conn: &Connection, n: usize) {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS blobs(id INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO blobs(b) VALUES (randomblob(4000))")
            .unwrap();
        for _ in 0..n {
            stmt.execute([]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
}

#[test]
fn open_sets_wal_and_disables_autocheckpoint() {
    let path = temp_db("pragmas");
    let db = Db::open(&path).unwrap();

    assert_eq!(db.journal_mode().unwrap().to_lowercase(), "wal");
    assert_eq!(db.wal_autocheckpoint().unwrap(), 0);
    assert_eq!(db.seq().unwrap(), Some(0)); // _litestream_seq row exists

    drop(db);
    remove_db(&path);
}

#[test]
fn autocheckpoint_disabled_lets_wal_grow_past_1000_pages() {
    let path = temp_db("grow");
    let db = Db::open(&path).unwrap();

    // Well past SQLite's default 1000-page autocheckpoint threshold.
    write_rows(db.connection(), 1500);

    // Nothing drained the WAL, so it holds > 1000 frames.
    assert!(
        db.wal_frame_count() > 1000,
        "expected WAL to grow unbounded, got {} frames",
        db.wal_frame_count()
    );

    drop(db);
    remove_db(&path);
}

#[test]
fn passive_checkpoint_drains_then_truncate_empties() {
    let path = temp_db("checkpoint");
    let mut db = Db::open(&path).unwrap();
    write_rows(db.connection(), 300);
    assert!(db.wal_frame_count() > 0);

    // PASSIVE moves every frame into the DB (no other readers) but leaves the
    // -wal file in place.
    let r = db.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(r.fully_checkpointed(), "not fully checkpointed: {r:?}");
    assert!(db.wal_size() > 0, "PASSIVE should not truncate the file");

    // TRUNCATE empties the -wal file on disk.
    let r = db.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(!r.busy);
    assert_eq!(db.wal_size(), 0, "TRUNCATE should empty the -wal file");

    drop(db);
    remove_db(&path);
}

#[test]
fn read_lock_blocks_wal_reset() {
    let path = temp_db("readlock");
    let mut db = Db::open(&path).unwrap();
    write_rows(db.connection(), 200);
    assert!(db.wal_size() > 0);

    // Pin a read-mark.
    db.acquire_read_lock().unwrap();
    assert!(db.read_lock_held());

    // A different connection cannot truncate the WAL while the mark is held.
    let other = Connection::open(&path).unwrap();
    other.busy_timeout(Duration::from_millis(200)).unwrap();
    let (busy, _log, _ckpt): (i64, i64, i64) = other
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(busy, 1, "reader should block a TRUNCATE checkpoint");
    assert!(db.wal_size() > 0, "WAL must not be truncated while pinned");

    // Once the mark is released, the same checkpoint succeeds.
    db.release_read_lock().unwrap();
    let (busy, _log, _ckpt): (i64, i64, i64) = other
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(busy, 0, "checkpoint should succeed once the reader is gone");
    assert_eq!(db.wal_size(), 0);

    drop(other);
    drop(db);
    remove_db(&path);
}

#[test]
fn persist_wal_keeps_wal_file_after_last_connection_closes() {
    let path = temp_db("persist");
    let mut wal = path.clone().into_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);

    {
        let db = Db::open(&path).unwrap();
        write_rows(db.connection(), 50);
        assert!(db.wal_size() > 0);
        // Db is the only connection; dropping it closes the last connection.
    }

    // With SQLITE_FCNTL_PERSIST_WAL set, SQLite keeps the -wal file rather than
    // deleting it on last close.
    assert!(
        wal.exists(),
        "-wal should persist after the last connection closes"
    );

    remove_db(&path);
}
