//! Peak-memory ceiling for the one-shot snapshot + restore paths.
//!
//! This is the counterpart to `scripts/bench/run.sh`, which measures the
//! *steady-state replication* daemon's RSS over a live workload. Here we pin
//! down the *one-shot* peak that a large database used to blow up:
//! `build_snapshot` once slurped the whole DB (`fs::read`) and `restore` built
//! the whole image in a `Vec<u8>`. Both are now O(page_size) on the database
//! side, so peak RSS must stay far below the database size.
//!
//! Ignored by default (it builds a multi-hundred-MB fixture). Run it with:
//!
//! ```sh
//! cargo test --test restore_memory -- --ignored --nocapture
//! # tune: LITERSTREAM_RSS_MB (fixture size) LITERSTREAM_RSS_CEIL_MB (ceiling)
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{Syncer, restore_to_path};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use rusqlite::Connection;

/// Peak resident set size of this process so far, in bytes. `ru_maxrss` is
/// reported in bytes on macOS and in kibibytes on Linux.
fn peak_rss_bytes() -> u64 {
    // SAFETY: getrusage fills a plain POD struct; RUSAGE_SELF is always valid.
    let maxrss = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru.ru_maxrss as u64
    };
    if cfg!(target_os = "macos") {
        maxrss
    } else {
        maxrss * 1024
    }
}

fn env_mb(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Fills `path` with a WAL-mode database of at least `target_bytes`, using a
/// highly compressible payload (so the *replica* stays small while the
/// *decompressed* image is large — the gap this test exercises), then folds it
/// all into the main file with a TRUNCATE checkpoint.
fn build_fixture(path: &Path, target_bytes: u64) {
    let c = Connection::open(path).unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = c
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    c.execute_batch("CREATE TABLE blobs(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();

    let payload = vec![b'z'; 2048]; // compresses to almost nothing under LZ4
    let mut id: i64 = 0;
    loop {
        c.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = c
                .prepare("INSERT INTO blobs(id, payload) VALUES (?1, ?2)")
                .unwrap();
            for _ in 0..2000 {
                id += 1;
                stmt.execute(rusqlite::params![id, &payload]).unwrap();
            }
        }
        c.execute_batch("COMMIT").unwrap();
        if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
            + std::fs::metadata(wal_path(path)).map(|m| m.len()).unwrap_or(0)
            >= target_bytes
        {
            break;
        }
    }

    // Fold everything into the main DB file so build_snapshot reads it via pread
    // (not from the WAL), and empty the WAL.
    c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    c.close().unwrap();
}

fn wal_path(path: &Path) -> PathBuf {
    let mut p = path.to_path_buf().into_os_string();
    p.push("-wal");
    PathBuf::from(p)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "builds a large fixture; run explicitly with --ignored"]
async fn snapshot_and_restore_stay_under_a_memory_ceiling() {
    let size_mb = env_mb("LITERSTREAM_RSS_MB", 256);
    let ceil_mb = env_mb("LITERSTREAM_RSS_CEIL_MB", 128);
    let target = size_mb * 1024 * 1024;

    let dir = std::env::temp_dir().join(format!("literstream-rss-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("big.db");

    build_fixture(&db_path, target);
    let db_bytes = std::fs::metadata(&db_path).unwrap().len();
    assert!(
        db_bytes >= target,
        "fixture is {db_bytes} bytes, expected >= {target}"
    );

    let client = ReplicaClient::new(Arc::new(InMemory::new()), "");

    // Snapshot the large DB (exercises build_snapshot's page-at-a-time pread).
    let db = Db::open(&db_path).unwrap();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();
    syncer.sync().await.unwrap();
    // Rebuild the L9 snapshot from the chain (exercises the streaming k-way merge
    // in snapshot(), which must not materialize the whole image either).
    syncer.snapshot().await.unwrap();
    drop(syncer);

    // Restore straight to disk (exercises restore_to_path's streamed pwrite).
    let out = dir.join("restored.db");
    restore_to_path(&client, &out).await.unwrap();

    let peak_mb = peak_rss_bytes() as f64 / (1024.0 * 1024.0);
    eprintln!(
        "database {} MB, peak RSS {:.1} MB (ceiling {} MB)",
        db_bytes / (1024 * 1024),
        peak_mb,
        ceil_mb
    );

    // Correctness: the restored file is a valid database with every row.
    let restored = Connection::open(&out).unwrap();
    let integrity: String = restored
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let src = Connection::open(&db_path).unwrap();
    let want: i64 = src
        .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    let got: i64 = restored
        .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(got, want, "restored row count differs from source");

    assert!(
        peak_mb < ceil_mb as f64,
        "peak RSS {peak_mb:.1} MB exceeded the {ceil_mb} MB ceiling for a {} MB database \
         — a snapshot/restore path is buffering the whole image",
        db_bytes / (1024 * 1024)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fills `buf` with high-entropy bytes seeded by `id` (an LCG), so the encoded
/// LTX barely compresses — forcing a *large* staged snapshot and thus the
/// multipart upload path.
fn incompressible(id: i64, buf: &mut [u8]) {
    let mut x = (id as u64) ^ 0x9E37_79B9_7F4A_7C15;
    for b in buf.iter_mut() {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *b = (x >> 33) as u8;
    }
}

/// The multipart upload path (large, incompressible snapshot) must stream in
/// fixed chunks, not buffer the whole file. Uses a disk-backed replica so the
/// uploaded bytes don't count against our RSS (an in-memory store would hold the
/// whole object). Needs free disk for ~2× the fixture size (DB + replica copy).
#[tokio::test(flavor = "current_thread")]
#[ignore = "builds a large incompressible fixture; needs disk headroom; run with --ignored"]
async fn snapshot_upload_streams_under_a_memory_ceiling() {
    let size_mb = env_mb("LITERSTREAM_MP_MB", 64);
    let ceil_mb = env_mb("LITERSTREAM_MP_CEIL_MB", 40);
    let target = size_mb * 1024 * 1024;

    let dir = std::env::temp_dir().join(format!("literstream-mp-rss-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("big.db");

    // Build an *incompressible* fixture so the staged snapshot exceeds the
    // multipart threshold (16 MiB).
    {
        let c = Connection::open(&db_path).unwrap();
        c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        let _: String = c
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .unwrap();
        c.execute_batch("CREATE TABLE blobs(id INTEGER PRIMARY KEY, payload BLOB)")
            .unwrap();
        let mut payload = vec![0u8; 2048];
        let mut id: i64 = 0;
        loop {
            c.execute_batch("BEGIN").unwrap();
            {
                let mut stmt = c
                    .prepare("INSERT INTO blobs(id, payload) VALUES (?1, ?2)")
                    .unwrap();
                for _ in 0..2000 {
                    id += 1;
                    incompressible(id, &mut payload);
                    stmt.execute(rusqlite::params![id, &payload]).unwrap();
                }
            }
            c.execute_batch("COMMIT").unwrap();
            // autocheckpoint is off, so the data sits in the -wal file until the
            // TRUNCATE below — count both, or this loop never reaches `target`.
            let db_len = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
            let wal_len = std::fs::metadata(wal_path(&db_path))
                .map(|m| m.len())
                .unwrap_or(0);
            if db_len + wal_len >= target {
                break;
            }
        }
        c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        c.close().unwrap();
    }
    let db_bytes = std::fs::metadata(&db_path).unwrap().len();

    // Disk-backed replica: uploaded bytes go to disk, not our RSS.
    let replica = dir.join("replica");
    std::fs::create_dir_all(&replica).unwrap();
    let client = ReplicaClient::new(Arc::new(LocalFileSystem::new_with_prefix(&replica).unwrap()), "");

    let db = Db::open(&db_path).unwrap();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();
    syncer.sync().await.unwrap(); // snapshot: staged large -> multipart upload

    let peak_mb = peak_rss_bytes() as f64 / (1024.0 * 1024.0);
    eprintln!(
        "database {} MB (incompressible), peak RSS {:.1} MB (ceiling {} MB)",
        db_bytes / (1024 * 1024),
        peak_mb,
        ceil_mb
    );

    assert!(
        peak_mb < ceil_mb as f64,
        "peak RSS {peak_mb:.1} MB exceeded the {ceil_mb} MB ceiling uploading a {} MB snapshot \
         — the multipart upload is buffering the whole file",
        db_bytes / (1024 * 1024)
    );

    let _ = std::fs::remove_dir_all(&dir);
}
