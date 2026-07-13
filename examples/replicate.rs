//! Replicates a live SQLite database's WAL to a local LTX chain, then prints the
//! resulting files. Used by `scripts/cross-check-sync.sh` to feed the chain to
//! Go's `ltx apply`.
//!
//!     cargo run --example replicate -- <db-path> <replica-root>

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::Syncer;
use object_store::local::LocalFileSystem;
use rusqlite::Connection;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: {} <db-path> <replica-root>", args[0]);
        return ExitCode::from(2);
    }
    let db_path = std::path::PathBuf::from(&args[1]);
    let root = std::path::PathBuf::from(&args[2]);
    std::fs::create_dir_all(&root).expect("create replica root");

    let db = Db::open(&db_path).expect("open db");
    // A LocalFileSystem-backed replica lays LTX files out on disk exactly like a
    // remote store's keys, so the Go `ltx` cross-check can read them directly.
    let store = LocalFileSystem::new_with_prefix(&root).expect("local store");
    let client = ReplicaClient::new(Arc::new(store), "");
    let mut syncer = Syncer::open(db, client).await.expect("open syncer");

    // Application writer (separate connection).
    let w = Connection::open(&db_path).unwrap();
    w.busy_timeout(Duration::from_secs(5)).unwrap();
    w.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = w
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    w.execute_batch(
        "CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT, note TEXT)",
    )
    .unwrap();

    let insert = |lo: i64, hi: i64, note: &str| {
        w.execute_batch("BEGIN").unwrap();
        let mut stmt = w
            .prepare("INSERT INTO items(id,name,note) VALUES (?1,?2,?3)")
            .unwrap();
        for i in lo..hi {
            stmt.execute(rusqlite::params![i, format!("item-{i:04}"), note])
                .unwrap();
        }
        drop(stmt);
        w.execute_batch("COMMIT").unwrap();
    };

    insert(1, 101, "first");
    println!("sync 1: {:?}", syncer.sync().await.unwrap());
    insert(101, 201, "second");
    println!("sync 2: {:?}", syncer.sync().await.unwrap());
    w.execute_batch("BEGIN; UPDATE items SET note='updated' WHERE id<=10; COMMIT;")
        .unwrap();
    println!("sync 3: {:?}", syncer.sync().await.unwrap());

    println!("replica dir: {}", root.join("ltx/0").display());
    let mut files: Vec<_> = std::fs::read_dir(root.join("ltx/0"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect();
    files.sort();
    for f in files {
        println!("  {f}");
    }
    ExitCode::SUCCESS
}
