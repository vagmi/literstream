//! 00 — simple usage: replicate a live SQLite database and restore it.
//!
//!     cargo run --example 00_simple_usage
//!
//! The whole thing runs against a throwaway database in the system temp dir and
//! an *in-memory* replica, so there's nothing to configure. The five steps are
//! the same ones you'd use against S3 or GCS — only the object store changes.

use std::sync::Arc;
use std::time::Duration;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{Syncer, restore};
use object_store::memory::InMemory;
use rusqlite::Connection;

#[tokio::main]
async fn main() {
    // A real on-disk database in a throwaway directory.
    let dir = std::env::temp_dir().join(format!("literstream-00-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    // 1. Open the database. literstream puts it in WAL mode and disables SQLite's
    //    own autocheckpoint so that *it* owns checkpointing.
    let db = Db::open(&db_path).expect("open db");

    // 2. Point at a replica. Any `object_store` backend works; here it's
    //    in-memory. Swap in `LocalFileSystem`, S3, or GCS and nothing else changes.
    let client = ReplicaClient::new(Arc::new(InMemory::new()), "app");

    // 3. The syncer ties database and replica together and takes a single-writer
    //    lock so two processes can't replicate the same file at once.
    let mut syncer = Syncer::open(db, client).await.expect("open syncer");

    // 4. Your application writes through its *own* connection — literstream reads
    //    the WAL, it never writes your data for you.
    let writer = Connection::open(&db_path).unwrap();
    writer.busy_timeout(Duration::from_secs(5)).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO users(name) VALUES ('alice'), ('bob'), ('carol');",
        )
        .unwrap();

    // 5. sync() turns the newly committed WAL frames into one LTX file in the
    //    replica. The very first sync writes a full snapshot.
    println!("sync #1: {:?}", syncer.sync().await.unwrap());

    // More writes → an incremental LTX carrying only the changed pages.
    writer
        .execute_batch("INSERT INTO users(name) VALUES ('dave'), ('erin')")
        .unwrap();
    println!("sync #2: {:?}", syncer.sync().await.unwrap());

    // flush() drains anything still pending — call it before a clean shutdown.
    syncer.flush().await.unwrap();
    println!("replica is now at txid {}", syncer.position_txid());

    // Restore: rebuild the latest database image straight from the replica and
    // confirm the row count survived the round trip.
    let image = restore(syncer.client()).await.expect("restore");
    let restored = dir.join("restored.db");
    std::fs::write(&restored, &image).unwrap();
    let rows: i64 = Connection::open(&restored)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .unwrap();
    println!("restored {} bytes; users has {rows} rows", image.len());

    let _ = std::fs::remove_dir_all(&dir);
}
