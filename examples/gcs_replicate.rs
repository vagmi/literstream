//! Replicates a small live database to a GCS replica using literstream, then
//! compacts so a level-9 snapshot base exists (litestream's restore requires
//! one). Used by the litestream↔literstream cross-tool validation.
//!
//!     cargo run --example gcs_replicate -- <db> <bucket> <prefix>

use std::sync::Arc;
use std::time::Duration;

use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::Syncer;
use object_store::gcp::GoogleCloudStorageBuilder;
use rusqlite::Connection;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: {} <db> <bucket> <prefix>", args[0]);
        std::process::exit(2);
    }
    let db_path = std::path::PathBuf::from(&args[1]);
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));

    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(&args[2])
        .build()
        .expect("build gcs");
    let client = ReplicaClient::new(Arc::new(gcs), args[3].clone());

    let db = Db::open(&db_path).expect("open db");
    let mut syncer = Syncer::open(db, client).await.expect("open syncer");

    // Application writer inserts 150 rows across 3 transactions.
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
        println!("sync {}: {:?}", batch + 1, syncer.sync().await.unwrap());
    }

    // Compact so a level-9 snapshot base exists for litestream's restore.
    match syncer.compact().await.unwrap() {
        Some(info) => println!(
            "compacted -> snapshot base [1..{}], pruned {}",
            info.max_txid, info.pruned
        ),
        None => println!("nothing to compact"),
    }
    println!(
        "literstream replicated 150 rows to gs://{}/{}",
        args[2], args[3]
    );
}
