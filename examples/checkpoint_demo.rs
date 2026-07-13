//! A narrated walk through literstream's SQLite control model: WAL mode with
//! autocheckpoint disabled, the WAL growing unbounded, and manual PASSIVE /
//! TRUNCATE checkpoints draining it.
//!
//!     cargo run --example checkpoint_demo
//!
//! Writes to a throwaway database under the system temp directory.

use literstream::db::{CheckpointMode, Db};

fn main() {
    let path = std::env::temp_dir().join(format!("literstream-demo-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let mut p = path.clone().into_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(p);
    }

    let mut db = Db::open(&path).expect("open");
    println!("opened {}", db.path().display());
    println!("  journal_mode      = {}", db.journal_mode().unwrap());
    println!(
        "  wal_autocheckpoint= {}   (0 = we control checkpoints)",
        db.wal_autocheckpoint().unwrap()
    );
    println!("  page_size         = {}", db.page_size());
    println!();

    let conn = db.connection();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();

    println!("writing 5 batches of 400 rows; WAL grows because nothing checkpoints:");
    for batch in 1..=5 {
        conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO t(b) VALUES (randomblob(4000))")
                .unwrap();
            for _ in 0..400 {
                stmt.execute([]).unwrap();
            }
        }
        conn.execute_batch("COMMIT").unwrap();
        println!(
            "  after batch {batch}: wal = {:>7} bytes / {} frames",
            db.wal_size(),
            db.wal_frame_count()
        );
    }
    println!();

    let r = db.checkpoint(CheckpointMode::Passive).unwrap();
    println!(
        "PASSIVE checkpoint: busy={} log={} checkpointed={} (fully = {})",
        r.busy,
        r.log_frames,
        r.checkpointed_frames,
        r.fully_checkpointed()
    );
    println!(
        "  wal still on disk: {} bytes (PASSIVE never truncates the file)",
        db.wal_size()
    );

    let r = db.checkpoint(CheckpointMode::Truncate).unwrap();
    println!("TRUNCATE checkpoint: busy={}", r.busy);
    println!("  wal now: {} bytes", db.wal_size());

    for suffix in ["", "-wal", "-shm"] {
        let mut p = path.clone().into_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(p);
    }
}
