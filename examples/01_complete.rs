//! 01 — a complete replication driver: continuous sync, checkpointing, tiered
//! time-based compaction, retention, and point-in-time recovery.
//!
//!     cargo run --example 01_complete
//!     LITERSTREAM_DEMO_SECS=90 cargo run --example 01_complete   # quick run
//!
//! Random INSERT/UPDATE/DELETE traffic runs against a real SQLite database for
//! four minutes while a [`Driver`] replicates it to a local-disk replica and
//! compacts it in the background — exactly what a litestream deployment does.
//!
//! ## Compaction levels, matching litestream
//!
//! The driver runs a *multi-level, time-based* cascade just like litestream: raw
//! L0 files (one per sync) are merged into **L1 every 10s** and **L2 every 60s**,
//! with a full **snapshot every 60s**. `Driver::tick(now)` does it all — sync,
//! checkpoint, per-level compaction on each interval boundary, snapshot, and
//! retention.
//!
//! ## Why PITR keeps working *while* compacting
//!
//! Compaction alone would collapse history, but **time-based retention** keeps a
//! recent window of L0 files (here 90s) live, so every synced TXID inside that
//! window is still individually restorable. Older points fall out of the window
//! and degrade to the nearest compacted boundary — the granularity/storage
//! tradeoff, now *bounded by retention* rather than by compaction destroying
//! history. The PITR section at the end shows recent marks restoring exactly and
//! older marks snapping to a coarser boundary.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use literstream::db::{CheckpointMode, Db};
use literstream::storage::ReplicaClient;
use literstream::sync::{
    CompactionLevel, CompactionLevels, Driver, Syncer, restore, restore_to_timestamp,
    restore_to_txid,
};
use object_store::local::LocalFileSystem;
use rusqlite::{Connection, params};

/// How long the workload runs (override with `LITERSTREAM_DEMO_SECS`).
const DEFAULT_RUN_SECS: u64 = 240;
/// One driver tick per second.
const TICK: Duration = Duration::from_secs(1);
/// How often to record a recovery mark to restore to later.
const MARK_INTERVAL: Duration = Duration::from_secs(30);

/// A recovery point captured during the run, used to exercise PITR afterwards.
struct Mark {
    at_secs: u64,
    txid: u64,
    ts_ms: i64,
    rows: i64,
}

#[tokio::main]
async fn main() {
    let run = Duration::from_secs(
        std::env::var("LITERSTREAM_DEMO_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RUN_SECS),
    );

    let dir = std::env::temp_dir().join(format!("literstream-01-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");
    let replica_root = dir.join("replica");
    std::fs::create_dir_all(&replica_root).unwrap();

    println!("db      : {}", db_path.display());
    println!("replica : {}", replica_root.display());
    println!(
        "levels  : L1@10s, L2@60s, snapshot@60s, L0 retention 90s — running {}s\n",
        run.as_secs()
    );

    // The replicated database and a local-disk replica.
    let db = Db::open(&db_path).expect("open db");
    let store = LocalFileSystem::new_with_prefix(&replica_root).expect("local store");
    let client = ReplicaClient::new(Arc::new(store), "");
    let syncer = Syncer::open(db, client.clone()).await.expect("open syncer");

    // Tiered, time-based compaction — the same shape as litestream's defaults,
    // just faster so it's observable in a short demo.
    let levels = CompactionLevels::new(vec![
        CompactionLevel { level: 0, interval: Duration::ZERO },
        CompactionLevel { level: 1, interval: Duration::from_secs(10) },
        CompactionLevel { level: 2, interval: Duration::from_secs(60) },
    ])
    .unwrap();
    let mut driver = Driver::new(syncer, levels)
        .with_snapshot_interval(Duration::from_secs(60))
        .with_snapshot_retention(Duration::from_secs(24 * 60 * 60))
        .with_l0_retention(Duration::from_secs(90));
    // Tune checkpoints down so they fire during a short demo (defaults 1000/10000).
    driver.syncer_mut().min_checkpoint_frames = 200;
    driver.syncer_mut().truncate_frames = 2000;

    // The application writer — a separate connection, as in real usage.
    let writer = Connection::open(&db_path).unwrap();
    writer.busy_timeout(Duration::from_secs(5)).unwrap();
    writer
        .execute_batch("CREATE TABLE kv(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15);
    let mut next_id: i64 = 1;
    insert_batch(&writer, &mut rng, &mut next_id, 50);

    // ---- the driver loop -------------------------------------------------
    let start = Instant::now();
    let mut last_mark = Duration::ZERO;
    let mut marks: Vec<Mark> = Vec::new();

    loop {
        let elapsed = start.elapsed();
        if elapsed >= run {
            break;
        }

        random_ops(&writer, &mut rng, &mut next_id);
        let report = driver.tick(SystemTime::now()).await.unwrap();

        // Narrate the interesting events.
        let t = elapsed.as_secs();
        for (level, info) in &report.compactions {
            println!(
                "  t+{t:>3}s  L{level} compact -> [{}..={}] ({} inputs)",
                info.min_txid, info.max_txid, info.inputs
            );
        }
        if let Some(info) = &report.snapshot {
            println!("  t+{t:>3}s  snapshot -> [1..={}]", info.max_txid);
        }
        if report.l0_pruned > 0 {
            println!("  t+{t:>3}s  retention pruned {} L0 file(s)", report.l0_pruned);
        }
        if let Some((CheckpointMode::Truncate, res)) = &report.checkpoint {
            println!("  t+{t:>3}s  TRUNCATE checkpoint ({} frames)", res.log_frames);
        }

        if elapsed - last_mark >= MARK_INTERVAL {
            last_mark = elapsed;
            let mark = Mark {
                at_secs: t,
                txid: driver.syncer().position_txid(),
                ts_ms: now_ms(),
                rows: row_count(&writer),
            };
            println!("  --- mark t+{t:>3}s: txid={} rows={} ---", mark.txid, mark.rows);
            marks.push(mark);
        }

        std::thread::sleep(TICK);
    }

    driver.flush().await.unwrap();
    let final_rows = row_count(&writer);
    println!(
        "\nworkload done: {final_rows} rows at txid {}",
        driver.syncer().position_txid()
    );
    print_storage(&client).await;

    // ---- point-in-time recovery -----------------------------------------
    println!("\n=== point-in-time recovery ===");

    let latest = restore(&client).await.expect("restore latest");
    println!(
        "latest              -> {} rows (expected {final_rows})",
        count_rows(&latest, &dir, "latest")
    );

    // Recent marks (inside the L0 retention window) restore exactly; older marks
    // snap down to the nearest compacted boundary.
    for m in &marks {
        match restore_to_txid(&client, m.txid).await {
            Ok(r) if r.txid == m.txid => println!(
                "@txid {:<4} (t+{:>3}s) -> {} rows (recorded {}) [exact]",
                m.txid,
                m.at_secs,
                count_rows(&r.image, &dir, "x"),
                m.rows
            ),
            Ok(r) => println!(
                "@txid {:<4} (t+{:>3}s) -> snapped to txid {} (retention window passed)",
                m.txid, m.at_secs, r.txid
            ),
            Err(e) => println!("@txid {:<4} (t+{:>3}s) -> {e}", m.txid, m.at_secs),
        }
    }

    // Timestamp recovery to the most recent mark (still within the window).
    if let Some(m) = marks.last() {
        match restore_to_timestamp(&client, m.ts_ms).await {
            Ok(r) => println!(
                "@time (t+{:>3}s)     -> txid {}, {} rows",
                m.at_secs,
                r.txid,
                count_rows(&r.image, &dir, "ts")
            ),
            Err(e) => println!("@time (t+{}s) -> {e}", m.at_secs),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

async fn print_storage(client: &ReplicaClient) {
    let mut parts = Vec::new();
    let mut total = 0u64;
    for level in [0u32, 1, 2, 9] {
        let files = client.list_ltx(level).await.unwrap();
        if !files.is_empty() {
            parts.push(format!("L{level}={}", files.len()));
            total += files.iter().map(|f| f.size).sum::<u64>();
        }
    }
    println!("replica: {}  ({} KiB total)", parts.join(" "), total / 1024);
}

/// Applies a random burst of inserts, updates, and deletes in one transaction.
fn random_ops(w: &Connection, rng: &mut Rng, next_id: &mut i64) {
    let inserts = 5 + (rng.next() % 16) as usize;
    insert_batch(w, rng, next_id, inserts);

    let upper = (*next_id).max(1) as u64;
    w.execute_batch("BEGIN").unwrap();
    for _ in 0..(rng.next() % 8) {
        let id = (rng.next() % upper) as i64;
        w.execute(
            "UPDATE kv SET val = ?2 WHERE id = ?1",
            params![id, format!("u{}", rng.next())],
        )
        .unwrap();
    }
    for _ in 0..(rng.next() % 4) {
        let id = (rng.next() % upper) as i64;
        w.execute("DELETE FROM kv WHERE id = ?1", params![id]).unwrap();
    }
    w.execute_batch("COMMIT").unwrap();
}

/// Inserts `n` fresh rows in one transaction.
fn insert_batch(w: &Connection, rng: &mut Rng, next_id: &mut i64, n: usize) {
    w.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = w.prepare("INSERT INTO kv(id, val) VALUES (?1, ?2)").unwrap();
        for _ in 0..n {
            let id = *next_id;
            *next_id += 1;
            stmt.execute(params![id, format!("v{}", rng.next())]).unwrap();
        }
    }
    w.execute_batch("COMMIT").unwrap();
}

fn row_count(w: &Connection) -> i64 {
    w.query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0)).unwrap()
}

/// Writes a restored image to a temp file and counts its rows.
fn count_rows(image: &[u8], dir: &Path, tag: &str) -> i64 {
    let p = dir.join(format!("restore-{tag}.db"));
    std::fs::write(&p, image).unwrap();
    let n = Connection::open(&p)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    let _ = std::fs::remove_file(&p);
    n
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A tiny xorshift64 PRNG so the example needs no `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
