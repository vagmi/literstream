//! Fan-out benchmark: many small, sparingly-written databases replicating to a
//! shared object store, the way an app that manages ~100 tenant databases would
//! use literstream. It measures what that workload actually stresses: the
//! resource footprint of N in-process `Driver`s, correctness at fan-out, and the
//! object-store request cost, especially the fixed "idle tax" of ticking
//! databases that are not being written.
//!
//! An `ObjectStore` wrapper counts every operation by kind, so the output is a
//! request budget (puts, lists, gets, multipart parts, deletes), not just a
//! pass/fail. Deletes are free on GCS; lists and inserts are the Class A ops that
//! cost money, and for a mostly-idle fleet the lists dominate.
//!
//! Run (GCS):
//!   ulimit -n 4096
//!   GCS_BUCKET=literstream-test-bucket \
//!     cargo run --release --example fanout_bench
//!
//! Tunables (env): FANOUT_N, FANOUT_DURATION_S, FANOUT_TICK_MS, FANOUT_AGG_TPS,
//!   FANOUT_MIN_KB, FANOUT_MAX_KB, FANOUT_PREFIX, FANOUT_LOCAL=<dir> (use a local
//!   filesystem replica instead of GCS, for a free dry run).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime};

use futures::future::join_all;
use futures::stream::BoxStream;
use futures::StreamExt;
use literstream::db::Db;
use literstream::storage::ReplicaClient;
use literstream::sync::{CompactionLevels, Driver, restore_to_path};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
    UploadPart, path::Path as OsPath,
};
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Counting object-store wrapper (the request-cost instrument).
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Counters {
    put: AtomicU64,
    mpu_init: AtomicU64,
    mpu_part: AtomicU64,
    mpu_complete: AtomicU64,
    get: AtomicU64, // includes ranged gets and HEAD (both go through get_opts)
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> [u64; 8] {
        [
            self.put.load(Relaxed),
            self.mpu_init.load(Relaxed),
            self.mpu_part.load(Relaxed),
            self.mpu_complete.load(Relaxed),
            self.get.load(Relaxed),
            self.list.load(Relaxed),
            self.delete.load(Relaxed),
            self.copy.load(Relaxed),
        ]
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    c: Arc<Counters>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.c.put.fetch_add(1, Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.c.mpu_init.fetch_add(1, Relaxed);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner,
            c: self.c.clone(),
        }))
    }

    async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
        self.c.get.fetch_add(1, Relaxed);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<OsPath>>,
    ) -> BoxStream<'static, OsResult<OsPath>> {
        let c = self.c.clone();
        self.inner
            .delete_stream(locations)
            .inspect(move |r| {
                if r.is_ok() {
                    c.delete.fetch_add(1, Relaxed);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &OsPath, to: &OsPath, options: CopyOptions) -> OsResult<()> {
        self.c.copy.fetch_add(1, Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    c: Arc<Counters>,
}

#[async_trait::async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.c.mpu_part.fetch_add(1, Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        self.c.mpu_complete.fetch_add(1, Relaxed);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.inner.abort().await
    }
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Peak resident set size so far, in bytes (macOS reports ru_maxrss in bytes,
/// Linux in kibibytes).
fn peak_rss_bytes() -> u64 {
    let maxrss = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru.ru_maxrss as u64
    };
    if cfg!(target_os = "macos") { maxrss } else { maxrss * 1024 }
}

/// Total CPU time (user + system) so far, in seconds.
fn cpu_secs() -> f64 {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        let s = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        s(ru.ru_utime) + s(ru.ru_stime)
    }
}

/// Cheap deterministic PRNG (no external crate), so a run is reproducible.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 17
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Creates a WAL-mode database at `path` seeded to at least `target` bytes, then
/// TRUNCATE-checkpoints so the bytes live in the main file (a realistic starting
/// snapshot size).
fn seed_db(path: &std::path::Path, target: u64, rng: &mut Lcg) {
    let c = Connection::open(path).unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).unwrap();
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    let payload = vec![0u8; 400];
    loop {
        c.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = c.prepare("INSERT INTO t(v) VALUES (?1)").unwrap();
            for _ in 0..200 {
                stmt.execute(rusqlite::params![&payload]).unwrap();
            }
        }
        c.execute_batch("COMMIT").unwrap();
        let wal = {
            let mut p = path.to_path_buf().into_os_string();
            p.push("-wal");
            std::fs::metadata(std::path::PathBuf::from(p)).map(|m| m.len()).unwrap_or(0)
        };
        if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) + wal >= target {
            break;
        }
        let _ = rng.next();
    }
    c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    c.close().unwrap();
}

/// A separate long-lived writer connection, as a real application would hold.
fn writer(path: &std::path::Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).unwrap();
    c
}

fn row_count(path: &std::path::Path) -> i64 {
    let c = Connection::open(path).unwrap();
    c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
}

#[tokio::main]
async fn main() {
    let n = env_u64("FANOUT_N", 100) as usize;
    let duration = Duration::from_secs(env_u64("FANOUT_DURATION_S", 60));
    let tick = Duration::from_millis(env_u64("FANOUT_TICK_MS", 1000));
    let agg_tps = env_u64("FANOUT_AGG_TPS", 20);
    let min_kb = env_u64("FANOUT_MIN_KB", 100);
    let max_kb = env_u64("FANOUT_MAX_KB", 5000);
    let base_prefix = std::env::var("FANOUT_PREFIX")
        .unwrap_or_else(|_| format!("fanout-{}", std::process::id()));

    // Backend: local filesystem (free dry run) or GCS.
    let raw: Arc<dyn ObjectStore> = if let Ok(dir) = std::env::var("FANOUT_LOCAL") {
        std::fs::create_dir_all(&dir).unwrap();
        println!("backend  : LocalFileSystem({dir})");
        Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap())
    } else {
        let bucket = std::env::var("GCS_BUCKET")
            .or_else(|_| std::env::var("LITESTREAM_GCS_BUCKET"))
            .expect("set GCS_BUCKET (or FANOUT_LOCAL for a local dry run)");
        println!("backend  : gs://{bucket}");
        Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket).build().unwrap())
    };
    let counters = Arc::new(Counters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: raw.clone(),
        c: counters.clone(),
    });

    println!(
        "workload : {n} databases, {}s, ~{agg_tps} txns/s aggregate, tick {}ms, sizes {min_kb}-{max_kb} KB",
        duration.as_secs(),
        tick.as_millis()
    );
    println!("prefix   : {base_prefix}/db-<i>\n");

    let dir = std::env::temp_dir().join(format!("literstream-fanout-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // --- Setup: seed databases, open a Driver per database. ---
    let t_setup = Instant::now();
    let mut rng = Lcg(0x1234_5678_9abc_def0);
    let mut db_paths = Vec::with_capacity(n);
    for i in 0..n {
        let path = dir.join(format!("db-{i}.db"));
        let target = (min_kb + rng.below(max_kb - min_kb + 1)) * 1024;
        seed_db(&path, target, &mut rng);
        db_paths.push(path);
    }

    let mut opens = Vec::with_capacity(n);
    for (i, path) in db_paths.iter().enumerate() {
        let client = ReplicaClient::new(store.clone(), format!("{base_prefix}/db-{i}"));
        let db = Db::open(path).unwrap();
        opens.push(literstream::sync::Syncer::open(db, client));
    }
    // Open all syncers concurrently: each does a networked derive_position.
    let mut drivers: Vec<Driver> = join_all(opens)
        .await
        .into_iter()
        .map(|s| {
            // Short snapshot cadence so snapshots (and their list traffic) fire
            // within a benchmark-length run; compaction uses the defaults.
            Driver::new(s.unwrap(), CompactionLevels::default())
                .with_snapshot_interval(Duration::from_secs(env_u64("FANOUT_SNAPSHOT_S", 30)))
        })
        .collect();
    let writers: Vec<Connection> = db_paths.iter().map(|p| writer(p)).collect();
    let setup_ops = counters.snapshot();
    let setup_secs = t_setup.elapsed().as_secs_f64();
    let rss_after_setup = peak_rss_bytes() as f64 / (1024.0 * 1024.0);
    println!(
        "setup    : {n} databases seeded + opened in {setup_secs:.1}s, RSS {rss_after_setup:.1} MB  \
         ({} list, {} get during open)\n",
        setup_ops[5], setup_ops[4]
    );

    // --- Run: sparse writes across the fleet, tick every driver each interval. ---
    let run_start_ops = counters.snapshot();
    let t_run = Instant::now();
    let mut writes_issued: u64 = 0;
    let mut resnapshots: u64 = 0;
    let mut max_backlog: u64 = 0;
    let per_tick_writes = (agg_tps * tick.as_millis() as u64 / 1000).max(1);

    while t_run.elapsed() < duration {
        let step_start = Instant::now();

        // Workload: a handful of small transactions, each to a random database.
        for _ in 0..per_tick_writes {
            let i = rng.below(n as u64) as usize;
            let w = &writers[i];
            w.execute_batch("BEGIN").unwrap();
            {
                let mut stmt = w.prepare("INSERT INTO t(v) VALUES (?1)").unwrap();
                let payload = vec![0u8; 400];
                for _ in 0..5 {
                    stmt.execute(rusqlite::params![&payload]).unwrap();
                }
            }
            w.execute_batch("COMMIT").unwrap();
            writes_issued += 1;
        }

        // Tick every driver concurrently (I/O interleaves; disjoint &mut borrows).
        let now = SystemTime::now();
        let reports = join_all(drivers.iter_mut().map(|d| d.tick(now))).await;
        for r in reports.into_iter().flatten() {
            if r.resnapshot_fired {
                resnapshots += 1;
            }
            max_backlog = max_backlog.max(r.staged_backlog_bytes);
        }

        if let Some(rem) = tick.checked_sub(step_start.elapsed()) {
            tokio::time::sleep(rem).await;
        }
    }
    let run_secs = t_run.elapsed().as_secs_f64();
    let peak_run_mb = peak_rss_bytes() as f64 / (1024.0 * 1024.0);

    // Drain everything still pending so the replicas are complete.
    for d in &mut drivers {
        d.flush().await.unwrap();
    }
    let run_ops: Vec<i128> = counters
        .snapshot()
        .iter()
        .zip(run_start_ops.iter())
        .map(|(a, b)| *a as i128 - *b as i128)
        .collect();

    // --- Verify: every database restores exactly. Stream to disk (O(page)) so
    // verifying all N at once does not itself balloon memory. ---
    let restored: Vec<bool> = join_all(drivers.iter().enumerate().map(|(i, d)| {
        let want = row_count(&db_paths[i]);
        let client = d.client().clone();
        let out = dir.join(format!("restored-{i}.db"));
        async move {
            restore_to_path(&client, &out)
                .await
                .map(|_| row_count(&out) == want)
                .unwrap_or(false)
        }
    }))
    .await;
    let ok = restored.iter().filter(|b| **b).count();

    // --- Report. ---
    let peak_mb = peak_rss_bytes() as f64 / (1024.0 * 1024.0);
    let cpu = cpu_secs();
    let class_a = run_ops[0] + run_ops[1] + run_ops[2] + run_ops[5] + run_ops[7]; // put+mpu_init+mpu_part+list+copy
    println!("\n===== results =====");
    println!("databases          : {n}");
    println!("correctness        : {ok}/{n} restored exactly");
    println!("writes issued      : {writes_issued}  (~{:.1}/s aggregate)", writes_issued as f64 / run_secs);
    println!("RSS after setup    : {rss_after_setup:.1} MB  ({n} idle drivers, replication not yet running)");
    println!("peak RSS in run    : {peak_run_mb:.1} MB  (steady-state replication of all {n})");
    println!("peak RSS w/ verify : {peak_mb:.1} MB  (also restoring all {n} at once; a bench artifact)");
    println!("CPU total          : {cpu:.2} s");
    println!("over-fold snapshots: {resnapshots}");
    println!("max staged backlog : {} bytes", max_backlog);
    println!("\nobject-store ops during the {run_secs:.0}s run (delta):");
    let label = ["put", "mpu_init", "mpu_part", "mpu_complete", "get", "list", "delete", "copy"];
    for (i, l) in label.iter().enumerate() {
        println!("  {l:<12} {:>8}   ({:>6.1}/s)", run_ops[i], run_ops[i] as f64 / run_secs);
    }
    println!(
        "  Class A (billed: put+mpu+list+copy) {class_a}  ({:.1}/s). deletes are free on GCS.",
        class_a as f64 / run_secs
    );
    println!(
        "  list ops/s is the idle tax: it scales with databases x compaction/retention cadence, \n  not with write volume."
    );

    // --- Cleanup: remove replica objects and local databases. ---
    print!("\ncleanup  : deleting {base_prefix}/ from the bucket... ");
    let mut stream = store.list(Some(&OsPath::from(base_prefix.as_str())));
    let mut locs = Vec::new();
    while let Some(meta) = stream.next().await {
        if let Ok(m) = meta {
            locs.push(m.location);
        }
    }
    let n_del = locs.len();
    futures::stream::iter(locs)
        .for_each_concurrent(32, |loc| {
            let store = store.clone();
            async move {
                let _ = store.delete(&loc).await;
            }
        })
        .await;
    println!("{n_del} objects removed");
    let _ = std::fs::remove_dir_all(&dir);
}
