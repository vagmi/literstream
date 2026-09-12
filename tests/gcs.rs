//! Integration tests against Google Cloud Storage — the primary target, and a
//! backend that natively enforces conditional writes (if-generation-match), so
//! the CAS equivocation guard is fully effective here.
//!
//! Three things only a real bucket can settle: that CAS is actually enforced,
//! that ranged GETs behave (direct page reads, and the 100-byte header pre-pass
//! an incremental restore issues per delta file), and that a real DELETE opens a
//! chain gap the way a retention prune does.
//!
//! Gated: set credentials + bucket, then run explicitly:
//!
//!   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
//!   export LITESTREAM_GCS_BUCKET=your-bucket
//!   cargo test --test gcs -- --ignored --nocapture
//!
//! Without `LITESTREAM_GCS_BUCKET` each test prints a skip note and returns.
//! Each run uses its own key prefix; `incremental_restore_over_gcs` sweeps its
//! prefix afterwards, including when an assertion fails.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, Ordering::Relaxed};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use literstream::db::Db;
use literstream::storage::{PutOutcome, ReplicaClient};
use literstream::sync::{
    CatchUp, FallbackReason, ReplicaReader, SyncOutcome, Syncer, catch_up, restore,
    restore_to_path, restore_to_txid,
};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
    path::Path as OsPath,
};
use rusqlite::Connection;

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// The GCS store plus a fresh per-run key prefix, or `None` when the bucket env
/// isn't set. Split from [`gcs_client_from_env`] so a test can wrap the store.
fn gcs_store_from_env() -> Option<(Arc<dyn ObjectStore>, String)> {
    let bucket = std::env::var("LITESTREAM_GCS_BUCKET").ok()?;
    // Reads GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SERVICE_ACCOUNT from the env.
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .ok()?;
    let prefix = format!(
        "itest-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    Some((Arc::new(gcs), prefix))
}

fn gcs_client_from_env() -> Option<ReplicaClient> {
    let (store, prefix) = gcs_store_from_env()?;
    Some(ReplicaClient::new(store, prefix))
}

/// Deletes every LTX file this run wrote, so the bucket doesn't accumulate
/// per-run prefixes. Best-effort: a failure here must not fail the test.
async fn cleanup(client: &ReplicaClient) {
    for level in 0..=9 {
        let Ok(files) = client.list_ltx(level).await else {
            continue;
        };
        for f in files {
            let _ = client.delete_ltx(f.level, f.min_txid, f.max_txid).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires GCS: GOOGLE_APPLICATION_CREDENTIALS + LITESTREAM_GCS_BUCKET"]
async fn cas_guard_enforced_on_gcs() {
    let Some(client) = gcs_client_from_env() else {
        eprintln!("skipping: LITESTREAM_GCS_BUCKET not set");
        return;
    };

    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
            .await
            .unwrap(),
        PutOutcome::AlreadyIdentical,
        "GCS must enforce if-generation-match so re-create is caught"
    );
    assert_eq!(
        client
            .put_ltx_cas(0, 1, 1, Bytes::from_static(b"beta"))
            .await
            .unwrap(),
        PutOutcome::Conflict
    );
    assert_eq!(
        client.get_ltx(0, 1, 1).await.unwrap(),
        Bytes::from_static(b"alpha")
    );
    let _ = client.delete_ltx(0, 1, 1).await;
    println!("gcs CAS guard ok");
}

#[tokio::test]
#[ignore = "requires GCS: GOOGLE_APPLICATION_CREDENTIALS + LITESTREAM_GCS_BUCKET"]
async fn replicate_and_restore_over_gcs() {
    let Some(client) = gcs_client_from_env() else {
        eprintln!("skipping: LITESTREAM_GCS_BUCKET not set");
        return;
    };

    let dir = std::env::temp_dir().join(format!("literstream-gcs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    let db = Db::open(&db_path).unwrap();
    let mut syncer = Syncer::open(db, client.clone()).await.unwrap();

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
        let outcome = syncer.sync().await.unwrap();
        assert!(matches!(
            outcome,
            SyncOutcome::Snapshot { .. } | SyncOutcome::Incremental { .. }
        ));
    }

    let image = restore(&client).await.unwrap();
    let restored = dir.join("restored.db");
    std::fs::write(&restored, &image).unwrap();
    let rc = Connection::open(&restored).unwrap();
    let integrity: String = rc
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    let count: i64 = rc
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    assert_eq!(count, 150);

    // Point-in-time restore to the first transaction (50 rows).
    let pit = restore_to_txid(&client, 1).await.unwrap();
    assert_eq!(pit.txid, 1);

    // Direct page reads via ranged GETs (proves get_range/head work on GCS).
    let ps = 4096usize;
    let mut reader = ReplicaReader::open(&client, None).await.unwrap();
    for pgno in 1..=(image.len() / ps) as u32 {
        let page = reader.read_page(pgno).await.unwrap().expect("page exists");
        let start = (pgno as usize - 1) * ps;
        assert_eq!(
            &page[..],
            &image[start..start + ps],
            "gcs page {pgno} differs"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
    println!("gcs round-trip + PITR + direct page reads ok: 150 rows");
}

// ---------------------------------------------------------------------------
// Incremental restore over GCS.
// ---------------------------------------------------------------------------

/// Counts what actually goes over the wire. Only GETs matter here: the claim
/// under test is that an up-to-date `catch_up` downloads *nothing*, and that a
/// small delta downloads far less than the database. Request counts are left
/// unasserted — `object_store`'s retry layer can legitimately repeat a request —
/// but "zero bytes" survives any number of retries of zero requests.
#[derive(Debug, Default)]
struct Counters {
    get: AtomicU64,
    get_bytes: AtomicU64,
}

impl Counters {
    fn reset(&self) {
        self.get.store(0, Relaxed);
        self.get_bytes.store(0, Relaxed);
    }
    fn gets(&self) -> u64 {
        self.get.load(Relaxed)
    }
    fn bytes(&self) -> u64 {
        self.get_bytes.load(Relaxed)
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
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
        self.c.get.fetch_add(1, Relaxed);
        let r = self.inner.get_opts(location, options).await?;
        self.c
            .get_bytes
            .fetch_add(r.range.end - r.range.start, Relaxed);
        Ok(r)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<OsPath>>,
    ) -> BoxStream<'static, OsResult<OsPath>> {
        self.inner.delete_stream(locations).boxed()
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &OsPath, to: &OsPath, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Catching a warm local copy up to HEAD against a real bucket.
///
/// The in-memory suite (`tests/incremental_restore.rs`) already pins the logic
/// down deterministically. What only GCS can prove is that the pieces the delta
/// path leans on behave the same over the network: the 100-byte **ranged GET per
/// delta file** that the header pre-pass issues, the ten LISTs the plan needs,
/// and a real DELETE opening a genuine chain gap.
#[tokio::test]
#[ignore = "requires GCS: GOOGLE_APPLICATION_CREDENTIALS + LITESTREAM_GCS_BUCKET"]
async fn incremental_restore_over_gcs() {
    let Some((inner, prefix)) = gcs_store_from_env() else {
        eprintln!("skipping: LITESTREAM_GCS_BUCKET not set");
        return;
    };
    let counters = Arc::new(Counters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner,
        c: counters.clone(),
    });
    let client = ReplicaClient::new(store, prefix);

    // Sweep the bucket even when an assertion fails. This test gets re-run while
    // iterating, and a panic partway through would otherwise strand its prefix.
    let result = AssertUnwindSafe(run_incremental_over_gcs(&client, &counters))
        .catch_unwind()
        .await;
    cleanup(&client).await;
    match result {
        Ok(summary) => println!("{summary}"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn run_incremental_over_gcs(client: &ReplicaClient, counters: &Arc<Counters>) -> String {
    let dir = std::env::temp_dir().join(format!("literstream-gcs-incr-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("app.db");

    let mut syncer = Syncer::open(Db::open(&db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = Connection::open(&db_path).unwrap();
    w.busy_timeout(Duration::from_secs(5)).unwrap();
    w.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let _: String = w
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    w.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();

    // A database big enough that rebuilding it is visibly more work than a delta.
    let insert = |lo: i64, hi: i64, note: &str| {
        w.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = w
                .prepare("INSERT INTO items(id, note) VALUES (?1, ?2)")
                .unwrap();
            for i in lo..hi {
                stmt.execute(rusqlite::params![i, note]).unwrap();
            }
        }
        w.execute_batch("COMMIT").unwrap();
    };
    insert(1, 20_000, "bulk");
    syncer.sync().await.unwrap();

    // --- 1. A full restore leaves a usable starting point. -------------------
    let warm = dir.join("warm.db");
    let head = restore_to_path(&client, &warm).await.unwrap();
    let db_bytes = std::fs::metadata(&warm).unwrap().len();
    let marker = std::fs::read_to_string(dir.join("warm.db-litestream/RESTORED")).unwrap();
    assert!(
        marker.contains(r#""state":"clean""#),
        "expected a clean marker, got {marker}"
    );

    // --- 2. Nothing changed: catching up must not download a byte. -----------
    counters.reset();
    assert_eq!(
        catch_up(&client, &warm).await.unwrap(),
        CatchUp::UpToDate { txid: head }
    );
    assert_eq!(
        counters.gets(),
        0,
        "an up-to-date catch-up over GCS must fetch nothing"
    );

    // --- 3. A small delta must cost a small download. ------------------------
    insert(100_000, 100_005, "tiny");
    syncer.sync().await.unwrap();

    counters.reset();
    let outcome = catch_up(&client, &warm).await.unwrap();
    assert!(
        matches!(outcome, CatchUp::Incremental { files: 1, .. }),
        "expected a one-file delta, got {outcome:?}"
    );
    let fetched = counters.bytes();
    assert!(
        fetched * 20 < db_bytes,
        "delta fetched {fetched} bytes for a {db_bytes}-byte database"
    );

    // ...and lands on exactly the image a full rebuild would produce.
    let fresh = dir.join("fresh.db");
    restore_to_path(&client, &fresh).await.unwrap();
    assert_eq!(
        std::fs::read(&warm).unwrap(),
        std::fs::read(&fresh).unwrap(),
        "incremental image differs from a full restore"
    );
    let rc = Connection::open(&warm).unwrap();
    let integrity: String = rc
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    let count: i64 = rc
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    assert_eq!(count, 20_004);
    drop(rc);

    // --- 4. A real DELETE opens a real gap; catch_up absorbs it. -------------
    // Two more transactions, so there is still chain *past* the hole we punch —
    // deleting the newest file would just move HEAD back onto the warm copy.
    insert(200_000, 200_010, "third");
    syncer.sync().await.unwrap();
    insert(300_000, 300_010, "fourth");
    syncer.sync().await.unwrap();
    // Remove the file bridging the warm copy (txid 2) to the rest of the chain —
    // what a compaction plus retention prune does in the wild.
    client.delete_ltx(0, 3, 3).await.unwrap();

    let outcome = catch_up(&client, &warm).await.unwrap();
    assert!(
        matches!(
            outcome,
            CatchUp::FullRestore { reason: FallbackReason::ChainGap, .. }
        ),
        "expected a chain-gap fallback, got {outcome:?}"
    );
    let fresh2 = dir.join("fresh2.db");
    restore_to_path(&client, &fresh2).await.unwrap();
    assert_eq!(
        std::fs::read(&warm).unwrap(),
        std::fs::read(&fresh2).unwrap(),
        "fallback rebuild differs from a full restore"
    );

    drop(syncer);
    let _ = std::fs::remove_dir_all(&dir);
    format!(
        "gcs incremental ok: {db_bytes}-byte db, delta fetched {fetched} bytes, \
         up-to-date fetched 0, chain gap fell back correctly"
    )
}
