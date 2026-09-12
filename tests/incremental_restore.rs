//! Incremental restore: catching a warm local copy up to HEAD by downloading
//! only the LTX files written since it was last current.
//!
//! The load-bearing test here is `differential`: for a write history, a full
//! restore at HEAD must be **byte-identical** to a restore at some earlier TXID
//! followed by an incremental catch-up over the delta. Byte-identical, not
//! "opens and queries the same" — a page-offset bug can leave a database that
//! passes `PRAGMA integrity_check` and still holds wrong contents in a table
//! nobody looked at. Everything else checks that the unsafe paths are refused
//! rather than half-applied.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

use futures::StreamExt;
use futures::stream::BoxStream;
use literstream::db::Db;
use literstream::ltx::Header;
use literstream::storage::ReplicaClient;
use literstream::sync::{
    CatchUp, FallbackReason, SyncError, Syncer, catch_up, restore_incremental, restore_to_path,
    restore_to_txid,
};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
    path::Path as OsPath,
};
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Harness (same shape as tests/sync_replicate.rs).
// ---------------------------------------------------------------------------

fn memory_client() -> ReplicaClient {
    ReplicaClient::new(Arc::new(InMemory::new()), "")
}

struct TempCase {
    dir: PathBuf,
    db_path: PathBuf,
}

impl TempCase {
    fn new(tag: &str) -> TempCase {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "literstream-incr-{}-{tag}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("app.db");
        TempCase { dir, db_path }
    }

    /// A fresh output path (plus its `-litestream` sidecar) inside the case dir.
    fn out(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for TempCase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn writer(path: &Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
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

/// One call = one WAL commit = one TXID.
fn insert_range(c: &Connection, lo: i64, hi: i64, note: &str) {
    c.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = c
            .prepare("INSERT INTO items(id, name, note) VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in lo..hi {
            stmt.execute(rusqlite::params![i, format!("name-{i}"), note])
                .unwrap();
        }
    }
    c.execute_batch("COMMIT").unwrap();
}

/// Cheap deterministic PRNG (no external crate), so a run is reproducible.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// Counting object store — the request-cost instrument.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Counters {
    get: AtomicU64,
    /// Bytes actually transferred by GETs, ranged ones included.
    get_bytes: AtomicU64,
    list: AtomicU64,
}

impl Counters {
    fn reset(&self) {
        self.get.store(0, Relaxed);
        self.get_bytes.store(0, Relaxed);
        self.list.store(0, Relaxed);
    }
    fn gets(&self) -> u64 {
        self.get.load(Relaxed)
    }
    fn bytes(&self) -> u64 {
        self.get_bytes.load(Relaxed)
    }
    fn lists(&self) -> u64 {
        self.list.load(Relaxed)
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
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &OsPath, to: &OsPath, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn counting_client() -> (ReplicaClient, Arc<Counters>) {
    let c = Arc::new(Counters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: Arc::new(InMemory::new()),
        c: c.clone(),
    });
    (ReplicaClient::new(store, ""), c)
}

// ---------------------------------------------------------------------------
// The differential property.
// ---------------------------------------------------------------------------

/// For every intermediate TXID `t` in `marks`: restore the image as of `t`, catch
/// it up incrementally, and assert the bytes equal a full restore at HEAD.
async fn assert_delta_matches_full(client: &ReplicaClient, tc: &TempCase, marks: &[u64]) {
    let full = tc.out("full.db");
    restore_to_path(client, &full).await.unwrap();
    let expected = std::fs::read(&full).unwrap();

    for (i, &t) in marks.iter().enumerate() {
        let at_t = restore_to_txid(client, t).await.unwrap();
        let path = tc.out(&format!("delta-{i}.db"));
        std::fs::write(&path, &at_t.image).unwrap();

        let reached = restore_incremental(client, &path, at_t.txid).await.unwrap();
        let got = std::fs::read(&path).unwrap();

        assert_eq!(
            got.len(),
            expected.len(),
            "size differs after catching up from txid {}",
            at_t.txid
        );
        assert_eq!(
            got, expected,
            "image differs after catching up from txid {} (reached {reached})",
            at_t.txid
        );
    }
}

#[tokio::test]
async fn differential_inserts_only() {
    let tc = TempCase::new("diff-inserts");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    let mut marks = Vec::new();
    for b in 0..8 {
        insert_range(&w, b * 50 + 1, b * 50 + 51, "batch");
        syncer.sync().await.unwrap();
        marks.push(b as u64 + 1);
    }
    assert_delta_matches_full(&client, &tc, &marks).await;
}

#[tokio::test]
async fn differential_delete_and_vacuum_shrinks() {
    let tc = TempCase::new("diff-vacuum");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    // Grow well past one page of content, then throw most of it away and VACUUM,
    // so the final commit is far below what earlier files in the delta carry.
    insert_range(&w, 1, 3000, "bulk");
    syncer.sync().await.unwrap();
    let big = 1;

    w.execute_batch("DELETE FROM items WHERE id > 50").unwrap();
    syncer.sync().await.unwrap();
    w.execute_batch("VACUUM").unwrap();
    syncer.sync().await.unwrap();

    insert_range(&w, 10_000, 10_020, "after-vacuum");
    syncer.sync().await.unwrap();

    assert_delta_matches_full(&client, &tc, &[big, 2, 3]).await;
}

#[tokio::test]
async fn differential_vacuum_then_regrow_inside_one_delta() {
    // The case a single `set_len` to the final commit would miss: the database
    // shrinks and grows back *within* the delta, so the final size says nothing
    // about the low-water mark the file passed through. Truncating to the plan's
    // minimum commit first re-zeroes whatever the VACUUM cut.
    let tc = TempCase::new("diff-regrow");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    insert_range(&w, 1, 4000, "bulk");
    syncer.sync().await.unwrap();
    let mark = 1;

    w.execute_batch("DELETE FROM items WHERE id > 20").unwrap();
    syncer.sync().await.unwrap();
    w.execute_batch("VACUUM").unwrap();
    syncer.sync().await.unwrap();
    insert_range(&w, 20_000, 22_000, "regrow");
    syncer.sync().await.unwrap();

    // Prove the fixture is actually the shape it claims: some file in the delta
    // must carry a commit below both the local size and the final one, or the
    // min-commit truncate is never reached and this test proves nothing.
    let commits = l0_commits(&client, 2..=4).await;
    let low = *commits.iter().min().unwrap();
    assert!(
        low < commits[0] && low < *commits.last().unwrap(),
        "expected a dip in commit sizes inside the delta, got {commits:?}"
    );

    assert_delta_matches_full(&client, &tc, &[mark]).await;
}

/// Each L0 file's `commit` (database size in pages) over a TXID range.
async fn l0_commits(
    client: &ReplicaClient,
    txids: std::ops::RangeInclusive<u64>,
) -> Vec<u32> {
    let mut out = Vec::new();
    for t in txids {
        let bytes = client.get_ltx(0, t, t).await.unwrap();
        out.push(Header::decode(&bytes).unwrap().commit);
    }
    out
}

#[tokio::test]
async fn differential_across_compaction_and_snapshot() {
    let tc = TempCase::new("diff-compact");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    let mut marks = Vec::new();
    for b in 0..4 {
        insert_range(&w, b * 30 + 1, b * 30 + 31, "pre");
        syncer.sync().await.unwrap();
        marks.push(b as u64 + 1);
    }
    // The delta now has to span levels: a coarse L1 file covers part of it.
    syncer.compact_level(1).await.unwrap().unwrap();
    for b in 4..7 {
        insert_range(&w, b * 30 + 1, b * 30 + 31, "post");
        syncer.sync().await.unwrap();
        marks.push(b as u64 + 1);
    }
    syncer.snapshot().await.unwrap().unwrap();
    insert_range(&w, 500, 530, "after-snapshot");
    syncer.sync().await.unwrap();

    assert_delta_matches_full(&client, &tc, &marks).await;
}

#[tokio::test]
async fn differential_randomized_history() {
    let tc = TempCase::new("diff-random");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    let mut rng = Lcg(0x5EED_1234_ABCD_0001);
    let mut marks = Vec::new();
    let mut next_id: i64 = 1;
    for step in 0..20u64 {
        match rng.below(10) {
            0..=5 => {
                let n = 20 + rng.below(300) as i64;
                insert_range(&w, next_id, next_id + n, "rand");
                next_id += n;
            }
            6 | 7 => {
                let cut = rng.below(next_id.max(1) as u64) as i64;
                w.execute_batch(&format!("DELETE FROM items WHERE id > {cut}"))
                    .unwrap();
            }
            8 => w.execute_batch("VACUUM").unwrap(),
            _ => {
                w.execute_batch("UPDATE items SET note = 'touched' WHERE id % 7 = 0")
                    .unwrap();
            }
        }
        syncer.sync().await.unwrap();
        if step % 3 == 0 {
            marks.push(step + 1);
        }
        if step == 11 {
            syncer.compact_level(1).await.unwrap();
        }
    }
    // Only marks that are still restorable (every TXID here still is).
    assert_delta_matches_full(&client, &tc, &marks).await;
}

// ---------------------------------------------------------------------------
// The cost claims.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn up_to_date_downloads_nothing() {
    let tc = TempCase::new("uptodate");
    let (client, counters) = counting_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);
    insert_range(&w, 1, 200, "a");
    syncer.sync().await.unwrap();

    let out = tc.out("warm.db");
    let head = restore_to_path(&client, &out).await.unwrap();

    // A full restore leaves a usable starting point, so the very next catch-up
    // is free: ten LISTs (one per level) and not a single GET.
    counters.reset();
    let outcome = catch_up(&client, &out).await.unwrap();
    assert_eq!(outcome, CatchUp::UpToDate { txid: head });
    assert_eq!(counters.gets(), 0, "an up-to-date catch-up must fetch nothing");
    assert_eq!(counters.lists(), 10, "one LIST per level, and no more");
}

#[tokio::test]
async fn bytes_downloaded_scale_with_the_delta() {
    let tc = TempCase::new("scaling");
    let (client, counters) = counting_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);

    // A database big enough that rebuilding it is visibly expensive.
    insert_range(&w, 1, 20_000, "bulk");
    syncer.sync().await.unwrap();

    let out = tc.out("warm.db");
    restore_to_path(&client, &out).await.unwrap();
    let db_bytes = std::fs::metadata(&out).unwrap().len();

    // ...then a tiny change.
    insert_range(&w, 100_000, 100_005, "tiny");
    syncer.sync().await.unwrap();

    counters.reset();
    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(
        matches!(outcome, CatchUp::Incremental { files: 1, .. }),
        "expected a one-file delta, got {outcome:?}"
    );

    let fetched = counters.bytes();
    assert!(
        fetched * 20 < db_bytes,
        "delta fetched {fetched} bytes for a {db_bytes}-byte database — \
         this is the whole reason the feature exists"
    );

    // And it is still the right image.
    let full = tc.out("full.db");
    restore_to_path(&client, &full).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&full).unwrap());
}

// ---------------------------------------------------------------------------
// The refusals.
// ---------------------------------------------------------------------------

/// `<db>-litestream/RESTORED`.
fn marker_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.to_path_buf().into_os_string();
    p.push("-litestream");
    PathBuf::from(p).join("RESTORED")
}

/// Replicates, restores to `out`, then commits `extra` more transactions.
async fn warm_copy_then_diverge(
    tc: &TempCase,
    client: &ReplicaClient,
    out: &Path,
    extra: usize,
) -> Connection {
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);
    insert_range(&w, 1, 300, "base");
    syncer.sync().await.unwrap();
    restore_to_path(client, out).await.unwrap();

    for i in 0..extra {
        insert_range(&w, 1000 + i as i64 * 10, 1010 + i as i64 * 10, "extra");
        syncer.sync().await.unwrap();
    }
    w
}

#[tokio::test]
async fn full_restore_leaves_a_usable_marker() {
    let tc = TempCase::new("bootstrap");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 0).await;

    let marker = std::fs::read_to_string(marker_path(&out)).unwrap();
    assert!(
        marker.contains(r#""state":"clean""#),
        "a completed restore must leave a clean marker, got {marker}"
    );
}

#[tokio::test]
async fn no_marker_falls_back() {
    let tc = TempCase::new("nomarker");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 2).await;

    std::fs::remove_file(marker_path(&out)).unwrap();
    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(matches!(
        outcome,
        CatchUp::FullRestore { reason: FallbackReason::NoMarker, .. }
    ));
}

#[tokio::test]
async fn catch_up_restores_a_database_that_is_not_there_yet() {
    // The front door works as the *only* restore call a caller makes.
    let tc = TempCase::new("cold");
    let client = memory_client();
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    let w = writer(&tc.db_path);
    ensure_table(&w);
    insert_range(&w, 1, 100, "a");
    syncer.sync().await.unwrap();

    let out = tc.out("cold.db");
    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(matches!(
        outcome,
        CatchUp::FullRestore { reason: FallbackReason::NoMarker, .. }
    ));
    assert!(out.exists());
    // And the second call is free.
    assert!(matches!(
        catch_up(&client, &out).await.unwrap(),
        CatchUp::UpToDate { .. }
    ));
}

#[tokio::test]
async fn torn_apply_falls_back() {
    let tc = TempCase::new("torn");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 2).await;

    // A crash between the two marker writes leaves this behind: the file is a
    // mixture of two images, and nothing in it says so.
    std::fs::write(marker_path(&out), r#"{"state":"applying","from":1,"to":3}"#).unwrap();
    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(matches!(
        outcome,
        CatchUp::FullRestore { reason: FallbackReason::DirtyMarker, .. }
    ));

    // The rebuild is correct, and leaves a clean marker behind it.
    let full = tc.out("full.db");
    restore_to_path(&client, &full).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&full).unwrap());
    assert!(matches!(
        catch_up(&client, &out).await.unwrap(),
        CatchUp::UpToDate { .. }
    ));
}

#[tokio::test]
async fn foreign_writes_fall_back() {
    let tc = TempCase::new("foreign");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 2).await;

    // Something else touched the file behind literstream's back.
    let mut bytes = std::fs::read(&out).unwrap();
    bytes.push(0);
    std::fs::write(&out, &bytes).unwrap();

    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(matches!(
        outcome,
        CatchUp::FullRestore { reason: FallbackReason::LengthMismatch, .. }
    ));
}

#[tokio::test]
async fn page_size_change_falls_back() {
    let tc = TempCase::new("pagesize");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 2).await;

    // Claim the local image is at 8192-byte pages. Applying 4096-byte pages at
    // 8192-byte offsets would produce a structurally plausible, entirely wrong
    // database — the failure this guard exists to prevent is a silent one.
    let file_len = std::fs::metadata(&out).unwrap().len();
    std::fs::write(
        marker_path(&out),
        format!(
            r#"{{"state":"clean","txid":1,"page_size":8192,"commit":1,"file_len":{file_len}}}"#
        ),
    )
    .unwrap();

    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(
        matches!(
            outcome,
            CatchUp::FullRestore { reason: FallbackReason::PageSizeChanged, .. }
        ),
        "got {outcome:?}"
    );
}

#[tokio::test]
async fn local_ahead_of_the_replica_falls_back() {
    let tc = TempCase::new("ahead");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 1).await;

    let file_len = std::fs::metadata(&out).unwrap().len();
    std::fs::write(
        marker_path(&out),
        format!(
            r#"{{"state":"clean","txid":9999,"page_size":4096,"commit":1,"file_len":{file_len}}}"#
        ),
    )
    .unwrap();

    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(matches!(
        outcome,
        CatchUp::FullRestore { reason: FallbackReason::LocalAhead, .. }
    ));
}

#[tokio::test]
async fn chain_gap_falls_back_but_restore_incremental_errors() {
    let tc = TempCase::new("gap");
    let client = memory_client();
    let out = tc.out("warm.db");
    let _w = warm_copy_then_diverge(&tc, &client, &out, 3).await;

    // The file bridging the local position to the rest of the chain is gone —
    // compacted into a coarser level and retention-pruned, in the real world.
    client.delete_ltx(0, 2, 2).await.unwrap();

    // The unchecked entry point refuses outright...
    assert!(matches!(
        restore_incremental(&client, &out, 1).await,
        Err(SyncError::ChainGap { from: 1 })
    ));

    // ...while the front door absorbs it. This is the difference between them.
    let outcome = catch_up(&client, &out).await.unwrap();
    assert!(
        matches!(
            outcome,
            CatchUp::FullRestore { reason: FallbackReason::ChainGap, .. }
        ),
        "got {outcome:?}"
    );

    let full = tc.out("full.db");
    restore_to_path(&client, &full).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&full).unwrap());
}

#[tokio::test]
async fn catch_up_is_idempotent_and_lands_on_the_full_image() {
    let tc = TempCase::new("repeat");
    let client = memory_client();
    let out = tc.out("warm.db");
    let w = warm_copy_then_diverge(&tc, &client, &out, 2).await;

    let first = catch_up(&client, &out).await.unwrap();
    assert!(matches!(first, CatchUp::Incremental { from: 1, .. }), "got {first:?}");
    assert!(matches!(
        catch_up(&client, &out).await.unwrap(),
        CatchUp::UpToDate { .. }
    ));

    // More writes, another delta, still byte-identical to a fresh full restore.
    let mut syncer = Syncer::open(Db::open(&tc.db_path).unwrap(), client.clone())
        .await
        .unwrap();
    insert_range(&w, 5000, 5100, "more");
    syncer.sync().await.unwrap();

    assert!(matches!(
        catch_up(&client, &out).await.unwrap(),
        CatchUp::Incremental { .. }
    ));
    let full = tc.out("full.db");
    restore_to_path(&client, &full).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&full).unwrap());
}
