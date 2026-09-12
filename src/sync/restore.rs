//! Rebuilding a database from a replica's LTX chain — full, point-in-time, and
//! incremental.
//!
//! Three shapes of restore live here:
//!
//! - **Full** ([`restore`], [`restore_to_path`]): plan a greedy cover from TXID 1
//!   and write every page. Correct from a known anchor, and proportional to the
//!   database.
//! - **Point-in-time** ([`restore_to_txid`], [`restore_to_timestamp`]): the same
//!   cover, stopped short of a target.
//! - **Incremental** ([`catch_up`], [`restore_incremental`]): take a local image
//!   that is already at some TXID and apply only the files written since, so the
//!   cost is proportional to *what changed*.
//!
//! The invariant the incremental path rests on:
//!
//! > Every page whose content differs between `from_txid` and `to_txid` appears in
//! > at least one file of the delta plan.
//!
//! That holds by construction of the chain — a page cannot change without a
//! transaction, and the plan covers every transaction in the range.
//!
//! What it does *not* give us is a way to check that a local image really is at
//! `from_txid`. literstream writes every file with `HEADER_FLAG_NO_CHECKSUM` and
//! `pre_apply_checksum = 0` (litestream's restore rejects files carrying a rolling
//! checksum), so the chain carries no rolling state to compare against. A
//! `from_txid` that is too high silently skips the files holding the missing pages
//! and yields a database that decodes, opens, and answers queries wrongly. So
//! literstream owns the claim instead of accepting it: [`catch_up`] trusts only the
//! position marker it wrote itself, cross-checked against the file on disk, and
//! falls back to a full restore whenever the delta path is not provably safe.

use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
// Positional file I/O (pread/pwrite): `read_exact_at` / `write_all_at`. Unix
// (Linux + macOS); a Windows port would use `std::os::windows::fs::FileExt`.
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::ltx::{
    HEADER_SIZE, Header, INDEX_FOOTER_SIZE, decode_page_frame, decode_page_index, lock_pgno,
    read_file,
};
use crate::storage::ReplicaClient;

use super::{MAX_LEVEL, SyncError, litestream_dir, with_extension_suffix};

/// Lists every LTX file across all levels as `(level, min_txid, max_txid)`.
///
/// One LIST request **per level** — ten in total. That is the floor cost of any
/// restore, including a [`catch_up`] that turns out to be already current.
pub(super) async fn list_all_levels(
    client: &ReplicaClient,
) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let mut files = Vec::new();
    for level in 0..=MAX_LEVEL {
        for f in client.list_ltx(level).await? {
            files.push((level, f.min_txid, f.max_txid));
        }
    }
    Ok(files)
}

/// The greedy cover both planners share: starting at `from`, repeatedly pick the
/// file that begins contiguously and reaches the furthest, preferring higher
/// (compacted) levels on ties. This uses the fewest files to cover the range.
///
/// Returns the plan (ascending by TXID) and the TXID it reaches — `from` itself
/// when nothing extends past it.
fn greedy_cover(files: &[(u32, u64, u64)], from: u64) -> (Vec<(u32, u64, u64)>, u64) {
    let mut plan: Vec<(u32, u64, u64)> = Vec::new();
    let mut pos: u64 = from;
    while let Some(&next) = files
        .iter()
        .filter(|(_, min, max)| *min <= pos + 1 && *max > pos)
        .max_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)))
    {
        pos = next.2;
        plan.push(next);
    }
    (plan, pos)
}

/// A greedy restore plan covering the whole chain from TXID 1.
pub(super) fn plan_restore(files: &[(u32, u64, u64)]) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let (plan, _) = greedy_cover(files, 0);
    if plan.first().map(|(_, min, _)| *min) != Some(1) {
        return Err(SyncError::NoSnapshot);
    }
    Ok(plan)
}

/// A greedy cover of `(from_txid, HEAD]` — the delta a local image at
/// `from_txid` needs to become current.
///
/// Three behaviours worth naming:
///
/// - **An empty plan is success, not failure.** `from_txid == HEAD` is the common
///   warm case: nobody else wrote while we were away. It costs ten LISTs and zero
///   GETs.
/// - **The first file usually straddles** (`min_txid <= from_txid < max_txid`):
///   it covers transactions we already have *and* ones we do not. Applying all of
///   it is correct — an LTX file holds one entry per page, the latest version
///   within its range, so re-applying a page we already hold writes identical
///   bytes.
/// - **No contiguous run to HEAD is a gap**, not a partial answer: the chain
///   covering `from_txid + 1` was compacted into a coarser level and
///   retention-pruned, or the local copy predates the oldest retained file.
///   [`SyncError::ChainGap`], distinct from [`SyncError::NoSnapshot`] (nothing to
///   restore at all) and [`SyncError::TxidTooOld`] (a PITR target is unreachable).
fn plan_restore_from(
    files: &[(u32, u64, u64)],
    from_txid: u64,
) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let head = files.iter().map(|(_, _, max)| *max).max().unwrap_or(0);
    if from_txid >= head {
        return Ok(Vec::new());
    }
    let (plan, reached) = greedy_cover(files, from_txid);
    // Reaching short of HEAD means a hole — either nothing starts at
    // `from_txid + 1`, or the run stops partway. Neither is safe to half-apply.
    if reached < head {
        return Err(SyncError::ChainGap { from: from_txid });
    }
    Ok(plan)
}

/// A restore plan reaching as close to `target` as the available files allow,
/// without overshooting it. Filtering to files ending at or before `target`
/// before planning is what lets a fine-grained L0 file be used for an early
/// point even when a later snapshot (whose range extends past `target`) exists.
pub(super) fn plan_restore_to(
    files: &[(u32, u64, u64)],
    target: u64,
) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let filtered: Vec<(u32, u64, u64)> = files
        .iter()
        .copied()
        .filter(|(_, _, max)| *max <= target)
        .collect();
    plan_restore(&filtered)
}

/// A point-in-time restore result: the database image and the TXID it reflects
/// (which may be earlier than requested if the exact point was compacted away).
#[derive(Clone, Debug)]
pub struct RestoreResult {
    pub image: Vec<u8>,
    pub txid: u64,
}

/// Applies a plan (files in TXID order) into a database image.
async fn apply_plan(
    client: &ReplicaClient,
    plan: &[(u32, u64, u64)],
) -> Result<Vec<u8>, SyncError> {
    let mut image: Vec<u8> = Vec::new();
    for &(level, min, max) in plan {
        let bytes = client.get_ltx(level, min, max).await?;
        let file = read_file(&bytes)?;
        let page_size = file.header.page_size as usize;

        image.resize(file.header.commit as usize * page_size, 0);
        for (pgno, data) in file.pages {
            let start = (pgno as usize - 1) * page_size;
            image[start..start + page_size].copy_from_slice(&data);
        }
    }
    Ok(image)
}

/// Reconstructs the latest database image from a replica's LTX chain.
///
/// Buffers the whole image in memory — convenient for tests and small
/// databases. For anything large, prefer [`restore_to_path`], which streams to
/// disk with O(page_size) resident memory.
pub async fn restore(client: &ReplicaClient) -> Result<Vec<u8>, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore(&files)?;
    apply_plan(client, &plan).await
}

/// Reconstructs the latest database image straight onto disk at `path`,
/// returning the TXID it reflects.
///
/// Unlike [`restore`], the full image is never held in memory: pages are
/// `pwrite`-n into a pre-sized file as they decode, so resident memory is
/// O(page_size) plus a `commit`-bit "already written" set (≈ 32 KB per 1 GB of
/// database). Files are applied newest-first — the first writer of each page
/// wins — so each hot page is written exactly once, not once per file that
/// touched it.
pub async fn restore_to_path(client: &ReplicaClient, path: &Path) -> Result<u64, SyncError> {
    let files = list_all_levels(client).await?;
    restore_to_path_with_files(client, &files, path).await
}

/// [`restore_to_path`] with the level listing already in hand, so [`catch_up`]'s
/// fallback doesn't repeat ten LIST requests it just made.
async fn restore_to_path_with_files(
    client: &ReplicaClient,
    files: &[(u32, u64, u64)],
    path: &Path,
) -> Result<u64, SyncError> {
    let plan = plan_restore(files)?;
    apply_plan_to_path(client, &plan, path).await
}

/// Applies a restore plan (files in ascending TXID order) directly to a file,
/// newest-first with a page-dedup set. Returns the restored TXID.
async fn apply_plan_to_path(
    client: &ReplicaClient,
    plan: &[(u32, u64, u64)],
    path: &Path,
) -> Result<u64, SyncError> {
    // The newest file fixes the final database size (a VACUUM may have shrunk it
    // below what an older file carried). One ranged GET of its 100-byte header.
    let &(nl, nmin, nmax) = plan.last().ok_or(SyncError::NoSnapshot)?;
    let head = client
        .get_ltx_range(nl, nmin, nmax, 0, HEADER_SIZE as u64)
        .await?;
    let nh = Header::decode(&head)?;
    let page_size = nh.page_size as usize;
    let commit = nh.commit as usize;
    let file_len = (commit * page_size) as u64;

    // Mark the image in flight before the first byte lands. Until the `Clean`
    // marker replaces this, `path` is a torn image and `catch_up` must rebuild
    // it rather than trust any earlier position.
    write_marker(path, &Marker::Applying { from: 0, to: nmax })?;

    // Pre-size the output; `set_len` zero-fills, which also covers any gaps (the
    // lock page in >1 GiB databases is never encoded and stays zero).
    let out = File::create(path)?;
    out.set_len(file_len)?;

    let mut written = DenseSet(vec![false; commit + 1]); // 1-indexed; [0] unused.
    let lock = lock_pgno(nh.page_size);
    written.claim(lock); // Leave the lock page zero-filled.

    apply_newest_first(client, plan, &out, page_size, commit, &mut written).await?;
    out.sync_all()?;

    write_marker(
        path,
        &Marker::Clean {
            txid: nmax,
            page_size: nh.page_size,
            commit: nh.commit,
            file_len,
        },
    )?;
    Ok(nmax)
}

/// The "already written" set for a newest-first apply: the first file (walking
/// backwards) to carry a page holds its latest version, so every later claim on
/// that page is an older version to skip.
trait PageSet {
    /// Marks `pgno` written, returning `true` only if this is the first claim.
    fn claim(&mut self, pgno: u32) -> bool;
}

/// Full restore: every page in `1..=commit` gets written, so a dense byte per
/// page is bounded by the database anyway and is the cheapest lookup.
struct DenseSet(Vec<bool>);

impl PageSet for DenseSet {
    fn claim(&mut self, pgno: u32) -> bool {
        match self.0.get_mut(pgno as usize) {
            Some(w) if !*w => {
                *w = true;
                true
            }
            _ => false,
        }
    }
}

/// Delta apply: only the delta's own pages are ever claimed, so the set is sized
/// by what changed rather than by the database — a few hundred bytes instead of
/// a 50 MB allocation when a 200 GB database moved 40 KB.
struct SparseSet(HashSet<u32>);

impl PageSet for SparseSet {
    fn claim(&mut self, pgno: u32) -> bool {
        self.0.insert(pgno)
    }
}

/// Walks `plan` newest-first, pwrite-ing each page the first time it is seen.
///
/// Decodes each file page-by-page from its index — never materializing a whole
/// file's pages — so resident memory is O(page_size) plus the page set. Handles
/// both the LZ4 block and frame (litestream) formats. Pages beyond `commit` are
/// dropped: a VACUUM inside the range may have shrunk the database below what an
/// older file in the plan carried.
async fn apply_newest_first(
    client: &ReplicaClient,
    plan: &[(u32, u64, u64)],
    out: &File,
    page_size: usize,
    commit: usize,
    written: &mut impl PageSet,
) -> Result<(), SyncError> {
    for &(level, min, max) in plan.iter().rev() {
        let bytes = client.get_ltx(level, min, max).await?;
        for_each_indexed_page(&bytes, page_size, |pgno, data| {
            let p = pgno as usize;
            if (1..=commit).contains(&p) && written.claim(pgno) {
                out.write_all_at(data, ((p - 1) * page_size) as u64)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Decodes an LTX file's pages one at a time via its page index, invoking `f`
/// with each `(pgno, page_data)`. Only one decompressed page is resident at a
/// time — unlike [`read_file`], which returns every page at once.
fn for_each_indexed_page(
    bytes: &[u8],
    page_size: usize,
    mut f: impl FnMut(u32, &[u8]) -> Result<(), SyncError>,
) -> Result<(), SyncError> {
    let footer = INDEX_FOOTER_SIZE as usize;
    if bytes.len() < HEADER_SIZE + footer {
        return Err(SyncError::Ltx(crate::ltx::LtxError::ShortBuffer {
            need: HEADER_SIZE + footer,
            got: bytes.len(),
        }));
    }
    let index_size_at = bytes.len() - footer;
    let index_len =
        u64::from_be_bytes(bytes[index_size_at..index_size_at + 8].try_into().unwrap()) as usize;
    let index_start = index_size_at - index_len;
    let index = decode_page_index(&bytes[index_start..index_size_at])?;

    for elem in &index {
        let start = elem.offset as usize;
        let end = start + elem.size as usize;
        if end > bytes.len() {
            return Err(SyncError::Ltx(crate::ltx::LtxError::ShortBuffer {
                need: end,
                got: bytes.len(),
            }));
        }
        let (ph, data) = decode_page_frame(&bytes[start..end], page_size)?;
        f(ph.pgno, &data)?;
    }
    Ok(())
}

/// Restores the database as of `target_txid` (point-in-time recovery).
///
/// Reconstructs the newest state whose TXID is at or before `target_txid`,
/// preferring the finest-grained files available (so an exact synced TXID still
/// in L0 restores exactly). If `target_txid` predates the oldest restorable
/// point (e.g. it was compacted into a later snapshot and its L0 file was
/// retention-pruned), returns [`SyncError::TxidTooOld`].
pub async fn restore_to_txid(
    client: &ReplicaClient,
    target_txid: u64,
) -> Result<RestoreResult, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = match plan_restore_to(&files, target_txid) {
        Ok(plan) => plan,
        Err(SyncError::NoSnapshot) => {
            return Err(SyncError::TxidTooOld {
                requested: target_txid,
            });
        }
        Err(e) => return Err(e),
    };
    let Some(&(_, _, txid)) = plan.last() else {
        return Err(SyncError::TxidTooOld {
            requested: target_txid,
        });
    };

    let image = apply_plan(client, &plan).await?;
    Ok(RestoreResult { image, txid })
}

/// Restores the database as of `timestamp_ms` (milliseconds since the Unix
/// epoch), snapping to the newest transaction committed at or before it.
pub async fn restore_to_timestamp(
    client: &ReplicaClient,
    timestamp_ms: i64,
) -> Result<RestoreResult, SyncError> {
    let files = list_all_levels(client).await?;

    // The target TXID is the largest `max_txid` among all files whose header
    // timestamp is at or before the requested time. Each file's timestamp
    // reflects its newest content, so scanning every level (not just the latest
    // restore plan) finds the finest boundary — a recent snapshot's timestamp
    // won't hide the older L0/L1 files that carry earlier points.
    let mut target_txid = 0;
    for &(level, min, max) in &files {
        if max <= target_txid {
            continue;
        }
        let head = client
            .get_ltx_range(level, min, max, 0, HEADER_SIZE as u64)
            .await?;
        if Header::decode(&head)?.timestamp <= timestamp_ms {
            target_txid = max;
        }
    }
    if target_txid == 0 {
        return Err(SyncError::TxidTooOld { requested: 0 });
    }
    restore_to_txid(client, target_txid).await
}

// ---------------------------------------------------------------------------
// The position marker
// ---------------------------------------------------------------------------

/// literstream's record of what a local database image actually *is*, written
/// beside it at `<db>-litestream/RESTORED`.
///
/// The LTX chain carries no rolling checksum, by design (see the module doc), so
/// nothing inside a restored file says which TXID it reflects. This marker is
/// that claim, made by the only party entitled to make it: the code that wrote
/// the bytes.
///
/// Two phases, because a torn apply is worse than no apply. `Applying` is
/// written and fsync'd before the first page write; `Clean` replaces it after
/// `sync_all`. A crash in between leaves `Applying` behind, and the next
/// [`catch_up`] knows the file is a mixture of two images rather than either one
/// — without it, a crash mid-apply leaves a marker saying 481, a file that is
/// neither 481 nor 530, and no way to tell.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "lowercase")]
enum Marker {
    Clean {
        txid: u64,
        page_size: u32,
        commit: u32,
        file_len: u64,
    },
    Applying {
        from: u64,
        to: u64,
    },
}

/// `<db>-litestream/RESTORED` — a sibling of the staging (`ltx/`) and compaction
/// (`compact/`) directories, deliberately not inside either: `recover_staging`
/// walks that tree looking for uploadable files.
fn marker_path(db_path: &Path) -> PathBuf {
    litestream_dir(db_path).join("RESTORED")
}

/// Reads the marker beside `db_path`, or `None` if there isn't a usable one.
///
/// Every failure — absent, unreadable, truncated, or written in a shape we don't
/// understand — collapses to `None`: "we know nothing", so rebuild from scratch.
/// None of them is worth surfacing as an error, because the fallback is always
/// correct and only ever slower.
fn read_marker(db_path: &Path) -> Option<Marker> {
    serde_json::from_slice(&fs::read(marker_path(db_path)).ok()?).ok()
}

/// Writes the marker durably: tmp file → `sync_all` → `rename` → directory
/// fsync, the same recipe `Syncer::stage` uses for staged LTX files. The rename
/// is the commit point, so the marker itself is never observed torn.
fn write_marker(db_path: &Path, marker: &Marker) -> Result<(), SyncError> {
    let final_path = marker_path(db_path);
    let dir = litestream_dir(db_path);
    fs::create_dir_all(&dir)?;
    let tmp = with_extension_suffix(&final_path, ".tmp");

    let mut f = File::create(&tmp)?;
    f.write_all(&serde_json::to_vec(marker).expect("marker serializes"))?;
    f.sync_all()?;
    drop(f);

    fs::rename(&tmp, &final_path)?;
    // Persist the rename (the new directory entry), not just the file data.
    let _ = File::open(&dir).and_then(|d| d.sync_all());
    Ok(())
}

/// The page size an existing database file is using, read from bytes 16..18 of
/// its SQLite header (big-endian, where the value `1` encodes 65536).
///
/// This is what lets [`restore_incremental`] refuse a page-size change even
/// though it has no marker to consult: the file states its own geometry.
/// `None` if the file is missing or too short to have a header.
fn local_page_size(path: &Path) -> Option<u32> {
    let mut hdr = [0u8; 18];
    File::open(path).ok()?.read_exact_at(&mut hdr, 0).ok()?;
    match u16::from_be_bytes([hdr[16], hdr[17]]) {
        1 => Some(65536),
        n => Some(n as u32),
    }
}

// ---------------------------------------------------------------------------
// Incremental restore
// ---------------------------------------------------------------------------

/// The shape a delta will leave the file in, from its files' headers alone.
#[derive(Clone, Copy, Debug)]
struct DeltaShape {
    page_size: u32,
    /// The newest header's `commit` — the database size once the delta lands.
    final_commit: u32,
    /// The smallest `commit` anywhere in the plan: the low-water mark the file
    /// passed through, which is what makes a VACUUM inside the delta safe.
    min_commit: u32,
}

/// Reads every plan file's 100-byte header — one ranged GET each — before
/// anything on disk is touched.
///
/// Cost is O(delta files), which is exactly the scaling the feature promises.
/// Doing it up front is what lets the applier resize correctly and reject a
/// page-size change *before* it has written a single misaligned page.
async fn delta_shape(
    client: &ReplicaClient,
    plan: &[(u32, u64, u64)],
) -> Result<DeltaShape, SyncError> {
    let mut page_size = 0;
    let mut final_commit = 0;
    let mut min_commit = u32::MAX;
    for &(level, min, max) in plan {
        let head = client
            .get_ltx_range(level, min, max, 0, HEADER_SIZE as u64)
            .await?;
        let h = Header::decode(&head)?;
        if page_size != 0 && h.page_size != page_size {
            return Err(SyncError::Ltx(crate::ltx::LtxError::InvalidPageSize(
                h.page_size,
            )));
        }
        page_size = h.page_size;
        final_commit = h.commit; // plan is ascending, so the last write wins
        min_commit = min_commit.min(h.commit);
    }
    Ok(DeltaShape {
        page_size,
        final_commit,
        min_commit,
    })
}

/// Applies a delta plan onto the existing image at `path`.
///
/// Where a full restore gets its file geometry free from `File::create` +
/// `set_len`, this has to establish it deliberately:
///
/// - **Resize before applying**, in two steps. Truncating to the delta's
///   *minimum* commit discards everything past the low-water mark the database
///   passed through; growing to the final commit then zero-fills the tail, so
///   untouched pages read as zero — matching full restore, and covering the lock
///   page when the database crosses 1 GiB within the delta. The truncate handles
///   an outright shrink (a VACUUM at the end of the delta) and, as insurance,
///   re-zeroes a region that shrank and grew back *inside* the delta. That second
///   case needs a page that changed without being written, which SQLite does not
///   do — the step costs one `ftruncate` and removes the need to rely on that.
/// - **Never truncate the whole file.** `OpenOptions::write`, not
///   `File::create`: the pages we don't touch are the point.
async fn apply_delta_to_path(
    client: &ReplicaClient,
    plan: &[(u32, u64, u64)],
    path: &Path,
    from_txid: u64,
    shape: DeltaShape,
) -> Result<u64, SyncError> {
    let to_txid = plan.last().expect("delta plan is non-empty").2;
    let page_size = shape.page_size as usize;
    let commit = shape.final_commit as usize;
    let file_len = (commit * page_size) as u64;

    // From here until the `Clean` marker lands, `path` is a mixture of two
    // images. Say so on disk, and fsync it, before touching a byte.
    write_marker(
        path,
        &Marker::Applying {
            from: from_txid,
            to: to_txid,
        },
    )?;

    let out = OpenOptions::new().write(true).open(path)?;
    out.set_len(shape.min_commit as u64 * page_size as u64)?;
    out.set_len(file_len)?;

    let mut written = SparseSet(HashSet::new());
    written.claim(lock_pgno(shape.page_size)); // Leave the lock page zero-filled.

    apply_newest_first(client, plan, &out, page_size, commit, &mut written).await?;
    out.sync_all()?;

    write_marker(
        path,
        &Marker::Clean {
            txid: to_txid,
            page_size: shape.page_size,
            commit: shape.final_commit,
            file_len,
        },
    )?;
    Ok(to_txid)
}

/// Applies every LTX file after `from_txid` onto the existing image at `path`,
/// returning the TXID it now reflects.
///
/// **`from_txid` is taken on faith.** Nothing in the chain can confirm that the
/// file really is at that transaction, and a value that is too high silently
/// skips the files carrying the missing pages — leaving a database that decodes,
/// opens, and answers queries wrongly. The one thing this *can* check, it does:
/// the local file's own SQLite header must agree with the delta's page size.
///
/// Errors with [`SyncError::ChainGap`] rather than falling back. For callers that
/// track position durably themselves, and for tests; **most callers want
/// [`catch_up`]**, which reads literstream's own marker and handles the gap case.
///
/// The image must not be open in SQLite. Catching up a database with a live
/// connection on it is a different and much harder problem.
pub async fn restore_incremental(
    client: &ReplicaClient,
    path: &Path,
    from_txid: u64,
) -> Result<u64, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore_from(&files, from_txid)?;
    if plan.is_empty() {
        return Ok(from_txid); // already at HEAD
    }

    let shape = delta_shape(client, &plan).await?;
    // Applying 4096-byte pages at 8192-byte offsets yields a file that is
    // structurally plausible and entirely wrong, and `PRAGMA page_size` +
    // `VACUUM` is all it takes to get there. Refuse rather than guess.
    let local = local_page_size(path)
        .ok_or(SyncError::Ltx(crate::ltx::LtxError::InvalidPageSize(0)))?;
    if local != shape.page_size {
        return Err(SyncError::Ltx(crate::ltx::LtxError::InvalidPageSize(
            shape.page_size,
        )));
    }

    apply_delta_to_path(client, &plan, path, from_txid, shape).await
}

/// What [`catch_up`] actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUp {
    /// Already current. Ten LIST requests, zero GETs, nothing written.
    UpToDate { txid: u64 },
    /// Applied `files` delta files to reach `to`.
    Incremental { from: u64, to: u64, files: usize },
    /// The delta path was not usable; rebuilt the whole image instead.
    FullRestore { to: u64, reason: FallbackReason },
}

/// Why [`catch_up`] rebuilt instead of applying a delta. Informational — every
/// one of these still produces a correct image, just an expensive one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    /// No position marker beside the file (including: no file at all).
    NoMarker,
    /// A previous apply was interrupted, so the image is torn.
    DirtyMarker,
    /// The file is not the size the marker claims — something else touched it.
    LengthMismatch,
    /// No contiguous run of files bridges the local TXID to HEAD.
    ChainGap,
    /// The delta is at a different page size, so every byte offset differs.
    PageSizeChanged,
    /// The marker is ahead of the replica: it was reset, rewound, or is a
    /// different replica than the one this image came from.
    LocalAhead,
}

impl fmt::Display for CatchUp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatchUp::UpToDate { txid } => write!(f, "up to date at txid {txid}"),
            CatchUp::Incremental { from, to, files } => {
                write!(f, "applied {files} file(s), txid {from} -> {to}")
            }
            CatchUp::FullRestore { to, reason } => {
                write!(f, "full restore to txid {to} ({reason})")
            }
        }
    }
}

impl fmt::Display for FallbackReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FallbackReason::NoMarker => "no position marker",
            FallbackReason::DirtyMarker => "previous apply was interrupted",
            FallbackReason::LengthMismatch => "file length does not match the marker",
            FallbackReason::ChainGap => "no contiguous chain from the local txid",
            FallbackReason::PageSizeChanged => "page size changed",
            FallbackReason::LocalAhead => "local copy is ahead of the replica",
        };
        f.write_str(s)
    }
}

/// Makes the database at `path` current, cheaply when it can.
///
/// Reads the position marker literstream wrote beside `path` and falls back to a
/// full [`restore_to_path`] whenever the incremental path is not *provably* safe
/// — including when there is no local file at all, so this works as the only
/// restore call a caller ever makes.
///
/// That the gap case is handled here is the whole point. Every caller would
/// otherwise write the same fallback arm, and the one that forgets ships a bug
/// that only surfaces after a compaction. [`CatchUp`] reports which branch ran so
/// callers can log it.
///
/// The image must not be open in SQLite — this writes to a file nobody holds.
pub async fn catch_up(client: &ReplicaClient, path: &Path) -> Result<CatchUp, SyncError> {
    let files = list_all_levels(client).await?;
    let head = files.iter().map(|(_, _, max)| *max).max().unwrap_or(0);

    let Some(marker) = read_marker(path) else {
        return full_restore(client, &files, path, FallbackReason::NoMarker).await;
    };
    let Marker::Clean {
        txid,
        page_size,
        file_len,
        ..
    } = marker
    else {
        return full_restore(client, &files, path, FallbackReason::DirtyMarker).await;
    };

    // The marker describes a file; check it still describes *this* file. A weak
    // test — an in-place edit of the same length passes — but it catches what
    // actually happens: a truncation, an append, a different file entirely. A
    // stronger check is blocked on the chain carrying no checksum, which is also
    // why the marker has to carry `file_len` at all.
    if fs::metadata(path).ok().map(|m| m.len()) != Some(file_len) {
        return full_restore(client, &files, path, FallbackReason::LengthMismatch).await;
    }
    if txid > head {
        return full_restore(client, &files, path, FallbackReason::LocalAhead).await;
    }
    if txid == head {
        return Ok(CatchUp::UpToDate { txid });
    }

    let plan = match plan_restore_from(&files, txid) {
        Ok(plan) => plan,
        Err(SyncError::ChainGap { .. }) => {
            return full_restore(client, &files, path, FallbackReason::ChainGap).await;
        }
        Err(e) => return Err(e),
    };
    if plan.is_empty() {
        return Ok(CatchUp::UpToDate { txid });
    }

    let shape = delta_shape(client, &plan).await?;
    if shape.page_size != page_size || local_page_size(path) != Some(page_size) {
        return full_restore(client, &files, path, FallbackReason::PageSizeChanged).await;
    }

    let n = plan.len();
    let to = apply_delta_to_path(client, &plan, path, txid, shape).await?;
    Ok(CatchUp::Incremental {
        from: txid,
        to,
        files: n,
    })
}

/// The fallback arm of [`catch_up`], reusing the listing already in hand.
async fn full_restore(
    client: &ReplicaClient,
    files: &[(u32, u64, u64)],
    path: &Path,
    reason: FallbackReason,
) -> Result<CatchUp, SyncError> {
    let to = restore_to_path_with_files(client, files, path).await?;
    Ok(CatchUp::FullRestore { to, reason })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(level, min_txid, max_txid)`, the shape `list_all_levels` returns.
    fn f(level: u32, min: u64, max: u64) -> (u32, u64, u64) {
        (level, min, max)
    }

    /// A snapshot at 1..=100, then L0 singletons 101..=105.
    fn chain() -> Vec<(u32, u64, u64)> {
        vec![
            f(9, 1, 100),
            f(0, 101, 101),
            f(0, 102, 102),
            f(0, 103, 103),
            f(0, 104, 104),
            f(0, 105, 105),
        ]
    }

    #[test]
    fn delta_covers_only_what_changed() {
        let plan = plan_restore_from(&chain(), 102).unwrap();
        assert_eq!(plan, vec![f(0, 103, 103), f(0, 104, 104), f(0, 105, 105)]);
    }

    #[test]
    fn at_head_plans_nothing() {
        // The common warm case: nobody wrote while we were away.
        assert_eq!(plan_restore_from(&chain(), 105).unwrap(), vec![]);
        // A marker ahead of the replica is the caller's problem, not a gap —
        // `catch_up` turns this into a LocalAhead fallback.
        assert_eq!(plan_restore_from(&chain(), 200).unwrap(), vec![]);
        assert_eq!(plan_restore_from(&[], 0).unwrap(), vec![]);
    }

    #[test]
    fn first_file_may_straddle_the_local_position() {
        // An L1 file covering 101..=105 begins *before* where we are. Taking it
        // whole is correct: it holds the latest version of each page in its
        // range, so the transactions we already have re-apply identically.
        let mut files = chain();
        files.push(f(1, 101, 105));
        let plan = plan_restore_from(&files, 103).unwrap();
        assert_eq!(plan, vec![f(1, 101, 105)]);
    }

    #[test]
    fn coarser_levels_win_ties() {
        // Two files reach 105; the compacted one is one GET instead of three.
        let mut files = chain();
        files.push(f(2, 101, 105));
        files.push(f(1, 101, 105));
        assert_eq!(plan_restore_from(&files, 100).unwrap(), vec![f(2, 101, 105)]);
    }

    #[test]
    fn a_hole_in_the_chain_is_a_gap() {
        // 103 was compacted into a coarser level and retention-pruned.
        let files: Vec<_> = chain().into_iter().filter(|f| f.1 != 103).collect();
        assert!(matches!(
            plan_restore_from(&files, 102),
            Err(SyncError::ChainGap { from: 102 })
        ));
        // The local copy predates the oldest retained file: the snapshot starts
        // at 50, so nothing carries what happened before it.
        let pruned = vec![f(9, 50, 100), f(0, 101, 101)];
        assert!(matches!(
            plan_restore_from(&pruned, 20),
            Err(SyncError::ChainGap { from: 20 })
        ));
    }

    #[test]
    fn a_run_that_stops_short_of_head_is_a_gap() {
        // Covers 101..=103, then a hole, then a file at 105 nothing reaches.
        let files = vec![f(0, 101, 101), f(0, 102, 102), f(0, 103, 103), f(0, 105, 105)];
        assert!(matches!(
            plan_restore_from(&files, 100),
            Err(SyncError::ChainGap { from: 100 })
        ));
    }

    #[test]
    fn full_and_delta_planners_share_one_walk() {
        // `plan_restore` is `plan_restore_from` anchored at 0, plus the
        // must-start-at-1 rule. Planning from 0 over a complete chain gives the
        // same files as a full restore.
        let files = chain();
        assert_eq!(
            plan_restore(&files).unwrap(),
            greedy_cover(&files, 0).0,
            "full plan is the greedy cover from zero"
        );
    }

    #[test]
    fn marker_round_trips_through_json() {
        let clean = Marker::Clean { txid: 481, page_size: 4096, commit: 12000, file_len: 49152000 };
        let json = serde_json::to_string(&clean).unwrap();
        assert_eq!(
            json,
            r#"{"state":"clean","txid":481,"page_size":4096,"commit":12000,"file_len":49152000}"#
        );
        assert_eq!(serde_json::from_str::<Marker>(&json).unwrap(), clean);

        let applying = Marker::Applying { from: 481, to: 530 };
        let json = serde_json::to_string(&applying).unwrap();
        assert_eq!(json, r#"{"state":"applying","from":481,"to":530}"#);
        assert_eq!(serde_json::from_str::<Marker>(&json).unwrap(), applying);

        // Anything we don't understand is "no marker", never a hard error.
        assert!(serde_json::from_str::<Marker>(r#"{"state":"future"}"#).is_err());
    }
}
