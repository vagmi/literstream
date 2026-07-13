//! The WAL→LTX sync engine (Phases 3–4): monitor a live database's WAL and turn
//! newly-committed frames into an immutable LTX chain in object storage.
//!
//! It ties together the pure-bytes pieces — [`crate::db`] controls SQLite and
//! checkpoints, [`crate::wal`] reads committed frames, [`crate::ltx`] encodes
//! them — and stores the result through a [`ReplicaClient`] over any
//! `object_store` backend (in-memory, local disk, S3/Garage, GCS).
//!
//! Async shape: the SQLite/WAL read and LTX encoding happen synchronously under
//! a pinned read lock; the lock is then released and the resulting bytes are
//! `await`-uploaded. Replication position advances only after a successful
//! upload, so a failed upload is retried from the same point.
//!
//! Model (as litestream): each sync = one L0 file at `TXID = prev+1`
//! (`MinTXID == MaxTXID`); the first sync writes a `MinTXID=1` snapshot, later
//! syncs only the changed pages. Files carry `HeaderFlagNoChecksum` and LZ4
//! *frame*-compressed pages — matching litestream (ltx v0.5.1) exactly, so the
//! real litestream binary restores our replicas and we restore its.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::db::{CheckpointMode, CheckpointResult, Db};
use crate::lock::ProcessLock;
use crate::ltx::{
    Checksum, Decoder, Encoder, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE, Header, compact, lock_pgno,
    read_file,
};
use crate::storage::{PutOutcome, ReplicaClient};
use crate::wal::{PageMap, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalHeader, WalReader};

mod error;
mod reader;
pub use error::SyncError;
pub use reader::ReplicaReader;

/// Default WAL-frame threshold for a PASSIVE checkpoint (~4 MB @ 4 KB).
pub const DEFAULT_MIN_CHECKPOINT_FRAMES: u64 = 1000;
/// Default WAL-frame threshold for an emergency TRUNCATE checkpoint.
pub const DEFAULT_TRUNCATE_FRAMES: u64 = 10_000;

/// L0 is the raw, uncompacted level.
const LEVEL0: u32 = 0;
/// Level 9 holds full snapshots (matches litestream's `SnapshotLevel`), where we
/// write the compacted base so litestream's restore can use it as the base.
const SNAPSHOT_LEVEL: u32 = 9;
/// Highest level scanned during restore/resume — covers litestream's levels 0–8
/// plus the snapshot level 9.
const MAX_LEVEL: u32 = 9;

/// What a [`Syncer::compact`] produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionInfo {
    /// New compacted base TXID range (`1..=max_txid`).
    pub min_txid: u64,
    pub max_txid: u64,
    /// Number of input files merged.
    pub inputs: usize,
    /// Number of superseded files deleted.
    pub pruned: usize,
}

/// What a single [`Syncer::sync`] produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncOutcome {
    /// No new committed WAL frames; nothing written.
    Skipped,
    /// A full snapshot (`MinTXID == 1`, all pages) was written.
    Snapshot { txid: u64, pages: u32 },
    /// An incremental file with the changed pages was written.
    Incremental { txid: u64, pages: usize },
}

/// In-memory replication position (Phase 5 will persist/resume this).
#[derive(Clone, Copy, Debug, Default)]
struct Position {
    txid: u64,
    wal_offset: u64,
    salt1: u32,
    salt2: u32,
    synced_to_wal_end: bool,
}

enum Plan {
    Skip,
    Snapshot { offset: u64 },
    Incremental { offset: u64, salt1: u32, salt2: u32 },
}

/// An LTX file built in memory, ready to upload, along with the state changes to
/// commit once the upload succeeds.
struct BuiltFile {
    outcome: SyncOutcome,
    min_txid: u64,
    max_txid: u64,
    bytes: Vec<u8>,
    new_pos: Position,
}

/// Replicates a [`Db`]'s WAL to an object-store replica.
pub struct Syncer {
    db: Db,
    client: ReplicaClient,
    pos: Position,
    /// Held for the syncer's lifetime to enforce a single host-local writer.
    _lock: ProcessLock,
    /// PASSIVE checkpoint when the WAL reaches this many frames.
    pub min_checkpoint_frames: u64,
    /// TRUNCATE checkpoint at this many frames (0 disables).
    pub truncate_frames: u64,
}

impl Syncer {
    /// Opens a syncer over `client`, resuming from any existing chain there.
    ///
    /// Takes a host-local single-writer lock on the database; fails with
    /// [`SyncError::Lock`] if another literstream process holds it.
    pub async fn open(db: Db, client: ReplicaClient) -> Result<Syncer, SyncError> {
        let lock = ProcessLock::acquire(db.path())?;
        let pos = derive_position(&client).await?;
        Ok(Syncer {
            db,
            client,
            pos,
            _lock: lock,
            min_checkpoint_frames: DEFAULT_MIN_CHECKPOINT_FRAMES,
            truncate_frames: DEFAULT_TRUNCATE_FRAMES,
        })
    }

    /// Syncs repeatedly until no new frames remain, returning the number of LTX
    /// files written. Use this to drain before shutdown.
    pub async fn flush(&mut self) -> Result<u32, SyncError> {
        let mut written = 0;
        while self.sync().await? != SyncOutcome::Skipped {
            written += 1;
        }
        Ok(written)
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn client(&self) -> &ReplicaClient {
        &self.client
    }

    pub fn position_txid(&self) -> u64 {
        self.pos.txid
    }

    /// Performs one sync cycle: build the LTX under a pinned read lock, then
    /// upload it and advance the position.
    pub async fn sync(&mut self) -> Result<SyncOutcome, SyncError> {
        self.db.acquire_read_lock()?;
        let built = self.build_under_lock();
        let _ = self.db.release_read_lock();

        let Some(b) = built? else {
            return Ok(SyncOutcome::Skipped);
        };

        // Guard against split-brain: never overwrite a different LTX already at
        // this TXID. Identical bytes = an idempotent retry of our own upload.
        match self
            .client
            .put_ltx_cas(LEVEL0, b.min_txid, b.max_txid, Bytes::from(b.bytes))
            .await?
        {
            PutOutcome::Created | PutOutcome::AlreadyIdentical => {}
            PutOutcome::Conflict => return Err(SyncError::Equivocation { txid: b.max_txid }),
        }

        self.pos = b.new_pos;
        Ok(b.outcome)
    }

    /// Reads the WAL/DB and builds the next LTX file (no network I/O).
    fn build_under_lock(&self) -> Result<Option<BuiltFile>, SyncError> {
        let mut wal_path = self.db.path().to_path_buf().into_os_string();
        wal_path.push("-wal");
        let wal = fs::read(&wal_path).unwrap_or_default();

        match self.plan(&wal)? {
            Plan::Skip => Ok(None),
            Plan::Snapshot { offset } => Ok(Some(self.build_snapshot(offset, &wal)?)),
            Plan::Incremental {
                offset,
                salt1,
                salt2,
            } => self.build_incremental(offset, salt1, salt2, &wal),
        }
    }

    fn plan(&self, wal: &[u8]) -> Result<Plan, SyncError> {
        if self.pos.txid == 0 {
            return Ok(Plan::Snapshot {
                offset: WAL_HEADER_SIZE as u64,
            });
        }
        if wal.len() < WAL_HEADER_SIZE {
            return Ok(if self.pos.synced_to_wal_end {
                Plan::Skip
            } else {
                Plan::Snapshot {
                    offset: WAL_HEADER_SIZE as u64,
                }
            });
        }

        let header = WalHeader::parse(wal)?;
        let wal_size = wal.len() as u64;
        let salt_match = (header.salt1, header.salt2) == (self.pos.salt1, self.pos.salt2);

        if self.pos.wal_offset > wal_size || !salt_match {
            return Ok(if self.pos.synced_to_wal_end {
                Plan::Incremental {
                    offset: WAL_HEADER_SIZE as u64,
                    salt1: header.salt1,
                    salt2: header.salt2,
                }
            } else {
                Plan::Snapshot {
                    offset: WAL_HEADER_SIZE as u64,
                }
            });
        }

        Ok(Plan::Incremental {
            offset: self.pos.wal_offset,
            salt1: self.pos.salt1,
            salt2: self.pos.salt2,
        })
    }

    fn build_snapshot(&self, offset: u64, wal: &[u8]) -> Result<BuiltFile, SyncError> {
        let page_size = self.db.page_size() as usize;
        let db_bytes = fs::read(self.db.path())?;

        let wal_header = (wal.len() >= WAL_HEADER_SIZE)
            .then(|| WalHeader::parse(wal))
            .transpose()?;
        let page_map = match wal_header {
            Some(_) => WalReader::new_at_offset(wal, offset)?.page_map(),
            None => PageMap::default(),
        };
        let (salt1, salt2) = wal_header.map(|h| (h.salt1, h.salt2)).unwrap_or((0, 0));

        let commit = if page_map.commit > 0 {
            page_map.commit
        } else {
            (db_bytes.len() / page_size) as u32
        };
        let wal_size = page_map.end_offset.saturating_sub(offset);
        let txid = self.pos.txid + 1;
        let lock = lock_pgno(page_size as u32);

        let mut buf = Vec::new();
        {
            let mut enc = Encoder::new(&mut buf);
            enc.encode_header(no_checksum_header(
                page_size as u32,
                commit,
                txid,
                offset,
                wal_size,
                salt1,
                salt2,
            ))?;
            for pgno in 1..=commit {
                if pgno == lock {
                    continue;
                }
                let data = match page_map.pages.get(&pgno) {
                    Some(&off) => wal_page(wal, off, page_size),
                    None => {
                        let start = (pgno as usize - 1) * page_size;
                        &db_bytes[start..start + page_size]
                    }
                };
                enc.encode_page(pgno, data)?;
            }
            enc.finish()?;
        }

        Ok(BuiltFile {
            outcome: SyncOutcome::Snapshot {
                txid,
                pages: commit,
            },
            min_txid: txid,
            max_txid: txid,
            bytes: buf,
            new_pos: Position {
                txid,
                wal_offset: offset + wal_size,
                salt1,
                salt2,
                synced_to_wal_end: offset + wal_size == wal.len() as u64,
            },
        })
    }

    fn build_incremental(
        &self,
        offset: u64,
        salt1: u32,
        salt2: u32,
        wal: &[u8],
    ) -> Result<Option<BuiltFile>, SyncError> {
        let page_size = self.db.page_size() as usize;
        let page_map = WalReader::new_at_offset(wal, offset)?.page_map();
        if page_map.pages.is_empty() {
            return Ok(None);
        }

        let wal_size = page_map.end_offset - offset;
        let commit = page_map.commit;
        let txid = self.pos.txid + 1;

        let mut pgnos: Vec<u32> = page_map.pages.keys().copied().collect();
        pgnos.sort_unstable();

        let mut buf = Vec::new();
        {
            let mut enc = Encoder::new(&mut buf);
            enc.encode_header(no_checksum_header(
                page_size as u32,
                commit,
                txid,
                offset,
                wal_size,
                salt1,
                salt2,
            ))?;
            for &pgno in &pgnos {
                let data = wal_page(wal, page_map.pages[&pgno], page_size);
                enc.encode_page(pgno, data)?;
            }
            enc.finish()?;
        }

        let final_offset = offset + wal_size;
        Ok(Some(BuiltFile {
            outcome: SyncOutcome::Incremental {
                txid,
                pages: pgnos.len(),
            },
            min_txid: txid,
            max_txid: txid,
            bytes: buf,
            new_pos: Position {
                txid,
                wal_offset: final_offset,
                salt1,
                salt2,
                synced_to_wal_end: final_offset == wal.len() as u64,
            },
        }))
    }

    /// Applies the 3-tier checkpoint strategy based on WAL frame count. Sync
    /// before calling this so the frames being checkpointed are already stored.
    pub fn checkpoint_if_needed(
        &mut self,
    ) -> Result<Option<(CheckpointMode, CheckpointResult)>, SyncError> {
        let frames = self.db.wal_frame_count();
        let mode = if self.truncate_frames > 0 && frames >= self.truncate_frames {
            CheckpointMode::Truncate
        } else if frames >= self.min_checkpoint_frames {
            CheckpointMode::Passive
        } else {
            return Ok(None);
        };
        Ok(Some((mode, self.db.checkpoint(mode)?)))
    }

    /// Compacts the L0 chain into a single L1 base, pruning the merged files —
    /// bounding storage and restore length.
    ///
    /// The newest L0 file is deliberately *kept* uncompacted: compaction zeroes
    /// the WAL offset/salts, and that most recent file is what a restart reads to
    /// recover the WAL resume position. Returns `None` if there's nothing worth
    /// compacting (fewer than two uncompacted L0 files).
    pub async fn compact(&self) -> Result<Option<CompactionInfo>, SyncError> {
        let l0 = self.client.list_ltx(LEVEL0).await?;
        let l1 = self.client.list_ltx(SNAPSHOT_LEVEL).await?;

        let base = l1.iter().find(|f| f.min_txid == 1).copied();
        let base_max = base.map(|f| f.max_txid).unwrap_or(0);

        // L0 files not yet folded into the base, oldest-first; keep the newest.
        let new_l0: Vec<_> = l0
            .iter()
            .filter(|f| f.min_txid > base_max)
            .copied()
            .collect();
        if new_l0.len() < 2 {
            return Ok(None);
        }
        let to_merge = &new_l0[..new_l0.len() - 1];
        let new_max = to_merge.last().unwrap().max_txid;

        // Fetch inputs in TXID order: the existing base, then the L0 run.
        let mut buffers: Vec<Vec<u8>> = Vec::new();
        if let Some(b) = base {
            let bytes = self
                .client
                .get_ltx(SNAPSHOT_LEVEL, b.min_txid, b.max_txid)
                .await?;
            buffers.push(bytes.to_vec());
        }
        for f in to_merge {
            let bytes = self.client.get_ltx(LEVEL0, f.min_txid, f.max_txid).await?;
            buffers.push(bytes.to_vec());
        }

        let refs: Vec<&[u8]> = buffers.iter().map(|v| v.as_slice()).collect();
        let inputs = refs.len();
        let merged = compact(&refs)?;

        // Publish the new base, then delete what it supersedes.
        self.client
            .put_ltx(SNAPSHOT_LEVEL, 1, new_max, Bytes::from(merged))
            .await?;
        let mut pruned = 0;
        if let Some(b) = base {
            self.client
                .delete_ltx(SNAPSHOT_LEVEL, b.min_txid, b.max_txid)
                .await?;
            pruned += 1;
        }
        for f in to_merge {
            self.client
                .delete_ltx(LEVEL0, f.min_txid, f.max_txid)
                .await?;
            pruned += 1;
        }

        Ok(Some(CompactionInfo {
            min_txid: 1,
            max_txid: new_max,
            inputs,
            pruned,
        }))
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn wal_page(wal: &[u8], frame_offset: u64, page_size: usize) -> &[u8] {
    let start = frame_offset as usize + WAL_FRAME_HEADER_SIZE;
    &wal[start..start + page_size]
}

/// A litestream-compatible LTX header: checksum tracking disabled
/// (`HeaderFlagNoChecksum`, pre-apply = 0). litestream restore rejects files
/// that carry a rolling checksum, so we omit it — the WAL frame checksums and
/// the LTX file checksum still protect integrity.
#[allow(clippy::too_many_arguments)]
fn no_checksum_header(
    page_size: u32,
    commit: u32,
    txid: u64,
    wal_offset: u64,
    wal_size: u64,
    salt1: u32,
    salt2: u32,
) -> Header {
    Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit,
        min_txid: txid,
        max_txid: txid,
        timestamp: now_ms(),
        pre_apply_checksum: Checksum::ZERO,
        wal_offset: wal_offset as i64,
        wal_size: wal_size as i64,
        wal_salt1: salt1,
        wal_salt2: salt2,
        node_id: 0,
    }
}

/// Lists every LTX file across all levels as `(level, min_txid, max_txid)`.
async fn list_all_levels(client: &ReplicaClient) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let mut files = Vec::new();
    for level in 0..=MAX_LEVEL {
        for f in client.list_ltx(level).await? {
            files.push((level, f.min_txid, f.max_txid));
        }
    }
    Ok(files)
}

/// Reads the highest-TXID LTX header to recover the resume position. The newest
/// file is a kept L0 (compaction keeps it), so it carries the WAL position.
async fn derive_position(client: &ReplicaClient) -> Result<Position, SyncError> {
    let files = list_all_levels(client).await?;
    let Some(&(level, min, max)) = files.iter().max_by_key(|(_, _, max)| *max) else {
        return Ok(Position::default());
    };
    let bytes = client.get_ltx(level, min, max).await?;
    let mut dec = Decoder::new(&bytes[..]);
    let header = dec.decode_header()?;
    Ok(Position {
        txid: max,
        wal_offset: (header.wal_offset + header.wal_size) as u64,
        salt1: header.wal_salt1,
        salt2: header.wal_salt2,
        synced_to_wal_end: false,
    })
}

/// A greedy restore plan: starting from TXID 1, repeatedly pick the file that
/// begins contiguously and reaches the furthest, preferring higher (compacted)
/// levels on ties. This uses the fewest files to cover the whole range.
fn plan_restore(files: &[(u32, u64, u64)]) -> Result<Vec<(u32, u64, u64)>, SyncError> {
    let mut plan: Vec<(u32, u64, u64)> = Vec::new();
    let mut pos: u64 = 0;
    while let Some(&next) = files
        .iter()
        .filter(|(_, min, max)| *min <= pos + 1 && *max > pos)
        .max_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)))
    {
        pos = next.2;
        plan.push(next);
    }
    if plan.first().map(|(_, min, _)| *min) != Some(1) {
        return Err(SyncError::NoSnapshot);
    }
    Ok(plan)
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
pub async fn restore(client: &ReplicaClient) -> Result<Vec<u8>, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore(&files)?;
    apply_plan(client, &plan).await
}

/// Restores the database as of `target_txid` (point-in-time recovery).
///
/// Applies every plan file whose range ends at or before `target_txid`. The
/// result reflects the largest such boundary; if `target_txid` predates the
/// oldest retained file (e.g. compacted away), returns [`SyncError::TxidTooOld`].
pub async fn restore_to_txid(
    client: &ReplicaClient,
    target_txid: u64,
) -> Result<RestoreResult, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore(&files)?;

    let selected: Vec<(u32, u64, u64)> = plan
        .into_iter()
        .take_while(|(_, _, max)| *max <= target_txid)
        .collect();
    let Some(&(_, _, txid)) = selected.last() else {
        return Err(SyncError::TxidTooOld {
            requested: target_txid,
        });
    };

    let image = apply_plan(client, &selected).await?;
    Ok(RestoreResult { image, txid })
}

/// Restores the database as of `timestamp_ms` (milliseconds since the Unix
/// epoch), snapping to the newest transaction committed at or before it.
pub async fn restore_to_timestamp(
    client: &ReplicaClient,
    timestamp_ms: i64,
) -> Result<RestoreResult, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore(&files)?;

    // Plan files are TXID-ordered and their timestamps are monotonic.
    let mut target_txid = 0;
    for &(level, min, max) in &plan {
        let head = client
            .get_ltx_range(level, min, max, 0, HEADER_SIZE as u64)
            .await?;
        let header = Header::decode(&head)?;
        if header.timestamp <= timestamp_ms {
            target_txid = max;
        } else {
            break;
        }
    }
    if target_txid == 0 {
        return Err(SyncError::TxidTooOld { requested: 0 });
    }
    restore_to_txid(client, target_txid).await
}
