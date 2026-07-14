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
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::db::{CheckpointMode, CheckpointResult, Db};
use crate::lock::ProcessLock;
use crate::ltx::{
    Checksum, Decoder, Encoder, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE, Header, compact, lock_pgno,
    read_file,
};
use crate::storage::{PutOutcome, ReplicaClient};
use crate::wal::{
    PageMap, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalError, WalFrameHeader, WalHeader, WalReader,
};

mod driver;
mod error;
mod level;
mod reader;
mod retention;
pub use driver::{
    DEFAULT_L0_RETENTION, DEFAULT_L0_RETENTION_CHECK_INTERVAL, DEFAULT_SNAPSHOT_INTERVAL,
    DEFAULT_SNAPSHOT_RETENTION, Driver, TickReport,
};
pub use error::SyncError;
pub use level::{CompactionLevel, CompactionLevels, SNAPSHOT_LEVEL};
pub use reader::ReplicaReader;

/// Default WAL-frame growth before a checkpoint (~4 MB @ 4 KB).
pub const DEFAULT_MIN_CHECKPOINT_FRAMES: u64 = 1000;

/// L0 is the raw, uncompacted level.
const LEVEL0: u32 = 0;
/// Highest level scanned during restore/resume — covers litestream's levels 0–8
/// plus the snapshot level 9 ([`SNAPSHOT_LEVEL`]).
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
    /// Checkpoint when the WAL grows this many frames past the last checkpoint.
    pub min_checkpoint_frames: u64,
    /// WAL frame count (file high-water) right after the last checkpoint. PASSIVE
    /// checkpoints don't truncate the `-wal` file, so `wal_frame_count()` reflects
    /// the high-water, not live frames; we gate the next PASSIVE on growth beyond
    /// this baseline to avoid checkpointing (and re-snapshotting) every tick.
    checkpoint_baseline_frames: u64,
    /// Set when a checkpoint folded frames into the DB that we hadn't replicated
    /// yet (they now live only in the DB file). The next sync re-snapshots from
    /// the DB to recover them; cleared once that snapshot succeeds.
    pending_resync: bool,
}

impl Syncer {
    /// Opens a syncer over `client`, resuming from any existing chain there.
    ///
    /// Takes a host-local single-writer lock on the database; fails with
    /// [`SyncError::Lock`] if another literstream process holds it.
    pub async fn open(db: Db, client: ReplicaClient) -> Result<Syncer, SyncError> {
        let lock = ProcessLock::acquire(db.path())?;
        let pos = derive_position(&client).await?;
        let mut db = db;
        // Pin the WAL read-mark for the syncer's lifetime so an external
        // connection's checkpoint (e.g. on close) can't reset the WAL and
        // recycle frames we haven't replicated yet. Our own checkpoints
        // release and re-acquire it (see `Db::checkpoint`).
        db.acquire_read_lock()?;
        Ok(Syncer {
            db,
            client,
            pos,
            _lock: lock,
            min_checkpoint_frames: DEFAULT_MIN_CHECKPOINT_FRAMES,
            checkpoint_baseline_frames: 0,
            pending_resync: false,
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

    /// Performs one sync cycle: build the LTX under the pinned read lock, then
    /// upload it and advance the position. The read-mark is held continuously
    /// across syncs (acquired at [`Syncer::open`]); this call re-acquires it
    /// only if a checkpoint released it, and deliberately does not release it.
    pub async fn sync(&mut self) -> Result<SyncOutcome, SyncError> {
        match self.build_next()? {
            None => Ok(SyncOutcome::Skipped),
            Some(built) => self.commit_built(built).await,
        }
    }

    /// Reads the WAL/DB and builds the next LTX file in memory — no network I/O.
    /// Splitting build from upload lets [`Syncer::checkpoint_if_needed`] hold the
    /// built frames across a checkpoint (an in-memory shadow), so a WAL reset
    /// can't lose them.
    fn build_next(&mut self) -> Result<Option<BuiltFile>, SyncError> {
        self.db.acquire_read_lock()?;
        self.build_under_lock()
    }

    /// Uploads an already-built LTX file and advances the replication position.
    async fn commit_built(&mut self, b: BuiltFile) -> Result<SyncOutcome, SyncError> {
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

        // A snapshot re-reads the whole DB, recovering any frames a checkpoint
        // moved out of the WAL — the pending resync is satisfied.
        if matches!(b.outcome, SyncOutcome::Snapshot { .. }) {
            self.pending_resync = false;
        }

        self.pos = b.new_pos;
        Ok(b.outcome)
    }

    /// Reads the WAL/DB and builds the next LTX file (no network I/O).
    ///
    /// The frequent incremental path reads only the WAL *tail* (`[offset..]`),
    /// not the whole file, so per-sync memory is bounded to the new frames. Only
    /// the rare snapshot path reads the whole WAL (and DB).
    fn build_under_lock(&self) -> Result<Option<BuiltFile>, SyncError> {
        let mut p = self.db.path().to_path_buf().into_os_string();
        p.push("-wal");
        let wal_path = std::path::PathBuf::from(p);

        let wal_len = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        let header = if wal_len >= WAL_HEADER_SIZE as u64 {
            let mut hbuf = [0u8; WAL_HEADER_SIZE];
            read_exact_at(&wal_path, 0, &mut hbuf)?;
            Some(WalHeader::parse(&hbuf)?)
        } else {
            None
        };

        match self.plan(wal_len, header.as_ref()) {
            Plan::Skip => Ok(None),
            Plan::Snapshot { offset } => {
                // Rare (first sync): read the whole WAL for the snapshot.
                let wal = fs::read(&wal_path).unwrap_or_default();
                Ok(Some(self.build_snapshot(offset, &wal)?))
            }
            Plan::Incremental {
                offset,
                salt1,
                salt2,
            } => {
                // `header` is always `Some` here (plan only returns Incremental
                // when the WAL has a header).
                let header = header.expect("incremental plan implies a WAL header");
                self.build_incremental(offset, salt1, salt2, wal_len, header, &wal_path)
            }
        }
    }

    fn plan(&self, wal_len: u64, header: Option<&WalHeader>) -> Plan {
        if self.pos.txid == 0 {
            return Plan::Snapshot {
                offset: WAL_HEADER_SIZE as u64,
            };
        }
        let Some(header) = header else {
            return if self.pos.synced_to_wal_end {
                Plan::Skip
            } else {
                Plan::Snapshot {
                    offset: WAL_HEADER_SIZE as u64,
                }
            };
        };

        let salt_match = (header.salt1, header.salt2) == (self.pos.salt1, self.pos.salt2);

        // A new WAL generation (salt change) or a shrunk WAL means a checkpoint
        // restarted the log. If that checkpoint folded un-replicated frames into
        // the DB ([`Self::pending_resync`]), re-snapshot from the DB to recover
        // them — an incremental would only see the new generation and drop them.
        // Otherwise the old generation is fully in the LTX chain, so continue
        // with a cheap incremental from the start of the new generation.
        if self.pos.wal_offset > wal_len || !salt_match {
            return if self.pending_resync {
                Plan::Snapshot {
                    offset: WAL_HEADER_SIZE as u64,
                }
            } else {
                Plan::Incremental {
                    offset: WAL_HEADER_SIZE as u64,
                    salt1: header.salt1,
                    salt2: header.salt2,
                }
            };
        }

        Plan::Incremental {
            offset: self.pos.wal_offset,
            salt1: self.pos.salt1,
            salt2: self.pos.salt2,
        }
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
                // `>=` (not `==`): an empty/truncated WAL (len < header) still
                // means we've captured everything (it's all in the DB now), so
                // the next sync skips instead of re-snapshotting forever.
                synced_to_wal_end: offset + wal_size >= wal.len() as u64,
            },
        })
    }

    fn build_incremental(
        &self,
        offset: u64,
        salt1: u32,
        salt2: u32,
        wal_len: u64,
        header: WalHeader,
        wal_path: &Path,
    ) -> Result<Option<BuiltFile>, SyncError> {
        let page_size = self.db.page_size() as usize;
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + page_size as u64;

        // Seed the running checksum from the frame ending at `offset` (the header
        // when starting at the first frame). This lets us verify the tail without
        // reading the frames before `offset`.
        let seed = if offset <= WAL_HEADER_SIZE as u64 {
            (header.checksum1, header.checksum2)
        } else {
            let mut fbuf = [0u8; WAL_FRAME_HEADER_SIZE];
            read_exact_at(wal_path, offset - frame_size, &mut fbuf)?;
            let ph = WalFrameHeader::parse(&fbuf);
            if ph.salt1 != header.salt1 || ph.salt2 != header.salt2 {
                return Err(SyncError::Wal(WalError::PrevFrameMismatch));
            }
            (ph.checksum1, ph.checksum2)
        };

        // Read only the new frames (`[offset..]`), not the whole WAL.
        let tail = read_tail(wal_path, offset)?;
        let mut reader = WalReader::from_tail(header, &tail, offset, seed)?;
        let page_map = reader.page_map();
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
                let data = reader.page_data_at(page_map.pages[&pgno]);
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
                synced_to_wal_end: final_offset >= wal_len,
            },
        }))
    }

    /// Checkpoints the WAL when it has grown enough — **without blocking the
    /// application** (litestream's philosophy).
    ///
    /// It *builds* the pending LTX from the WAL tail (fast, local), checkpoints
    /// immediately (non-blocking PASSIVE; [`Db::checkpoint`] then seq-bumps to
    /// restart the WAL and keep it bounded), and only *then* uploads. Holding the
    /// built frames across the checkpoint — an in-memory shadow — means a WAL
    /// reset can't lose them, so the race window shrinks from an upload round-trip
    /// to a local build. If the checkpoint still folds a frame into the DB that we
    /// hadn't captured (a write landing in that tiny window), we *detect* it and
    /// re-snapshot on the next sync. Correctness by noticing, never by stalling a
    /// write.
    pub async fn checkpoint_if_needed(
        &mut self,
    ) -> Result<Option<(CheckpointMode, CheckpointResult)>, SyncError> {
        let frames = self.db.wal_frame_count();
        if frames.saturating_sub(self.checkpoint_baseline_frames) < self.min_checkpoint_frames {
            return Ok(None);
        }
        let mode = CheckpointMode::Passive;

        // 1. Build (don't upload) the frames committed up to now. This is the
        //    in-memory shadow: it survives the WAL reset below.
        let built = self.build_next()?;
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + self.db.page_size() as u64;
        let captured_offset = built
            .as_ref()
            .map(|b| b.new_pos.wal_offset)
            .unwrap_or(self.pos.wal_offset);
        let synced_frames = captured_offset.saturating_sub(WAL_HEADER_SIZE as u64) / frame_size;

        // 2. Checkpoint immediately — only a local build separates it from the
        //    capture above.
        let result = self.db.checkpoint(mode)?;

        // 3. Upload the captured frames (they outlived the reset because we held
        //    them). A failure here leaves them in the DB but not the chain, so
        //    flag a resync to recover from the DB next time.
        if let Some(b) = built {
            if let Err(e) = self.commit_built(b).await {
                self.pending_resync = true;
                return Err(e);
            }
        }

        // 4. Detect a frame that slipped into the DB during the build→checkpoint
        //    window (rare): the checkpoint moved more than we captured.
        if result.checkpointed_frames as u64 > synced_frames {
            self.pending_resync = true;
        }
        // Rebaseline only when the checkpoint made progress, so a skipped (busy)
        // checkpoint is retried on the next tick.
        if !result.busy {
            self.checkpoint_baseline_frames = self.db.wal_frame_count();
        }
        Ok(Some((mode, result)))
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

    /// Compacts the previous level (`dst_level - 1`) into `dst_level`, mirroring
    /// litestream's level-to-level cascade: the source files not yet folded into
    /// the destination frontier are merged into one new destination window.
    ///
    /// Does **not** delete the source files — that's retention's job (so a
    /// retention window can keep lower levels readable). Returns `None` when the
    /// destination is already caught up. `dst_level` must be a cascade level
    /// (`1..=8`); full snapshots go through [`Syncer::snapshot`].
    pub async fn compact_level(&self, dst_level: u32) -> Result<Option<CompactionInfo>, SyncError> {
        if dst_level == 0 || dst_level >= SNAPSHOT_LEVEL {
            return Err(SyncError::InvalidCompactionLevels(format!(
                "cannot compact into level {dst_level}; use snapshot() for the snapshot level"
            )));
        }
        let src_level = dst_level - 1;

        // The destination frontier: only pull source files past it.
        let dst_max = self
            .client
            .list_ltx(dst_level)
            .await?
            .iter()
            .map(|f| f.max_txid)
            .max()
            .unwrap_or(0);

        let mut src: Vec<_> = self
            .client
            .list_ltx(src_level)
            .await?
            .into_iter()
            .filter(|f| f.min_txid > dst_max)
            .collect();
        src.sort_by_key(|f| (f.min_txid, f.max_txid));
        if src.is_empty() {
            return Ok(None);
        }

        let min_txid = src.first().unwrap().min_txid;
        let max_txid = src.last().unwrap().max_txid;

        // Fetch in TXID order and k-way merge (keeps the latest version of each
        // page; NoChecksum, ltx-apply-compatible).
        let mut buffers = Vec::with_capacity(src.len());
        for f in &src {
            buffers.push(self.client.get_ltx(src_level, f.min_txid, f.max_txid).await?.to_vec());
        }
        let refs: Vec<&[u8]> = buffers.iter().map(|v| v.as_slice()).collect();
        let inputs = refs.len();
        let merged = compact(&refs)?;

        self.client
            .put_ltx(dst_level, min_txid, max_txid, Bytes::from(merged))
            .await?;

        Ok(Some(CompactionInfo {
            min_txid,
            max_txid,
            inputs,
            pruned: 0,
        }))
    }

    /// Writes a full database snapshot (`min_txid = 1`) at [`SNAPSHOT_LEVEL`] for
    /// the latest replicated position — the restore anchor that lets retention
    /// prune every lower-level file below it.
    ///
    /// The image is reconstructed from the existing LTX chain (byte-consistent
    /// with what a restore would produce), so it never re-reads SQLite and never
    /// touches the WAL-resume state carried by the newest L0. Returns `None` if
    /// nothing has been synced yet or a snapshot already covers this position.
    pub async fn snapshot(&self) -> Result<Option<CompactionInfo>, SyncError> {
        let files = list_all_levels(&self.client).await?;
        let Some(max_txid) = files.iter().map(|(_, _, max)| *max).max() else {
            return Ok(None);
        };
        if self
            .client
            .list_ltx(SNAPSHOT_LEVEL)
            .await?
            .iter()
            .any(|f| f.min_txid == 1 && f.max_txid == max_txid)
        {
            return Ok(None);
        }

        let plan = plan_restore(&files)?;
        let inputs = plan.len();
        let image = apply_plan(&self.client, &plan).await?;

        let page_size = self.db.page_size() as usize;
        let commit = (image.len() / page_size) as u32;
        let lock = lock_pgno(page_size as u32);

        let mut buf = Vec::new();
        {
            let mut enc = Encoder::new(&mut buf);
            enc.encode_header(Header {
                flags: HEADER_FLAG_NO_CHECKSUM,
                page_size: page_size as u32,
                commit,
                min_txid: 1,
                max_txid,
                timestamp: now_ms(),
                pre_apply_checksum: Checksum::ZERO,
                wal_offset: 0,
                wal_size: 0,
                wal_salt1: 0,
                wal_salt2: 0,
                node_id: 0,
            })?;
            for pgno in 1..=commit {
                if pgno == lock {
                    continue;
                }
                let start = (pgno as usize - 1) * page_size;
                enc.encode_page(pgno, &image[start..start + page_size])?;
            }
            enc.finish()?;
        }

        self.client
            .put_ltx(SNAPSHOT_LEVEL, 1, max_txid, Bytes::from(buf))
            .await?;

        Ok(Some(CompactionInfo {
            min_txid: 1,
            max_txid,
            inputs,
            pruned: 0,
        }))
    }

    /// Deletes snapshots older than `retention` (keeping the newest), then
    /// cascade-prunes every level `0..=max_level` below the minimum retained
    /// snapshot TXID. `now` is passed in for deterministic scheduling. Returns
    /// the minimum retained snapshot TXID (0 if none were kept by age).
    pub async fn enforce_snapshot_retention(
        &self,
        now: SystemTime,
        retention: Duration,
        max_level: u32,
    ) -> Result<u64, SyncError> {
        let cutoff = now.checked_sub(retention).unwrap_or(now);
        let snaps = self.client.list_ltx(SNAPSHOT_LEVEL).await?;
        let (deleted, min_retained) = retention::snapshot_expired(&snaps, cutoff);
        for f in &deleted {
            self.client.delete_ltx(f.level, f.min_txid, f.max_txid).await?;
        }
        if min_retained > 0 {
            for level in 0..=max_level {
                self.enforce_retention_by_txid(level, min_retained).await?;
            }
        }
        Ok(min_retained)
    }

    /// Deletes files at `level` whose `max_txid` is below `txid` (keeping the
    /// newest). Returns the number deleted.
    pub async fn enforce_retention_by_txid(
        &self,
        level: u32,
        txid: u64,
    ) -> Result<usize, SyncError> {
        let files = self.client.list_ltx(level).await?;
        let deleted = retention::below_txid(&files, txid);
        for f in &deleted {
            self.client.delete_ltx(f.level, f.min_txid, f.max_txid).await?;
        }
        Ok(deleted.len())
    }

    /// Deletes L0 files that have been folded into L1 (`max_txid <= max L1 TXID`)
    /// and are older than `retention`, keeping a contiguous, recent L0 tail and
    /// always the newest. A zero `retention` disables it. Returns the number
    /// deleted.
    pub async fn enforce_l0_retention(
        &self,
        now: SystemTime,
        retention: Duration,
    ) -> Result<usize, SyncError> {
        if retention.is_zero() {
            return Ok(0);
        }
        let cutoff = now.checked_sub(retention).unwrap_or(now);
        let max_l1 = self
            .client
            .list_ltx(1)
            .await?
            .iter()
            .map(|f| f.max_txid)
            .max()
            .unwrap_or(0);
        let files = self.client.list_ltx(LEVEL0).await?;
        let deleted = retention::l0_expired(&files, max_l1, cutoff);
        for f in &deleted {
            self.client.delete_ltx(f.level, f.min_txid, f.max_txid).await?;
        }
        Ok(deleted.len())
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

/// Reads exactly `buf.len()` bytes at `offset` from a file (for the WAL header
/// or a single frame header).
fn read_exact_at(path: &Path, offset: u64, buf: &mut [u8]) -> Result<(), SyncError> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)?;
    Ok(())
}

/// Reads a file from `offset` to EOF — the WAL tail of new frames.
fn read_tail(path: &Path, offset: u64) -> Result<Vec<u8>, SyncError> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
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

/// Recovers the resume position from the replica. The TXID is the global maximum
/// across all levels; the WAL offset/salts come from the newest **L0** file — the
/// only level that carries live WAL state (compaction and snapshots zero those
/// fields). Retention always keeps the newest L0, so it's normally present; if no
/// L0 remains, WAL state is unknown and the next sync re-snapshots.
async fn derive_position(client: &ReplicaClient) -> Result<Position, SyncError> {
    let files = list_all_levels(client).await?;
    let Some(&(_, _, txid)) = files.iter().max_by_key(|(_, _, max)| *max) else {
        return Ok(Position::default());
    };

    let newest_l0 = files
        .iter()
        .filter(|(level, _, _)| *level == LEVEL0)
        .max_by_key(|(_, _, max)| *max);

    let (wal_offset, salt1, salt2) = if let Some(&(level, min, max)) = newest_l0 {
        let bytes = client.get_ltx(level, min, max).await?;
        let header = Decoder::new(&bytes[..]).decode_header()?;
        (
            (header.wal_offset + header.wal_size) as u64,
            header.wal_salt1,
            header.wal_salt2,
        )
    } else {
        (0, 0, 0)
    };

    Ok(Position {
        txid,
        wal_offset,
        salt1,
        salt2,
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

/// A restore plan reaching as close to `target` as the available files allow,
/// without overshooting it. Filtering to files ending at or before `target`
/// before planning is what lets a fine-grained L0 file be used for an early
/// point even when a later snapshot (whose range extends past `target`) exists.
fn plan_restore_to(
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
pub async fn restore(client: &ReplicaClient) -> Result<Vec<u8>, SyncError> {
    let files = list_all_levels(client).await?;
    let plan = plan_restore(&files)?;
    apply_plan(client, &plan).await
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
