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
use std::io::{BufWriter, Read, Seek, SeekFrom};
// Positional file I/O (pread/pwrite): `read_exact_at` / `write_all_at`. Unix
// (Linux + macOS); a Windows port would use `std::os::windows::fs::FileExt`.
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::db::{CheckpointMode, CheckpointResult, Db};
use crate::lock::ProcessLock;
use crate::ltx::{
    Checksum, Decoder, Encoder, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE, Header, INDEX_FOOTER_SIZE,
    compact_to_writer, decode_page_frame, decode_page_index, lock_pgno, merge_to_writer, read_file,
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

/// Default WAL-frame growth before a checkpoint (~4 MB @ 4 KB), mirroring
/// litestream's `DefaultMinCheckpointPageN`.
pub const DEFAULT_MIN_CHECKPOINT_FRAMES: u64 = 1000;

/// Default blocking-TRUNCATE checkpoint threshold (~500 MB @ 4 KB), mirroring
/// litestream's `DefaultTruncatePageN`. The emergency brake that bounds the
/// on-disk WAL under sustained writes when PASSIVE keeps returning busy.
pub const DEFAULT_TRUNCATE_FRAMES: u64 = 121359;

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

/// An LTX file encoded to a durable local **staging file**, ready to upload,
/// along with the state changes to commit once the upload succeeds. The file is
/// fsync'd before it is returned, so it survives a crash (or an aggressive
/// checkpoint) and is re-uploaded on the next [`Syncer::open`].
struct StagedFile {
    outcome: SyncOutcome,
    level: u32,
    min_txid: u64,
    max_txid: u64,
    /// Path to the fsync'd `.ltx` in the staging directory.
    path: PathBuf,
    new_pos: Position,
}

/// Replicates a [`Db`]'s WAL to an object-store replica.
pub struct Syncer {
    db: Db,
    client: ReplicaClient,
    pos: Position,
    /// Held for the syncer's lifetime to enforce a single host-local writer.
    _lock: ProcessLock,
    /// Non-blocking PASSIVE checkpoint threshold: checkpoint once the current
    /// WAL generation holds this many logically-synced frames.
    pub min_checkpoint_frames: u64,
    /// Blocking TRUNCATE checkpoint threshold — a large emergency brake that
    /// bounds the on-disk WAL under sustained writes. Safe (never loses data)
    /// because staging makes durability independent of the checkpoint.
    pub truncate_frames: u64,
    /// Count of over-fold catch-up snapshots taken (the never-block fallback
    /// path). Exposed via [`Syncer::resnapshot_count`] so callers can alarm when
    /// the fallback fires.
    resnapshots: u64,
    /// Local staging directory (`<db>-litestream/ltx`). Every LTX is encoded and
    /// fsync'd here before the checkpoint that could fold its frames into the DB,
    /// then uploaded and removed. Leftovers are re-uploaded on [`Syncer::open`].
    staging: PathBuf,
}

impl Syncer {
    /// Opens a syncer over `client`, resuming from any existing chain there.
    ///
    /// Takes a host-local single-writer lock on the database; fails with
    /// [`SyncError::Lock`] if another literstream process holds it.
    pub async fn open(db: Db, client: ReplicaClient) -> Result<Syncer, SyncError> {
        let lock = ProcessLock::acquire(db.path())?;
        let staging = staging_root(db.path());
        // Re-upload any staged files a previous run crashed before shipping (and
        // drop half-written `.tmp` debris). This recovers the exact frames — no
        // whole-DB re-snapshot — and must run before we derive the position, so
        // the position reflects the now-complete chain.
        recover_staging(&staging, &client).await?;
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
            truncate_frames: DEFAULT_TRUNCATE_FRAMES,
            resnapshots: 0,
            staging,
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
            Some(staged) => self.upload_staged(staged).await,
        }
    }

    /// Reads the WAL/DB and encodes the next LTX to a fsync'd staging file — no
    /// network I/O. Splitting staging from upload lets
    /// [`Syncer::checkpoint_if_needed`] make the frames durable *before* the
    /// checkpoint that could fold them into the DB, so neither a WAL reset nor a
    /// crash can lose them.
    fn build_next(&mut self) -> Result<Option<StagedFile>, SyncError> {
        self.db.acquire_read_lock()?;
        self.build_under_lock()
    }

    /// Uploads a staged LTX file, removes it, and advances the replication
    /// position. A failed upload leaves the file staged for a later retry.
    async fn upload_staged(&mut self, s: StagedFile) -> Result<SyncOutcome, SyncError> {
        upload_ltx_file(&self.client, s.level, s.min_txid, s.max_txid, &s.path).await?;
        self.pos = s.new_pos;
        Ok(s.outcome)
    }

    /// The staged `.ltx` path for `(level, min, max)`.
    fn staged_ltx_path(&self, level: u32, min: u64, max: u64) -> PathBuf {
        self.staging
            .join(format!("{level:04x}"))
            .join(format!("{min:016x}-{max:016x}.ltx"))
    }

    /// Encodes an LTX into the staging directory durably: writes to a `.tmp`
    /// via a buffered writer, `sync_all`s it, atomically renames to `.ltx`, and
    /// fsyncs the directory. The renamed `.ltx` is complete-and-durable by
    /// construction; a leftover `.tmp` is crash debris cleaned on the next open.
    /// Returns the final `.ltx` path.
    fn stage<F>(&self, level: u32, min: u64, max: u64, encode: F) -> Result<PathBuf, SyncError>
    where
        F: FnOnce(&mut Encoder<BufWriter<File>>) -> Result<(), SyncError>,
    {
        let final_path = self.staged_ltx_path(level, min, max);
        let dir = final_path.parent().expect("staged path has a parent");
        fs::create_dir_all(dir)?;
        let tmp = with_extension_suffix(&final_path, ".tmp");

        let mut enc = Encoder::new(BufWriter::new(File::create(&tmp)?));
        encode(&mut enc)?;
        enc.finish()?;
        // `finish` flushed the BufWriter, so `into_inner` won't error.
        let file = enc
            .into_inner()
            .into_inner()
            .map_err(|e| SyncError::Io(e.into_error()))?;
        file.sync_all()?;
        drop(file);

        fs::rename(&tmp, &final_path)?;
        // Persist the rename (the new directory entry), not just the file data.
        let _ = File::open(dir).and_then(|d| d.sync_all());
        Ok(final_path)
    }

    /// Reads the WAL/DB and builds the next LTX file (no network I/O).
    ///
    /// The frequent incremental path reads only the WAL *tail* (`[offset..]`),
    /// not the whole file, so per-sync memory is bounded to the new frames. Only
    /// the rare snapshot path reads the whole WAL (and DB).
    fn build_under_lock(&self) -> Result<Option<StagedFile>, SyncError> {
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
        // restarted the log. The old generation is fully in the LTX chain (our
        // pinned read-mark blocks external resets, and our own checkpoints stage
        // + upload before resetting — with `checkpoint_if_needed` staging a
        // catch-up snapshot if a write raced the checkpoint), so a cheap
        // incremental from the start of the new generation is always correct.
        if self.pos.wal_offset > wal_len || !salt_match {
            return Plan::Incremental {
                offset: WAL_HEADER_SIZE as u64,
                salt1: header.salt1,
                salt2: header.salt2,
            };
        }

        Plan::Incremental {
            offset: self.pos.wal_offset,
            salt1: self.pos.salt1,
            salt2: self.pos.salt2,
        }
    }

    fn build_snapshot(&self, offset: u64, wal: &[u8]) -> Result<StagedFile, SyncError> {
        let page_size = self.db.page_size() as usize;
        // pread the DB one page at a time instead of slurping the whole file —
        // DB-side memory is O(page_size), not O(database).
        let db_file = File::open(self.db.path())?;
        let db_len = db_file.metadata()?.len() as usize;

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
            (db_len / page_size) as u32
        };
        let wal_size = page_map.end_offset.saturating_sub(offset);
        let txid = self.pos.txid + 1;
        let lock = lock_pgno(page_size as u32);

        // Encode straight into the fsync'd staging file (O(page_size)).
        let mut page = vec![0u8; page_size];
        let path = self.stage(LEVEL0, txid, txid, |enc| {
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
                match page_map.pages.get(&pgno) {
                    Some(&off) => enc.encode_page(pgno, wal_page(wal, off, page_size))?,
                    None => {
                        db_file.read_exact_at(&mut page, (pgno as u64 - 1) * page_size as u64)?;
                        enc.encode_page(pgno, &page)?;
                    }
                }
            }
            Ok(())
        })?;

        Ok(StagedFile {
            outcome: SyncOutcome::Snapshot {
                txid,
                pages: commit,
            },
            level: LEVEL0,
            min_txid: txid,
            max_txid: txid,
            path,
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

    /// Reads the current WAL and stages a full snapshot at the live position,
    /// regardless of what [`Syncer::plan`] would choose. Used to recover frames a
    /// checkpoint folded into the DB that we hadn't captured (the over-fold race).
    fn build_snapshot_now(&self) -> Result<StagedFile, SyncError> {
        let mut p = self.db.path().to_path_buf().into_os_string();
        p.push("-wal");
        let wal = fs::read(PathBuf::from(p)).unwrap_or_default();
        self.build_snapshot(WAL_HEADER_SIZE as u64, &wal)
    }

    fn build_incremental(
        &self,
        offset: u64,
        salt1: u32,
        salt2: u32,
        wal_len: u64,
        header: WalHeader,
        wal_path: &Path,
    ) -> Result<Option<StagedFile>, SyncError> {
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

        // Encode straight into the fsync'd staging file (O(new frames)).
        let path = self.stage(LEVEL0, txid, txid, |enc| {
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
            Ok(())
        })?;

        let final_offset = offset + wal_size;
        Ok(Some(StagedFile {
            outcome: SyncOutcome::Incremental {
                txid,
                pages: pgnos.len(),
            },
            level: LEVEL0,
            min_txid: txid,
            max_txid: txid,
            path,
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
    /// It *stages* the pending LTX from the WAL tail (fast, local) to a fsync'd
    /// file, checkpoints immediately (non-blocking PASSIVE; [`Db::checkpoint`]
    /// then seq-bumps to restart the WAL and keep it bounded), and only *then*
    /// uploads. Because the staged file is durable *before* the checkpoint, a WAL
    /// reset — or a crash — can't lose the frames: they are re-uploaded from
    /// staging on the next [`Syncer::open`]. If the checkpoint still folds a
    /// frame into the DB that we hadn't captured (a write landing in that tiny
    /// window), we *notice* and stage a catch-up snapshot from the DB right here,
    /// so it too is durable. Correctness by noticing, never by stalling a write.
    pub async fn checkpoint_if_needed(
        &mut self,
    ) -> Result<Option<(CheckpointMode, CheckpointResult)>, SyncError> {
        // Gate on the *logical* WAL frames synced this generation, not the `-wal`
        // file high-water: a PASSIVE checkpoint leaves stale frames in the file,
        // so a size-based gate re-triggers every tick (litestream issue #997).
        let live = self.live_wal_frames();
        let mode = if live >= self.truncate_frames {
            CheckpointMode::Truncate // emergency brake (blocking)
        } else if live >= self.min_checkpoint_frames {
            CheckpointMode::Passive
        } else {
            return Ok(None);
        };
        Ok(Some(self.run_checkpoint(mode).await?))
    }

    /// Runs one stage → checkpoint → upload cycle in `mode`, unconditionally.
    /// Used by [`Syncer::checkpoint_if_needed`] and the driver's time-based
    /// checkpoint. Never blocks the writer (beyond a TRUNCATE's reader wait).
    pub async fn checkpoint_now(
        &mut self,
        mode: CheckpointMode,
    ) -> Result<(CheckpointMode, CheckpointResult), SyncError> {
        self.run_checkpoint(mode).await
    }

    async fn run_checkpoint(
        &mut self,
        mode: CheckpointMode,
    ) -> Result<(CheckpointMode, CheckpointResult), SyncError> {
        // 1. Stage the frames committed up to now to a fsync'd .ltx — durable
        //    before the checkpoint can fold them into the DB.
        let staged = self.build_next()?;
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + self.db.page_size() as u64;
        let captured_offset = staged
            .as_ref()
            .map(|s| s.new_pos.wal_offset)
            .unwrap_or(self.pos.wal_offset);
        let synced_frames = captured_offset.saturating_sub(WAL_HEADER_SIZE as u64) / frame_size;

        // 2. Checkpoint immediately — safe, the frames are already on disk.
        let result = self.db.checkpoint(mode)?;

        // 3. Upload the staged file. A failure leaves it in the staging dir; the
        //    frames are not lost (recovered on the next open()), so surface the
        //    error and retry later rather than advancing the position.
        if let Some(s) = staged {
            self.upload_staged(s).await?;
        }

        // 4. Over-fold: a write landed in the build→checkpoint window and the
        //    checkpoint folded more frames than we captured. Those frames now
        //    live only in the DB file, so stage + upload a full snapshot NOW to
        //    recover them (durable; the staged file survives a crash here too).
        if result.checkpointed_frames as u64 > synced_frames {
            let snap = self.build_snapshot_now()?;
            self.upload_staged(snap).await?;
            self.resnapshots += 1;
        }

        Ok((mode, result))
    }

    /// Logically-synced WAL frames in the current generation (issue #997): the
    /// replicated extent, immune to the stale high-water a PASSIVE leaves behind.
    fn live_wal_frames(&self) -> u64 {
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + self.db.page_size() as u64;
        self.pos
            .wal_offset
            .saturating_sub(WAL_HEADER_SIZE as u64)
            / frame_size
    }

    /// Number of over-fold catch-up snapshots taken (the never-block fallback).
    pub fn resnapshot_count(&self) -> u64 {
        self.resnapshots
    }

    /// Total bytes of LTX files currently awaiting upload in the staging dir —
    /// the un-shipped backlog (grows during an object-store outage).
    pub fn staged_backlog_bytes(&self) -> u64 {
        staged_backlog_bytes(&self.staging)
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

        // Fetch inputs in TXID order: the existing base, then the L0 run. Keep
        // the ref-counted `Bytes` — no copy into an owned `Vec`.
        let mut buffers: Vec<Bytes> = Vec::new();
        if let Some(b) = base {
            buffers.push(
                self.client
                    .get_ltx(SNAPSHOT_LEVEL, b.min_txid, b.max_txid)
                    .await?,
            );
        }
        for f in to_merge {
            buffers.push(self.client.get_ltx(LEVEL0, f.min_txid, f.max_txid).await?);
        }

        let refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_ref()).collect();
        let inputs = refs.len();

        // Publish the new base, then delete what it supersedes. The merge streams
        // page-by-page into a temp file and uploads it in parts, so peak memory is
        // O(inputs + page_size), not O(merged size).
        self.compact_and_upload(SNAPSHOT_LEVEL, 1, new_max, &refs)
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
        let mut buffers: Vec<Bytes> = Vec::with_capacity(src.len());
        for f in &src {
            buffers.push(self.client.get_ltx(src_level, f.min_txid, f.max_txid).await?);
        }
        let refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_ref()).collect();
        let inputs = refs.len();

        self.compact_and_upload(dst_level, min_txid, max_txid, &refs)
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

        // Fetch the plan files and stream-merge them into the snapshot, newest
        // page winning, one page at a time. The final size comes from the newest
        // file's header (a VACUUM may have shrunk the database).
        let mut buffers: Vec<Bytes> = Vec::with_capacity(plan.len());
        for &(level, min, max) in &plan {
            buffers.push(self.client.get_ltx(level, min, max).await?);
        }
        let refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_ref()).collect();
        let newest = Header::decode(refs.last().expect("restore plan is non-empty"))?;

        let header = Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: newest.page_size,
            commit: newest.commit,
            min_txid: 1,
            max_txid,
            timestamp: now_ms(),
            pre_apply_checksum: Checksum::ZERO,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        };

        let tmp = self.compaction_temp(SNAPSHOT_LEVEL, 1, max_txid)?;
        merge_to_writer(&refs, header, BufWriter::new(File::create(&tmp)?))?;
        let uploaded = self
            .client
            .put_ltx_multipart(SNAPSHOT_LEVEL, 1, max_txid, &tmp)
            .await;
        fs::remove_file(&tmp).ok();
        uploaded?;

        Ok(Some(CompactionInfo {
            min_txid: 1,
            max_txid,
            inputs,
            pruned: 0,
        }))
    }

    /// Streams a compaction of `refs` into a temp file and uploads it to
    /// `(level, min, max)` with a multipart (streaming, no-CAS) put, then removes
    /// the temp. Peak memory is O(inputs + page_size), not O(merged size).
    async fn compact_and_upload(
        &self,
        level: u32,
        min: u64,
        max: u64,
        refs: &[&[u8]],
    ) -> Result<(), SyncError> {
        let tmp = self.compaction_temp(level, min, max)?;
        compact_to_writer(refs, BufWriter::new(File::create(&tmp)?))?;
        let uploaded = self.client.put_ltx_multipart(level, min, max, &tmp).await;
        fs::remove_file(&tmp).ok();
        uploaded?;
        Ok(())
    }

    /// A scratch path for a compaction or snapshot merge output, under this
    /// database's own `<db>-litestream/compact/` directory (not the system temp,
    /// so concurrent syncers on different databases never collide, and not under
    /// `ltx/`, so recovery never mistakes it for a staged file). Compaction is
    /// idempotent and re-runnable, so this file is not fsync'd or recovered.
    fn compaction_temp(&self, level: u32, min: u64, max: u64) -> Result<PathBuf, SyncError> {
        let dir = self
            .staging
            .parent()
            .unwrap_or(&self.staging)
            .join("compact");
        fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{level:04x}-{min:016x}-{max:016x}.ltx")))
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

/// The staging root for `db_path` — `<db>-litestream/ltx`, mirroring
/// litestream's `db.LTXDir()`. LTX files are encoded + fsync'd under
/// `<root>/<level:04x>/` before upload; the layout is local-only (the remote key
/// layout is unchanged), so it's just a familiar, debuggable shape.
fn staging_root(db_path: &Path) -> PathBuf {
    let mut p = db_path.to_path_buf().into_os_string();
    p.push("-litestream");
    PathBuf::from(p).join("ltx")
}

/// Sum of `.ltx` file sizes under a staging root — the un-uploaded backlog.
fn staged_backlog_bytes(staging: &Path) -> u64 {
    let Ok(levels) = fs::read_dir(staging) else {
        return 0;
    };
    let mut total = 0;
    for level in levels.flatten() {
        let Ok(files) = fs::read_dir(level.path()) else {
            continue;
        };
        for f in files.flatten() {
            if f.file_name().to_string_lossy().ends_with(".ltx")
                && let Ok(m) = f.metadata()
            {
                total += m.len();
            }
        }
    }
    total
}

/// Returns `path` with `suffix` appended to its file name (e.g. `x.ltx` → `x.ltx.tmp`).
fn with_extension_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.to_path_buf().into_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

/// A staged file at or above this size is streamed to the replica via a
/// multipart upload (O(chunk) memory); smaller ones are read into memory and
/// CAS-uploaded. The split keeps the if-not-exists guard on the small, frequent
/// L0 incrementals (where concurrent writers actually collide) and streams only
/// the large, effectively-single-writer files (snapshots, compaction output).
const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// Uploads a staged LTX file to the replica, then removes the local staged copy.
///
/// Small files go through the CAS guard (an idempotent retry of our own write is
/// fine; a genuinely different file at this TXID is [`SyncError::Equivocation`]);
/// large files stream via multipart (no CAS — see [`MULTIPART_THRESHOLD`]).
async fn upload_ltx_file(
    client: &ReplicaClient,
    level: u32,
    min: u64,
    max: u64,
    path: &Path,
) -> Result<(), SyncError> {
    let size = fs::metadata(path)?.len();
    if size >= MULTIPART_THRESHOLD {
        client.put_ltx_multipart(level, min, max, path).await?;
    } else {
        let bytes = fs::read(path)?;
        match client
            .put_ltx_cas(level, min, max, Bytes::from(bytes))
            .await?
        {
            PutOutcome::Created | PutOutcome::AlreadyIdentical => {}
            PutOutcome::Conflict => return Err(SyncError::Equivocation { txid: max }),
        }
    }
    fs::remove_file(path)?;
    Ok(())
}

/// Re-uploads any staged LTX left by a previous run (a crash between staging and
/// upload) and deletes half-written `.tmp` debris. Uploads ascending by TXID.
async fn recover_staging(staging: &Path, client: &ReplicaClient) -> Result<(), SyncError> {
    let level_dirs = match fs::read_dir(staging) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    let mut staged: Vec<(u32, u64, u64, PathBuf)> = Vec::new();
    for level_dir in level_dirs {
        let level_dir = level_dir?;
        let Some(level) = level_dir
            .file_name()
            .to_str()
            .and_then(|n| u32::from_str_radix(n, 16).ok())
        else {
            continue;
        };
        for entry in fs::read_dir(level_dir.path())? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".ltx.tmp") {
                let _ = fs::remove_file(&path); // incomplete write
            } else if let Some((min, max)) = crate::storage::parse_ltx_filename(&name) {
                staged.push((level, min, max, path));
            }
        }
    }

    staged.sort_by_key(|&(level, min, max, _)| (min, max, level));
    for (level, min, max, path) in staged {
        upload_ltx_file(client, level, min, max, &path).await?;
    }
    Ok(())
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
    let plan = plan_restore(&files)?;
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

    // Pre-size the output; `set_len` zero-fills, which also covers any gaps (the
    // lock page in >1 GiB databases is never encoded and stays zero).
    let out = File::create(path)?;
    out.set_len((commit * page_size) as u64)?;

    let lock = lock_pgno(nh.page_size) as usize;
    let mut written = vec![false; commit + 1]; // 1-indexed; [0] unused.
    if (1..=commit).contains(&lock) {
        written[lock] = true; // Leave the lock page zero-filled.
    }

    // Newest-first: the first file (going backwards) to carry a page holds its
    // latest version. Decode each file page-by-page from its index — never
    // materializing a whole file's pages — and pwrite the ones we haven't
    // written yet. Handles both the LZ4 block and frame (litestream) formats.
    for &(level, min, max) in plan.iter().rev() {
        let bytes = client.get_ltx(level, min, max).await?;
        for_each_indexed_page(&bytes, page_size, |pgno, data| {
            let p = pgno as usize;
            if (1..=commit).contains(&p) && !written[p] {
                written[p] = true;
                out.write_all_at(data, ((p - 1) * page_size) as u64)?;
            }
            Ok(())
        })?;
    }
    out.sync_all()?;
    Ok(nmax)
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
