//! The WAL→LTX sync engine (Phase 3): monitor a live database's WAL and turn
//! newly-committed frames into an immutable LTX chain on local disk.
//!
//! This ties together the pure-bytes pieces: [`crate::db`] controls SQLite and
//! checkpoints, [`crate::wal`] reads committed frames, and [`crate::ltx`]
//! encodes them. It mirrors litestream's model:
//!
//! - Each sync produces **one L0 LTX file** at `TXID = prev + 1`
//!   (`MinTXID == MaxTXID`), stored at `<root>/ltx/0/<min>-<max>.ltx`.
//! - The **first** sync writes a snapshot (`MinTXID = 1`, all pages); later syncs
//!   write only the pages changed in the new WAL segment.
//! - The header records `WALOffset`/`WALSize`/salts so the next sync knows where
//!   to resume; salt changes / truncation fall back to a snapshot when the
//!   incremental chain can't continue.
//!
//! Unlike litestream (which sets `NoChecksum`), we maintain the **rolling
//! database checksum** — the XOR of every page's checksum — updating it
//! incrementally as pages change. This is O(changed pages) per sync and lets the
//! standard `ltx apply` tool replay our chain (it verifies pre/post-apply
//! checksums against the reconstructed database).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{CheckpointMode, CheckpointResult, Db};
use crate::ltx::{CHECKSUM_FLAG, Checksum, Decoder, Encoder, Header, checksum_page, lock_pgno};
use crate::wal::{PageMap, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalHeader, WalReader};

mod error;
pub use error::SyncError;

/// Default WAL-frame threshold for a PASSIVE checkpoint (~4 MB @ 4 KB).
pub const DEFAULT_MIN_CHECKPOINT_FRAMES: u64 = 1000;
/// Default WAL-frame threshold for an emergency TRUNCATE checkpoint.
pub const DEFAULT_TRUNCATE_FRAMES: u64 = 10_000;

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
    /// Next WAL byte offset to read from (end of the last synced segment).
    wal_offset: u64,
    salt1: u32,
    salt2: u32,
    /// Whether the last sync reached the exact end of the WAL file.
    synced_to_wal_end: bool,
}

/// How the next sync should proceed.
enum Plan {
    Skip,
    Snapshot { offset: u64 },
    Incremental { offset: u64, salt1: u32, salt2: u32 },
}

/// Replicates a [`Db`]'s WAL to a local LTX directory.
pub struct Syncer {
    db: Db,
    ltx_dir: PathBuf,
    pos: Position,
    /// Per-page checksum of the current database state (index = pgno - 1).
    page_checksums: Vec<Checksum>,
    /// Rolling database checksum (XOR of `page_checksums`, with the flag bit).
    post_apply: Checksum,
    /// PASSIVE checkpoint when the WAL reaches this many frames.
    pub min_checkpoint_frames: u64,
    /// TRUNCATE checkpoint at this many frames (0 disables).
    pub truncate_frames: u64,
}

impl Syncer {
    /// Opens a syncer writing under `root/ltx/0`, resuming from any existing
    /// files there.
    pub fn open(db: Db, root: impl AsRef<Path>) -> Result<Syncer, SyncError> {
        let root = root.as_ref();
        let ltx_dir = root.join("ltx").join("0");
        fs::create_dir_all(&ltx_dir)?;
        let pos = derive_position(&ltx_dir)?;

        // On resume, rebuild the checksum state from the existing chain.
        let (page_checksums, post_apply) = if pos.txid > 0 {
            let image = restore(root)?;
            checksums_from_image(&image, db.page_size() as usize)
        } else {
            (Vec::new(), Checksum::ZERO)
        };

        Ok(Syncer {
            db,
            ltx_dir,
            pos,
            page_checksums,
            post_apply,
            min_checkpoint_frames: DEFAULT_MIN_CHECKPOINT_FRAMES,
            truncate_frames: DEFAULT_TRUNCATE_FRAMES,
        })
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn position_txid(&self) -> u64 {
        self.pos.txid
    }

    /// Performs one sync cycle under a pinned read lock (so the DB + WAL are a
    /// consistent snapshot while we read them).
    pub fn sync(&mut self) -> Result<SyncOutcome, SyncError> {
        self.db.acquire_read_lock()?;
        let result = self.sync_inner();
        let _ = self.db.release_read_lock();
        result
    }

    fn sync_inner(&mut self) -> Result<SyncOutcome, SyncError> {
        let mut wal_path = self.db.path().to_path_buf().into_os_string();
        wal_path.push("-wal");
        let wal = fs::read(&wal_path).unwrap_or_default();

        match self.plan(&wal)? {
            Plan::Skip => Ok(SyncOutcome::Skipped),
            Plan::Snapshot { offset } => self.write_snapshot(offset, &wal),
            Plan::Incremental {
                offset,
                salt1,
                salt2,
            } => self.write_incremental(offset, salt1, salt2, &wal),
        }
    }

    /// Decides snapshot vs. incremental vs. skip (a simplification of
    /// litestream's `verify`: we own checkpointing, so `synced_to_wal_end` is a
    /// sufficient proxy for "the chain can continue").
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

        // WAL truncated below our offset, or restarted with new salts.
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

    fn write_snapshot(&mut self, offset: u64, wal: &[u8]) -> Result<SyncOutcome, SyncError> {
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

        // A full snapshot has no prior state (MinTXID=1); a mid-chain full
        // rewrite chains off the previous position.
        let pre_apply = if txid == 1 {
            Checksum::ZERO
        } else {
            self.post_apply
        };

        let mut page_checksums = vec![Checksum::ZERO; commit as usize];
        let mut post = Checksum::ZERO;
        let mut buf = Vec::new();
        {
            let mut enc = Encoder::new(&mut buf);
            enc.encode_header(Header {
                flags: 0,
                page_size: page_size as u32,
                commit,
                min_txid: txid,
                max_txid: txid,
                timestamp: now_ms(),
                pre_apply_checksum: pre_apply,
                wal_offset: offset as i64,
                wal_size: wal_size as i64,
                wal_salt1: salt1,
                wal_salt2: salt2,
                node_id: 0,
            })?;

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

                let cp = checksum_page(pgno, data);
                page_checksums[pgno as usize - 1] = cp;
                post = Checksum(CHECKSUM_FLAG | (post.0 ^ cp.0));
            }

            enc.set_post_apply_checksum(post);
            enc.finish()?;
        }

        self.write_ltx_file(txid, txid, &buf)?;
        self.page_checksums = page_checksums;
        self.post_apply = post;
        self.pos = Position {
            txid,
            wal_offset: offset + wal_size,
            salt1,
            salt2,
            synced_to_wal_end: offset + wal_size == wal.len() as u64,
        };
        Ok(SyncOutcome::Snapshot {
            txid,
            pages: commit,
        })
    }

    fn write_incremental(
        &mut self,
        offset: u64,
        salt1: u32,
        salt2: u32,
        wal: &[u8],
    ) -> Result<SyncOutcome, SyncError> {
        let page_size = self.db.page_size() as usize;
        let page_map = WalReader::new_at_offset(wal, offset)?.page_map();
        if page_map.pages.is_empty() {
            return Ok(SyncOutcome::Skipped);
        }

        let wal_size = page_map.end_offset - offset;
        let commit = page_map.commit;
        let txid = self.pos.txid + 1;
        let pre_apply = self.post_apply;

        let mut pgnos: Vec<u32> = page_map.pages.keys().copied().collect();
        pgnos.sort_unstable();

        // Update the rolling checksum incrementally: remove each changed page's
        // old contribution, add its new one.
        let mut checksums = std::mem::take(&mut self.page_checksums);
        if checksums.len() < commit as usize {
            checksums.resize(commit as usize, Checksum::ZERO);
        }
        let mut post = pre_apply;

        let mut buf = Vec::new();
        {
            let mut enc = Encoder::new(&mut buf);
            enc.encode_header(Header {
                flags: 0,
                page_size: page_size as u32,
                commit,
                min_txid: txid,
                max_txid: txid,
                timestamp: now_ms(),
                pre_apply_checksum: pre_apply,
                wal_offset: offset as i64,
                wal_size: wal_size as i64,
                wal_salt1: salt1,
                wal_salt2: salt2,
                node_id: 0,
            })?;

            for &pgno in &pgnos {
                let data = wal_page(wal, page_map.pages[&pgno], page_size);
                enc.encode_page(pgno, data)?;

                let cp_new = checksum_page(pgno, data);
                let idx = pgno as usize - 1;
                let cp_old = checksums[idx];
                post = Checksum(CHECKSUM_FLAG | (post.0 ^ cp_old.0 ^ cp_new.0));
                checksums[idx] = cp_new;
            }

            enc.set_post_apply_checksum(post);
            enc.finish()?;
        }

        // Handle a database shrink (VACUUM): drop removed pages' contributions.
        if (commit as usize) < checksums.len() {
            for cp in &checksums[commit as usize..] {
                post = Checksum(CHECKSUM_FLAG | (post.0 ^ cp.0));
            }
            checksums.truncate(commit as usize);
        }

        self.write_ltx_file(txid, txid, &buf)?;
        self.page_checksums = checksums;
        self.post_apply = post;
        let final_offset = offset + wal_size;
        self.pos = Position {
            txid,
            wal_offset: final_offset,
            salt1,
            salt2,
            synced_to_wal_end: final_offset == wal.len() as u64,
        };
        Ok(SyncOutcome::Incremental {
            txid,
            pages: pgnos.len(),
        })
    }

    fn write_ltx_file(&self, min_txid: u64, max_txid: u64, bytes: &[u8]) -> Result<(), SyncError> {
        let path = self
            .ltx_dir
            .join(format!("{min_txid:016x}-{max_txid:016x}.ltx"));
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Applies the 3-tier checkpoint strategy based on WAL frame count.
    ///
    /// Sync before calling this so the frames being checkpointed are already
    /// captured as LTX.
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
        let result = self.db.checkpoint(mode)?;
        Ok(Some((mode, result)))
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

/// Computes per-page checksums and the rolling database checksum of an image.
fn checksums_from_image(image: &[u8], page_size: usize) -> (Vec<Checksum>, Checksum) {
    let n = image.len() / page_size;
    let mut v = Vec::with_capacity(n);
    let mut post = Checksum::ZERO;
    for pgno in 1..=n {
        let data = &image[(pgno - 1) * page_size..pgno * page_size];
        let cp = checksum_page(pgno as u32, data);
        v.push(cp);
        post = Checksum(CHECKSUM_FLAG | (post.0 ^ cp.0));
    }
    (v, post)
}

/// Parses an `<min>-<max>.ltx` filename into its TXID range.
fn parse_ltx_filename(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".ltx")?;
    let (min, max) = stem.split_once('-')?;
    Some((
        u64::from_str_radix(min, 16).ok()?,
        u64::from_str_radix(max, 16).ok()?,
    ))
}

/// Reads the highest-TXID LTX file to recover the resume position.
fn derive_position(ltx_dir: &Path) -> Result<Position, SyncError> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(ltx_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some((_min, max)) = parse_ltx_filename(&name)
            && best.as_ref().map(|(m, _)| max > *m).unwrap_or(true)
        {
            best = Some((max, entry.path()));
        }
    }

    let Some((max, path)) = best else {
        return Ok(Position::default());
    };

    let bytes = fs::read(&path)?;
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

/// Reconstructs the database image from a replica's LTX chain by applying the
/// snapshot and every incremental in TXID order.
pub fn restore(root: impl AsRef<Path>) -> Result<Vec<u8>, SyncError> {
    let ltx_dir = root.as_ref().join("ltx").join("0");

    let mut files: Vec<(u64, u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(&ltx_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some((min, max)) = parse_ltx_filename(&name) {
            files.push((min, max, entry.path()));
        }
    }
    files.sort_by_key(|(min, _, _)| *min);
    if files.first().map(|(min, _, _)| *min) != Some(1) {
        return Err(SyncError::NoSnapshot);
    }

    let mut image: Vec<u8> = Vec::new();
    for (_min, _max, path) in files {
        let bytes = fs::read(&path)?;
        let mut dec = Decoder::new(&bytes[..]);
        let header = dec.decode_header()?;
        let page_size = header.page_size as usize;

        image.resize(header.commit as usize * page_size, 0);

        let mut page = vec![0u8; page_size];
        while let Some(ph) = dec.decode_page(&mut page)? {
            let start = (ph.pgno as usize - 1) * page_size;
            image[start..start + page_size].copy_from_slice(&page);
        }
        dec.finish()?;
    }

    Ok(image)
}
