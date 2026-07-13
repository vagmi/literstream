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
//! syncs only the changed pages. We maintain the real rolling database checksum
//! (see the crate docs) so the standard `ltx apply` can replay our chain.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::db::{CheckpointMode, CheckpointResult, Db};
use crate::ltx::{CHECKSUM_FLAG, Checksum, Decoder, Encoder, Header, checksum_page, lock_pgno};
use crate::storage::ReplicaClient;
use crate::wal::{PageMap, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalHeader, WalReader};

mod error;
pub use error::SyncError;

/// Default WAL-frame threshold for a PASSIVE checkpoint (~4 MB @ 4 KB).
pub const DEFAULT_MIN_CHECKPOINT_FRAMES: u64 = 1000;
/// Default WAL-frame threshold for an emergency TRUNCATE checkpoint.
pub const DEFAULT_TRUNCATE_FRAMES: u64 = 10_000;

/// L0 is the raw, uncompacted level.
const LEVEL0: u32 = 0;

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
    new_page_checksums: Vec<Checksum>,
    new_post_apply: Checksum,
}

/// Replicates a [`Db`]'s WAL to an object-store replica.
pub struct Syncer {
    db: Db,
    client: ReplicaClient,
    pos: Position,
    page_checksums: Vec<Checksum>,
    post_apply: Checksum,
    /// PASSIVE checkpoint when the WAL reaches this many frames.
    pub min_checkpoint_frames: u64,
    /// TRUNCATE checkpoint at this many frames (0 disables).
    pub truncate_frames: u64,
}

impl Syncer {
    /// Opens a syncer over `client`, resuming from any existing chain there.
    pub async fn open(db: Db, client: ReplicaClient) -> Result<Syncer, SyncError> {
        let pos = derive_position(&client).await?;
        let (page_checksums, post_apply) = if pos.txid > 0 {
            let image = restore(&client).await?;
            checksums_from_image(&image, db.page_size() as usize)
        } else {
            (Vec::new(), Checksum::ZERO)
        };
        Ok(Syncer {
            db,
            client,
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

        self.client
            .put_ltx(LEVEL0, b.min_txid, b.max_txid, Bytes::from(b.bytes))
            .await?;

        self.pos = b.new_pos;
        self.page_checksums = b.new_page_checksums;
        self.post_apply = b.new_post_apply;
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
            new_page_checksums: page_checksums,
            new_post_apply: post,
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
        let pre_apply = self.post_apply;

        let mut pgnos: Vec<u32> = page_map.pages.keys().copied().collect();
        pgnos.sort_unstable();

        let mut checksums = self.page_checksums.clone();
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
                post = Checksum(CHECKSUM_FLAG | (post.0 ^ checksums[idx].0 ^ cp_new.0));
                checksums[idx] = cp_new;
            }
            enc.set_post_apply_checksum(post);
            enc.finish()?;
        }

        // VACUUM shrink: drop removed pages' contributions.
        if (commit as usize) < checksums.len() {
            for cp in &checksums[commit as usize..] {
                post = Checksum(CHECKSUM_FLAG | (post.0 ^ cp.0));
            }
            checksums.truncate(commit as usize);
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
            new_page_checksums: checksums,
            new_post_apply: post,
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

/// Reads the highest-TXID LTX header to recover the resume position.
async fn derive_position(client: &ReplicaClient) -> Result<Position, SyncError> {
    let files = client.list_ltx(LEVEL0).await?;
    let Some(last) = files.last() else {
        return Ok(Position::default());
    };
    let bytes = client.get_ltx(LEVEL0, last.min_txid, last.max_txid).await?;
    let mut dec = Decoder::new(&bytes[..]);
    let header = dec.decode_header()?;
    Ok(Position {
        txid: last.max_txid,
        wal_offset: (header.wal_offset + header.wal_size) as u64,
        salt1: header.wal_salt1,
        salt2: header.wal_salt2,
        synced_to_wal_end: false,
    })
}

/// Reconstructs the database image from a replica's LTX chain by applying the
/// snapshot and every incremental in TXID order.
pub async fn restore(client: &ReplicaClient) -> Result<Vec<u8>, SyncError> {
    let files = client.list_ltx(LEVEL0).await?;
    if files.first().map(|f| f.min_txid) != Some(1) {
        return Err(SyncError::NoSnapshot);
    }

    let mut image: Vec<u8> = Vec::new();
    for f in files {
        let bytes = client.get_ltx(LEVEL0, f.min_txid, f.max_txid).await?;
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
