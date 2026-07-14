//! A replication driver: the scheduler that turns the [`Syncer`] primitives
//! (`sync`, `checkpoint_if_needed`, `compact_level`, `snapshot`, and the
//! `enforce_*_retention` methods) into litestream's continuous behaviour.
//!
//! litestream runs this as a set of per-level goroutines on their own timers.
//! Because literstream is a library, the driver is instead a single
//! [`Driver::tick`] the caller invokes on whatever cadence it likes — real time
//! in production, a synthetic clock in tests. `now` is passed in explicitly, so
//! scheduling and retention are deterministic.
//!
//! Each `tick(now)`:
//! 1. syncs new WAL frames to L0,
//! 2. checkpoints the WAL if it has grown enough,
//! 3. for every configured level, runs a level-to-level compaction once per
//!    interval boundary (aligned to the wall clock, like litestream),
//! 4. writes a full snapshot on the snapshot interval and enforces snapshot
//!    retention (which cascade-prunes lower levels), and
//! 5. enforces time-based L0 retention.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::{CheckpointMode, CheckpointResult};

use super::level::{self, CompactionLevels};
use super::{CompactionInfo, SyncError, SyncOutcome, Syncer};

/// Default snapshot cadence, mirroring litestream's `DefaultSnapshotInterval`.
pub const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Default snapshot retention, mirroring litestream's `DefaultSnapshotRetention`.
pub const DEFAULT_SNAPSHOT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
/// Default L0 retention window, mirroring litestream's `DefaultL0Retention`.
pub const DEFAULT_L0_RETENTION: Duration = Duration::from_secs(5 * 60);
/// Default cadence for the L0 retention check, mirroring litestream's
/// `DefaultL0RetentionCheckInterval` — retention runs periodically, not every tick.
pub const DEFAULT_L0_RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// Default time-based checkpoint interval, mirroring litestream's
/// `DefaultCheckpointInterval` — so a low-write database still folds its WAL
/// into the DB file periodically even if it never crosses the frame threshold.
pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);

/// What a single [`Driver::tick`] did.
#[derive(Clone, Debug)]
pub struct TickReport {
    /// The sync outcome for the new WAL frames.
    pub synced: SyncOutcome,
    /// The checkpoint that ran, if the WAL crossed a threshold.
    pub checkpoint: Option<(CheckpointMode, CheckpointResult)>,
    /// Level-to-level compactions that fired this tick, as `(dst_level, info)`.
    pub compactions: Vec<(u32, CompactionInfo)>,
    /// The snapshot written this tick, if the snapshot interval elapsed.
    pub snapshot: Option<CompactionInfo>,
    /// Number of L0 files pruned by time-based retention this tick.
    pub l0_pruned: usize,
    /// True if the never-block fallback fired this tick: a checkpoint raced a
    /// write and a catch-up snapshot was taken. The event most worth alarming on.
    pub resnapshot_fired: bool,
    /// Bytes of LTX still staged locally awaiting upload (grows during an
    /// object-store outage; normally 0). Surface it to detect a stuck replica.
    pub staged_backlog_bytes: u64,
}

/// Drives continuous replication + tiered compaction + retention for a [`Syncer`].
pub struct Driver {
    syncer: Syncer,
    levels: CompactionLevels,
    snapshot_interval: Duration,
    snapshot_retention: Duration,
    l0_retention: Duration,
    l0_retention_interval: Duration,
    /// Time-based checkpoint cadence (0 disables it).
    checkpoint_interval: Duration,
    /// The last interval boundary each level was compacted at (index = level).
    last_compaction_boundary: Vec<SystemTime>,
    /// The last interval boundary a snapshot was taken at.
    last_snapshot_boundary: SystemTime,
    /// The last interval boundary L0 retention was enforced at.
    last_l0_retention_boundary: SystemTime,
    /// When the last checkpoint ran (for the time-based cadence).
    last_checkpoint_at: SystemTime,
    /// Whether new frames have been synced since the last checkpoint — gates the
    /// time-based checkpoint so `_litestream_seq` bookkeeping alone doesn't churn
    /// LTX files (litestream issue #896).
    synced_since_checkpoint: bool,
    /// Snapshot of `Syncer::resnapshot_count` at the last tick, to detect a fire.
    prev_resnapshots: u64,
}

impl Driver {
    /// Builds a driver over `syncer` with the given levels and litestream's
    /// default snapshot/retention windows. Use the `with_*` setters to override.
    pub fn new(syncer: Syncer, levels: CompactionLevels) -> Driver {
        let slots = levels.max_level() as usize + 1;
        Driver {
            syncer,
            levels,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            snapshot_retention: DEFAULT_SNAPSHOT_RETENTION,
            l0_retention: DEFAULT_L0_RETENTION,
            l0_retention_interval: DEFAULT_L0_RETENTION_CHECK_INTERVAL,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            last_compaction_boundary: vec![UNIX_EPOCH; slots],
            last_snapshot_boundary: UNIX_EPOCH,
            last_l0_retention_boundary: UNIX_EPOCH,
            last_checkpoint_at: UNIX_EPOCH,
            synced_since_checkpoint: false,
            prev_resnapshots: 0,
        }
    }

    /// Sets the time-based checkpoint interval (0 disables it). A low-write
    /// database that never crosses the frame threshold still folds its WAL on
    /// this cadence, so restores stay cheap.
    pub fn with_checkpoint_interval(mut self, d: Duration) -> Driver {
        self.checkpoint_interval = d;
        self
    }

    /// Sets how often a full snapshot is written (0 disables snapshots).
    pub fn with_snapshot_interval(mut self, d: Duration) -> Driver {
        self.snapshot_interval = d;
        self
    }

    /// Sets how long snapshots are retained.
    pub fn with_snapshot_retention(mut self, d: Duration) -> Driver {
        self.snapshot_retention = d;
        self
    }

    /// Sets how long L0 files are retained after being folded into L1 (0 disables
    /// time-based L0 retention).
    pub fn with_l0_retention(mut self, d: Duration) -> Driver {
        self.l0_retention = d;
        self
    }

    /// Sets how often the L0 retention check runs (it need not run every tick).
    pub fn with_l0_retention_interval(mut self, d: Duration) -> Driver {
        self.l0_retention_interval = d;
        self
    }

    /// The underlying syncer.
    pub fn syncer(&self) -> &Syncer {
        &self.syncer
    }

    /// Mutable access to the underlying syncer (e.g. to tune checkpoint fields).
    pub fn syncer_mut(&mut self) -> &mut Syncer {
        &mut self.syncer
    }

    /// The replica client (for restores).
    pub fn client(&self) -> &crate::storage::ReplicaClient {
        self.syncer.client()
    }

    /// Drains any pending WAL frames into the replica; see [`Syncer::flush`].
    pub async fn flush(&mut self) -> Result<u32, SyncError> {
        self.syncer.flush().await
    }

    /// Runs one scheduler step at wall-clock instant `now`.
    pub async fn tick(&mut self, now: SystemTime) -> Result<TickReport, SyncError> {
        // 1. Replicate new frames (one sync captures all committed frames to the
        //    current WAL end), then checkpoint if the WAL has grown enough. If a
        //    non-blocking checkpoint races the writer, the gap is detected and a
        //    catch-up snapshot is staged (see `checkpoint_if_needed`).
        let synced = self.syncer.sync().await?;
        if synced != SyncOutcome::Skipped {
            self.synced_since_checkpoint = true;
        }
        let mut checkpoint = self.syncer.checkpoint_if_needed().await?;

        // Time-based checkpoint: if the frame threshold wasn't crossed but enough
        // time has passed and we've synced real data since the last checkpoint,
        // fold the WAL anyway so a low-write database's restore stays cheap.
        if checkpoint.is_none()
            && !self.checkpoint_interval.is_zero()
            && self.synced_since_checkpoint
            && self.syncer.db().wal_frame_count() > 0
            && now
                .duration_since(self.last_checkpoint_at)
                .map(|d| d >= self.checkpoint_interval)
                .unwrap_or(false)
        {
            checkpoint = Some(self.syncer.checkpoint_now(CheckpointMode::Passive).await?);
        }
        if checkpoint.is_some() {
            self.last_checkpoint_at = now;
            self.synced_since_checkpoint = false;
        }

        // 2. Level-to-level compaction, once per interval boundary per level.
        let mut compactions = Vec::new();
        for lvl in self.levels.as_slice() {
            if lvl.level == 0 {
                continue;
            }
            let boundary = lvl.prev_compaction_at(now);
            if boundary > self.last_compaction_boundary[lvl.level as usize] {
                self.last_compaction_boundary[lvl.level as usize] = boundary;
                if let Some(info) = self.syncer.compact_level(lvl.level).await? {
                    compactions.push((lvl.level, info));
                }
            }
        }

        // 3. Snapshot on its own interval, then enforce snapshot retention
        //    (which cascade-prunes lower levels below the retained boundary).
        let mut snapshot = None;
        if !self.snapshot_interval.is_zero() {
            let boundary = level::interval_boundary(now, self.snapshot_interval);
            if boundary > self.last_snapshot_boundary {
                self.last_snapshot_boundary = boundary;
                snapshot = self.syncer.snapshot().await?;
                self.syncer
                    .enforce_snapshot_retention(now, self.snapshot_retention, self.levels.max_level())
                    .await?;
            }
        }

        // 4. Time-based L0 retention, on its own (coarser) cadence.
        let mut l0_pruned = 0;
        if !self.l0_retention.is_zero() && !self.l0_retention_interval.is_zero() {
            let boundary = level::interval_boundary(now, self.l0_retention_interval);
            if boundary > self.last_l0_retention_boundary {
                self.last_l0_retention_boundary = boundary;
                l0_pruned = self.syncer.enforce_l0_retention(now, self.l0_retention).await?;
            }
        }

        let resnapshot_count = self.syncer.resnapshot_count();
        let resnapshot_fired = resnapshot_count > self.prev_resnapshots;
        self.prev_resnapshots = resnapshot_count;

        Ok(TickReport {
            synced,
            checkpoint,
            compactions,
            snapshot,
            l0_pruned,
            resnapshot_fired,
            staged_backlog_bytes: self.syncer.staged_backlog_bytes(),
        })
    }
}
