//! Multi-level, time-based compaction configuration — a port of litestream's
//! `compaction_level.go`.
//!
//! Level 0 holds the raw LTX files written straight from the WAL. Higher levels
//! merge the previous level's files into coarser time-granularity windows, each
//! on its own interval. Level 9 ([`SNAPSHOT_LEVEL`]) holds full database
//! snapshots and is fed by a separate path, not by the level-to-level cascade.
//!
//! litestream's canonical defaults (see [`CompactionLevels::default_levels`]):
//! L1 every 30s, L2 every 5m, L3 every 1h, snapshots every 24h.
//!
//! Interval alignment mirrors Go's `time.Time.Truncate`: the "previous
//! compaction" instant is `now` floored to a multiple of the level's interval.
//! We align to the Unix epoch so, e.g., a 30s level fires at `:00`/`:30`. The
//! current time is passed in explicitly so scheduling is deterministic in tests.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::SyncError;

/// The level at which full database snapshots are stored (litestream's
/// `SnapshotLevel`). It is not part of the level-to-level cascade.
pub const SNAPSHOT_LEVEL: u32 = 9;

/// One tier of the compaction cascade.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionLevel {
    /// The numeric level; must equal its index in [`CompactionLevels`].
    pub level: u32,
    /// How often this level is compacted from the previous level. Zero only for
    /// level 0 (which is written directly from the WAL, never compacted into).
    pub interval: Duration,
}

impl CompactionLevel {
    /// The most recent interval-aligned instant at or before `now` (`now`
    /// floored to a multiple of `interval` since the Unix epoch). Returns `now`
    /// unchanged for a zero interval.
    pub fn prev_compaction_at(&self, now: SystemTime) -> SystemTime {
        truncate(now, self.interval)
    }

    /// The next interval-aligned instant strictly after [`Self::prev_compaction_at`].
    pub fn next_compaction_at(&self, now: SystemTime) -> SystemTime {
        self.prev_compaction_at(now) + self.interval
    }
}

/// A validated, level-ordered list of non-snapshot compaction levels
/// (index 0 = L0, and so on). The snapshot level is implicit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionLevels(Vec<CompactionLevel>);

impl CompactionLevels {
    /// Builds and validates a level list. Levels must be dense and in order
    /// (`levels[i].level == i`), only L0 may have a zero interval, and the
    /// highest level must not reach the snapshot level.
    pub fn new(levels: Vec<CompactionLevel>) -> Result<CompactionLevels, SyncError> {
        let levels = CompactionLevels(levels);
        levels.validate()?;
        Ok(levels)
    }

    /// litestream's canonical defaults: L1@30s, L2@5m, L3@1h.
    pub fn default_levels() -> CompactionLevels {
        CompactionLevels(vec![
            CompactionLevel { level: 0, interval: Duration::ZERO },
            CompactionLevel { level: 1, interval: Duration::from_secs(30) },
            CompactionLevel { level: 2, interval: Duration::from_secs(5 * 60) },
            CompactionLevel { level: 3, interval: Duration::from_secs(60 * 60) },
        ])
    }

    /// The configured non-snapshot levels, lowest-first.
    pub fn as_slice(&self) -> &[CompactionLevel] {
        &self.0
    }

    /// The level with this number, if configured (snapshot level excluded).
    pub fn get(&self, level: u32) -> Option<CompactionLevel> {
        self.0.get(level as usize).copied().filter(|l| l.level == level)
    }

    /// The highest configured non-snapshot level.
    pub fn max_level(&self) -> u32 {
        (self.0.len() as u32).saturating_sub(1)
    }

    /// The source level a compaction into `level` reads from. The snapshot
    /// level's source is the highest non-snapshot level. Returns `None` for L0.
    pub fn prev_level(&self, level: u32) -> Option<u32> {
        if level == SNAPSHOT_LEVEL {
            return Some(self.max_level());
        }
        if level == 0 {
            return None;
        }
        Some(level - 1)
    }

    /// The destination level a compaction of `level` writes to. The highest
    /// non-snapshot level's next is the snapshot level. Returns `None` for the
    /// snapshot level.
    pub fn next_level(&self, level: u32) -> Option<u32> {
        if level == SNAPSHOT_LEVEL {
            return None;
        }
        if level == self.max_level() {
            return Some(SNAPSHOT_LEVEL);
        }
        Some(level + 1)
    }

    /// True if `level` is a valid level number (a configured level or the
    /// snapshot level).
    pub fn is_valid_level(&self, level: u32) -> bool {
        level == SNAPSHOT_LEVEL || (level as usize) < self.0.len()
    }

    /// [`CompactionLevel::prev_compaction_at`] for a configured level.
    pub fn prev_compaction_at(&self, level: u32, now: SystemTime) -> Option<SystemTime> {
        self.get(level).map(|l| l.prev_compaction_at(now))
    }

    /// [`CompactionLevel::next_compaction_at`] for a configured level.
    pub fn next_compaction_at(&self, level: u32, now: SystemTime) -> Option<SystemTime> {
        self.get(level).map(|l| l.next_compaction_at(now))
    }

    fn validate(&self) -> Result<(), SyncError> {
        if self.0.is_empty() {
            return Err(invalid("at least one compaction level is required"));
        }
        for (i, lvl) in self.0.iter().enumerate() {
            if lvl.level as usize != i {
                return Err(invalid(format!(
                    "compaction level out of order: {}, expected {i}",
                    lvl.level
                )));
            }
            if lvl.level > SNAPSHOT_LEVEL - 1 {
                return Err(invalid(format!(
                    "compaction level cannot exceed {}",
                    SNAPSHOT_LEVEL - 1
                )));
            }
            if lvl.level == 0 && !lvl.interval.is_zero() {
                return Err(invalid("cannot set interval on compaction level zero"));
            }
            if lvl.level != 0 && lvl.interval.is_zero() {
                return Err(invalid(format!("interval required for level {}", lvl.level)));
            }
        }
        Ok(())
    }
}

impl Default for CompactionLevels {
    fn default() -> CompactionLevels {
        CompactionLevels::default_levels()
    }
}

fn invalid(msg: impl Into<String>) -> SyncError {
    SyncError::InvalidCompactionLevels(msg.into())
}

/// The most recent instant aligned to `interval` (floored to a multiple since
/// the Unix epoch); returns `now` unchanged for a zero interval. Used to
/// schedule the snapshot level, which isn't part of [`CompactionLevels`].
pub fn interval_boundary(now: SystemTime, interval: Duration) -> SystemTime {
    truncate(now, interval)
}

/// Floors `now` to a multiple of `interval` since the Unix epoch.
fn truncate(now: SystemTime, interval: Duration) -> SystemTime {
    if interval.is_zero() {
        return now;
    }
    let since = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let iv = interval.as_nanos();
    let truncated = (since.as_nanos() / iv) * iv;
    // Nanos since the epoch fit in u64 until well past year 2500.
    UNIX_EPOCH + Duration::from_nanos(truncated as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn defaults_are_valid_and_ordered() {
        let levels = CompactionLevels::default_levels();
        levels.validate().unwrap();
        assert_eq!(levels.max_level(), 3);
        assert_eq!(
            levels.get(1),
            Some(CompactionLevel { level: 1, interval: Duration::from_secs(30) })
        );
        assert_eq!(levels.get(9), None); // snapshot level is not in the list
    }

    #[test]
    fn prev_next_level_walk_the_cascade() {
        let levels = CompactionLevels::default_levels();
        assert_eq!(levels.prev_level(0), None);
        assert_eq!(levels.prev_level(1), Some(0));
        assert_eq!(levels.prev_level(SNAPSHOT_LEVEL), Some(3)); // snapshot pulls from max
        assert_eq!(levels.next_level(0), Some(1));
        assert_eq!(levels.next_level(3), Some(SNAPSHOT_LEVEL)); // max feeds the snapshot
        assert_eq!(levels.next_level(SNAPSHOT_LEVEL), None);
    }

    #[test]
    fn interval_alignment_floors_to_epoch_multiple() {
        let l1 = CompactionLevel { level: 1, interval: Duration::from_secs(30) };
        // 95s -> prev aligned at 90s, next at 120s.
        assert_eq!(l1.prev_compaction_at(at(95)), at(90));
        assert_eq!(l1.next_compaction_at(at(95)), at(120));
        // Exact multiple stays put for prev, advances for next.
        assert_eq!(l1.prev_compaction_at(at(120)), at(120));
        assert_eq!(l1.next_compaction_at(at(120)), at(150));
    }

    #[test]
    fn validation_rejects_bad_configs() {
        // Out-of-order level numbers.
        assert!(CompactionLevels::new(vec![
            CompactionLevel { level: 0, interval: Duration::ZERO },
            CompactionLevel { level: 2, interval: Duration::from_secs(1) },
        ])
        .is_err());
        // Interval on level zero.
        assert!(CompactionLevels::new(vec![CompactionLevel {
            level: 0,
            interval: Duration::from_secs(1),
        }])
        .is_err());
        // Missing interval on a non-zero level.
        assert!(CompactionLevels::new(vec![
            CompactionLevel { level: 0, interval: Duration::ZERO },
            CompactionLevel { level: 1, interval: Duration::ZERO },
        ])
        .is_err());
        // Empty.
        assert!(CompactionLevels::new(vec![]).is_err());
    }
}
