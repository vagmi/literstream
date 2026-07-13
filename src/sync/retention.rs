//! Time-based retention decisions — a port of litestream's retention rules in
//! `compactor.go` (`EnforceSnapshotRetention`, `EnforceL0Retention`,
//! `EnforceRetentionByTXID`).
//!
//! The *decisions* are pure functions over [`LtxFileInfo`] (which carries
//! `created_at`), so they're deterministic and unit-testable with synthetic
//! timestamps. [`super::Syncer`] wraps them with list + delete I/O.
//!
//! Universal rule across all three: **never delete the newest file at a level**
//! (the frontier a reader/resume needs), even if it's otherwise eligible.

use std::time::SystemTime;

use crate::storage::LtxFileInfo;

/// True if the file is strictly older than `cutoff`. Unknown creation time is
/// treated as "not expired" (kept) — we never delete a file whose age we can't
/// establish.
fn expired(f: &LtxFileInfo, cutoff: SystemTime) -> bool {
    match f.created_at {
        Some(t) => t < cutoff,
        None => false,
    }
}

/// Drops the newest file from `deleted` if it is the global newest of `files`
/// (the keep-newest guarantee). `files` and `deleted` are ascending by TXID.
fn keep_newest(files: &[LtxFileInfo], deleted: &mut Vec<LtxFileInfo>) {
    if let (Some(last), Some(dlast)) = (files.last(), deleted.last())
        && dlast == last
    {
        deleted.pop();
    }
}

/// Snapshot-level retention: delete snapshots older than `cutoff` (keep the
/// newest). Returns `(to_delete, min_retained_txid)`, where `min_retained_txid`
/// is the smallest `max_txid` among files kept by the age rule (0 if none) — the
/// bound lower levels cascade-prune below. Mirrors `EnforceSnapshotRetention`.
pub(crate) fn snapshot_expired(
    files: &[LtxFileInfo],
    cutoff: SystemTime,
) -> (Vec<LtxFileInfo>, u64) {
    let mut deleted = Vec::new();
    let mut min_retained: u64 = 0;
    for f in files {
        if expired(f, cutoff) {
            deleted.push(*f);
        } else if min_retained == 0 || f.max_txid < min_retained {
            min_retained = f.max_txid;
        }
    }
    keep_newest(files, &mut deleted);
    (deleted, min_retained)
}

/// L0 retention: an L0 file is deletable only once it has been folded into L1
/// (`max_txid <= max_l1_txid`) **and** is older than `cutoff`. Stops at the first
/// file newer than `cutoff` to preserve a contiguous L0 tail, and keeps the
/// newest. Returns nothing while `max_l1_txid == 0` (no L1 progress yet).
/// Mirrors `EnforceL0Retention`.
pub(crate) fn l0_expired(
    files: &[LtxFileInfo],
    max_l1_txid: u64,
    cutoff: SystemTime,
) -> Vec<LtxFileInfo> {
    if max_l1_txid == 0 {
        return Vec::new();
    }
    let mut deleted = Vec::new();
    let mut processed_all = true;
    for f in files {
        // Unknown age is treated as at-cutoff (eligible), matching litestream's
        // zero-timestamp handling; a file newer than the cutoff stops the scan.
        let too_new = matches!(f.created_at, Some(t) if t > cutoff);
        if too_new {
            processed_all = false;
            break;
        }
        if f.max_txid <= max_l1_txid {
            deleted.push(*f);
        }
    }
    if processed_all {
        keep_newest(files, &mut deleted);
    }
    deleted
}

/// TXID retention: delete files whose `max_txid < txid` (keep the newest).
/// Mirrors `EnforceRetentionByTXID` — used to cascade-prune lower levels below
/// the minimum retained snapshot TXID.
pub(crate) fn below_txid(files: &[LtxFileInfo], txid: u64) -> Vec<LtxFileInfo> {
    let mut deleted: Vec<LtxFileInfo> = files.iter().filter(|f| f.max_txid < txid).copied().collect();
    keep_newest(files, &mut deleted);
    deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn file(level: u32, min: u64, max: u64, created_secs: u64) -> LtxFileInfo {
        LtxFileInfo {
            level,
            min_txid: min,
            max_txid: max,
            size: 1,
            created_at: Some(UNIX_EPOCH + Duration::from_secs(created_secs)),
        }
    }
    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }
    fn txids(files: &[LtxFileInfo]) -> Vec<u64> {
        files.iter().map(|f| f.max_txid).collect()
    }

    #[test]
    fn snapshot_retention_keeps_newest_and_reports_min_retained() {
        let files = vec![
            file(9, 1, 10, 100),
            file(9, 1, 20, 200),
            file(9, 1, 30, 300),
        ];
        // cutoff 250: files at 100 and 200 expired, 300 retained.
        let (deleted, min_retained) = snapshot_expired(&files, at(250));
        assert_eq!(txids(&deleted), vec![10, 20]);
        assert_eq!(min_retained, 30);

        // cutoff far in the future: all expired, but the newest is still kept.
        let (deleted, _) = snapshot_expired(&files, at(10_000));
        assert_eq!(txids(&deleted), vec![10, 20]);
    }

    #[test]
    fn l0_retention_gates_on_l1_progress_and_time() {
        let files = vec![
            file(0, 1, 1, 100),
            file(0, 2, 2, 110),
            file(0, 3, 3, 120),
            file(0, 4, 4, 130),
        ];
        // No L1 yet -> keep everything.
        assert!(l0_expired(&files, 0, at(10_000)).is_empty());

        // L1 reached txid 2, cutoff far future: only txid 1,2 are folded-in; keep
        // newest is moot here (txid 4 not in the deletable set).
        assert_eq!(txids(&l0_expired(&files, 2, at(10_000))), vec![1, 2]);

        // A too-new file stops the scan (contiguous tail): cutoff 115 means txid 3
        // (t=120) is too new, so we stop before it; txid 1,2 deletable.
        assert_eq!(txids(&l0_expired(&files, 4, at(115))), vec![1, 2]);
    }

    #[test]
    fn l0_retention_keeps_newest_when_all_folded_and_old() {
        let files = vec![file(0, 1, 1, 100), file(0, 2, 2, 110), file(0, 3, 3, 120)];
        // All folded into L1 (max_l1=3) and all old: keep the newest (txid 3).
        assert_eq!(txids(&l0_expired(&files, 3, at(10_000))), vec![1, 2]);
    }

    #[test]
    fn below_txid_keeps_newest() {
        let files = vec![file(1, 1, 5, 0), file(1, 6, 10, 0), file(1, 11, 15, 0)];
        // max_txid 5 and 10 are < 12; 15 is not. Newest (15) untouched anyway.
        assert_eq!(txids(&below_txid(&files, 12)), vec![5, 10]);
        // Everything below the target: keep the newest (15 popped from deletes).
        assert_eq!(txids(&below_txid(&files, 999)), vec![5, 10]);
    }
}
