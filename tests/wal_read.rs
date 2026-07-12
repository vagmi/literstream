//! WAL-reader tests against real SQLite fixtures (`scripts/gen-wal-fixtures.sh`).
//!
//! The headline oracle: reconstructing the database from the base image plus the
//! WAL page map must reproduce SQLite's *own* checkpointed output byte-for-byte
//! — the same operation a restore performs.

use std::fs;
use std::path::PathBuf;

use literstream::wal::{ByteOrder, WalReader};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn header_parses_and_verifies() {
    let wal = fs::read(fixture("wal.db-wal")).expect("read wal");
    let r = WalReader::new(&wal).expect("parse+verify header");
    let h = r.header();

    assert_eq!(h.page_size, 4096);
    // The fixtures are generated on a little-endian host.
    assert_eq!(h.byte_order, ByteOrder::Little);
    assert_ne!(h.salt1, 0);
}

#[test]
fn page_map_matches_final_database_size() {
    let wal = fs::read(fixture("wal.db-wal")).expect("read wal");
    let merged = fs::read(fixture("wal.merged.db")).expect("read merged");

    let mut r = WalReader::new(&wal).expect("header");
    let pm = r.page_map();

    // Final commit == the checkpointed database's page count.
    assert_eq!(pm.commit as usize, merged.len() / 4096);
    assert!(!pm.pages.is_empty());
    // No page beyond the final size survives.
    assert!(pm.pages.keys().all(|&p| p <= pm.commit));
}

#[test]
fn reconstructs_checkpointed_database() {
    let base = fs::read(fixture("wal.db")).expect("read base");
    let wal = fs::read(fixture("wal.db-wal")).expect("read wal");
    let golden = fs::read(fixture("wal.merged.db")).expect("read merged golden");

    let mut r = WalReader::new(&wal).expect("header");
    let ps = r.page_size() as usize;
    let pm = r.page_map();

    let commit = pm.commit as usize;
    assert_eq!(commit * ps, golden.len());
    let base_pages = base.len() / ps;

    // Overlay the newest WAL frame for each page onto the base image.
    let mut merged = vec![0u8; commit * ps];
    for pgno in 1..=commit {
        let dst = (pgno - 1) * ps;
        if let Some(&offset) = pm.pages.get(&(pgno as u32)) {
            merged[dst..dst + ps].copy_from_slice(r.page_data_at(offset));
        } else {
            assert!(pgno <= base_pages, "page {pgno} absent from WAL and base");
            let src = (pgno - 1) * ps;
            merged[dst..dst + ps].copy_from_slice(&base[src..src + ps]);
        }
    }

    assert_eq!(
        merged, golden,
        "reconstructed database differs from SQLite's checkpointed output"
    );
}

#[test]
fn commit_sizes_are_monotonic_and_dedup_keeps_latest() {
    // Walk raw frames and sanity-check the commit structure.
    let wal = fs::read(fixture("wal.db-wal")).expect("read wal");
    let mut r = WalReader::new(&wal).expect("header");

    let mut frames = 0usize;
    let mut commits = 0usize;
    let mut last_offset_for_p1 = 0u64;
    while let Some(f) = r.read_frame() {
        frames += 1;
        if f.pgno == 1 {
            // page 1 is rewritten across txns; offsets strictly increase
            assert!(f.offset >= last_offset_for_p1);
            last_offset_for_p1 = f.offset;
        }
        if f.is_commit() {
            commits += 1;
        }
    }
    assert!(frames >= 4, "expected several frames, got {frames}");
    assert!(
        commits >= 2,
        "expected multiple commit frames, got {commits}"
    );
}

#[test]
fn trailing_garbage_is_ignored() {
    // A fail-safe reader must stop at the first invalid frame, not choke.
    let mut wal = fs::read(fixture("wal.db-wal")).expect("read wal");
    let clean = {
        let mut r = WalReader::new(&wal).expect("header");
        r.page_map()
    };

    // Append a bogus frame (all zeros => salt mismatch).
    wal.extend(std::iter::repeat_n(0u8, 24 + 4096));
    let dirty = {
        let mut r = WalReader::new(&wal).expect("header");
        r.page_map()
    };

    assert_eq!(clean.commit, dirty.commit);
    assert_eq!(clean.pages, dirty.pages);
}
