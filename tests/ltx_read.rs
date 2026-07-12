//! LTX read-path tests against a Litestream-format fixture produced by the Go
//! `superfly/ltx` library (see `scripts/gen-fixtures.sh`).
//!
//! Two independent oracles, both baked into the committed fixtures so this test
//! needs neither Go nor sqlite3:
//!   1. Reassembled snapshot pages == the original `.db` bytes (LZ4 + framing).
//!   2. Our recomputed checksums == the trailer bytes Go wrote (CRC64-ISO).

use std::fs;
use std::path::PathBuf;

use literstream::ltx::{Decoder, read_snapshot};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn snapshot_reconstructs_source_database() {
    let ltx = fs::read(fixture("simple.ltx")).expect("read simple.ltx");
    let db = fs::read(fixture("simple.db")).expect("read simple.db");

    let snap = read_snapshot(&ltx[..]).expect("decode snapshot");

    assert_eq!(snap.header.page_size, 4096);
    assert_eq!(snap.header.commit, 8);
    assert!(snap.header.is_snapshot());
    assert_eq!(
        snap.header.flags, 0,
        "fixture is generated with checksums on"
    );

    assert_eq!(snap.db.len(), db.len(), "page count / size mismatch");
    assert_eq!(
        snap.db, db,
        "reconstructed database image differs from the source .db"
    );
}

#[test]
fn trailer_checksums_match_go() {
    let ltx = fs::read(fixture("simple.ltx")).expect("read simple.ltx");

    // read_snapshot -> finish() verifies file + post-apply checksums internally;
    // reaching Ok already proves our CRC64-ISO matches Go's. We additionally
    // pin the exact golden values from `ltx dump`.
    let snap = read_snapshot(&ltx[..]).expect("decode + verify snapshot");

    assert_eq!(
        format!("{}", snap.trailer.post_apply_checksum),
        "9bb96fd3473e99f6"
    );
    assert_eq!(
        format!("{}", snap.trailer.file_checksum),
        "aac39ebfeb0f747d"
    );
}

#[test]
fn streaming_decoder_yields_pages_in_order() {
    let ltx = fs::read(fixture("simple.ltx")).expect("read simple.ltx");

    let mut dec = Decoder::new(&ltx[..]);
    let header = dec.decode_header().expect("header");
    let mut buf = vec![0u8; header.page_size as usize];

    let mut pgnos = Vec::new();
    while let Some(ph) = dec.decode_page(&mut buf).expect("page") {
        pgnos.push(ph.pgno);
    }
    let trailer = dec.finish().expect("finish + verify");

    assert_eq!(pgnos, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(dec.page_index().len(), 8, "index has one entry per page");
    assert_eq!(format!("{}", trailer.file_checksum), "aac39ebfeb0f747d");
}
