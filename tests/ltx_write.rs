//! LTX write-path tests: our encoder's output must decode back to the original
//! database, and its checksums must be self-consistent. The compression-
//! independent post-apply checksum must equal the value Go computes for the
//! same database (golden from `ltx dump`).
//!
//! Our encoder writes LZ4 *frame* format (litestream-compatible), so we decode
//! via the index-based `read_file` (which handles frame and block).

use std::fs;
use std::path::PathBuf;

use literstream::ltx::{DecodedFile, read_file, write_snapshot};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Reconstructs the database image from a decoded snapshot.
fn db_image(file: &DecodedFile) -> Vec<u8> {
    let ps = file.header.page_size as usize;
    let mut img = vec![0u8; file.header.commit as usize * ps];
    for (pgno, data) in &file.pages {
        let start = (*pgno as usize - 1) * ps;
        img[start..start + ps].copy_from_slice(data);
    }
    img
}

#[test]
fn encode_then_decode_reproduces_database() {
    let db = fs::read(fixture("simple.db")).expect("read simple.db");

    let mut out = Vec::new();
    let trailer = write_snapshot(&mut out, 4096, &db, 1, 1_700_000_000_000).expect("encode");

    // Post-apply is over uncompressed pages, so it matches Go exactly.
    assert_eq!(
        format!("{}", trailer.post_apply_checksum),
        "9bb96fd3473e99f6"
    );

    let file = read_file(&out).expect("decode our own output");
    assert_eq!(db_image(&file), db, "round-tripped database differs");
    assert_eq!(file.trailer.file_checksum, trailer.file_checksum);
}

#[test]
fn synthetic_pages_round_trip() {
    // Two 512-byte pages with distinct, compressible content.
    let page_size = 512usize;
    let mut db = vec![0u8; page_size * 2];
    for (i, b) in db.iter_mut().enumerate() {
        *b = ((i / page_size) as u8).wrapping_mul(17); // page 1 -> 0x00, page 2 -> 0x11
    }

    let mut out = Vec::new();
    write_snapshot(&mut out, page_size as u32, &db, 1, 0).expect("encode");

    let file = read_file(&out).expect("decode");
    assert_eq!(file.header.commit, 2);
    assert_eq!(db_image(&file), db);
}

#[test]
fn streaming_encode_matches_helper() {
    // The streaming Encoder and write_snapshot must produce identical bytes.
    use literstream::ltx::{Encoder, Header};

    let db = fs::read(fixture("simple.db")).expect("read simple.db");
    let page_size = 4096u32;
    let commit = (db.len() / page_size as usize) as u32;

    let mut manual = Vec::new();
    {
        let mut enc = Encoder::new(&mut manual);
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: 1,
            max_txid: 1,
            timestamp: 1_700_000_000_000,
            pre_apply_checksum: Default::default(),
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        })
        .unwrap();
        let mut post = literstream::ltx::Checksum::default();
        for pgno in 1..=commit {
            let start = (pgno as usize - 1) * page_size as usize;
            let data = &db[start..start + page_size as usize];
            enc.encode_page(pgno, data).unwrap();
            post = literstream::ltx::Checksum(
                literstream::ltx::CHECKSUM_FLAG
                    | (post.0 ^ literstream::ltx::checksum_page(pgno, data).0),
            );
        }
        enc.set_post_apply_checksum(post);
        enc.finish().unwrap();
    }

    let mut helper = Vec::new();
    write_snapshot(&mut helper, page_size, &db, 1, 1_700_000_000_000).unwrap();

    assert_eq!(manual, helper, "streaming and helper encoders diverged");

    // The output decodes cleanly with the full page count.
    let file = read_file(&manual).unwrap();
    assert_eq!(file.pages.len() as u32, commit);
}
