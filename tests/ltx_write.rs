//! LTX write-path tests: our encoder's output must decode back to the original
//! database, and its checksums must be self-consistent. The compression-
//! independent post-apply checksum must equal the value Go computes for the
//! same database (golden from `ltx dump`).

use std::fs;
use std::path::PathBuf;

use literstream::ltx::{Decoder, read_snapshot, write_snapshot};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
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

    // Our own decoder verifies the file checksum on the way through (finish()),
    // and the reconstructed image must be byte-identical to the source DB.
    let snap = read_snapshot(&out[..]).expect("decode our own output");
    assert_eq!(snap.db, db, "round-tripped database differs");
    assert_eq!(snap.trailer.file_checksum, trailer.file_checksum);
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

    let snap = read_snapshot(&out[..]).expect("decode");
    assert_eq!(snap.header.commit, 2);
    assert_eq!(snap.db, db);
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

    // And make sure the streaming decoder walks the whole thing cleanly.
    let mut dec = Decoder::new(&manual[..]);
    dec.decode_header().unwrap();
    let mut buf = vec![0u8; page_size as usize];
    let mut n = 0;
    while dec.decode_page(&mut buf).unwrap().is_some() {
        n += 1;
    }
    dec.finish().unwrap();
    assert_eq!(n, commit);
}
