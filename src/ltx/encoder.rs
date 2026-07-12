//! Streaming LTX writer.
//!
//! [`Encoder`] mirrors `github.com/superfly/ltx`'s Go encoder byte-for-byte in
//! *framing*: header, then per page `[page-header ‖ size ‖ lz4-block]`, then the
//! zero page-header terminator, the varint page index, and the trailer. The
//! running CRC64 is fed the header, each page's `[page-header ‖ size ‖
//! *uncompressed* data]`, the terminator, the index, and the trailer's
//! post-apply field — so the file checksum is compression-independent.
//!
//! Our LZ4 output need not be byte-identical to Go's (a different compressor
//! yields different compressed bytes and thus different file bytes), but any
//! valid LZ4-block decoder — including Go's — can read it. Binary compatibility
//! means mutual decodability, not identical bytes.

use std::io::Write;

use super::checksum::{CHECKSUM_FLAG, Checksum, Hasher, checksum_page};
use super::error::LtxError;
use super::format::{
    Header, PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE, PageHeader, TRAILER_SIZE, Trailer, lock_pgno,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Header,
    Page,
    Closed,
}

/// A streaming encoder for a single LTX file.
pub struct Encoder<W: Write> {
    w: W,
    state: State,
    hash: Hasher,
    /// Bytes written so far (on-disk / compressed layout).
    n: u64,
    header: Option<Header>,
    post_apply: Checksum,
    index: Vec<(u32, u64, u64)>, // (pgno, offset, size)
    prev_pgno: u32,
}

impl<W: Write> Encoder<W> {
    pub fn new(w: W) -> Self {
        Self {
            w,
            state: State::Header,
            hash: Hasher::new(),
            n: 0,
            header: None,
            post_apply: Checksum::ZERO,
            index: Vec::new(),
            prev_pgno: 0,
        }
    }

    /// Number of bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.n
    }

    /// Writes `b` to the underlying writer, feeding the file checksum.
    fn write_hashed(&mut self, b: &[u8]) -> Result<(), LtxError> {
        self.w.write_all(b)?;
        self.hash.update(b);
        self.n += b.len() as u64;
        Ok(())
    }

    /// Writes the header frame. Must be called first.
    pub fn encode_header(&mut self, header: Header) -> Result<(), LtxError> {
        debug_assert_eq!(self.state, State::Header);
        header.validate()?;
        let b = header.encode();
        self.write_hashed(&b)?;
        self.header = Some(header);
        self.state = State::Page;
        Ok(())
    }

    /// Sets the post-apply (rolling database) checksum. Call before [`finish`].
    ///
    /// [`finish`]: Encoder::finish
    pub fn set_post_apply_checksum(&mut self, c: Checksum) {
        self.post_apply = c;
    }

    /// Writes one page frame. Pages must be strictly ascending by `pgno`
    /// (snapshots additionally must start at 1 and be contiguous, skipping the
    /// lock page). `data` must be `page_size` bytes.
    pub fn encode_page(&mut self, pgno: u32, data: &[u8]) -> Result<(), LtxError> {
        let header = self.header.expect("encode_header must be called first");
        debug_assert_eq!(self.state, State::Page);

        if data.len() != header.page_size as usize {
            return Err(LtxError::WrongPageLength {
                expected: header.page_size,
                got: data.len(),
            });
        }
        if pgno == 0 {
            return Err(LtxError::ZeroPageNumber);
        }
        if pgno > header.commit {
            return Err(LtxError::PageBeyondCommit {
                pgno,
                commit: header.commit,
            });
        }
        let lock = lock_pgno(header.page_size);
        if pgno == lock {
            return Err(LtxError::LockPageEncoded(pgno));
        }

        // Ordering checks (mirror Go's encoder).
        if header.is_snapshot() {
            let ok = if self.prev_pgno == 0 {
                pgno == 1
            } else if self.prev_pgno == lock - 1 {
                pgno == self.prev_pgno + 2 // skip the lock page
            } else {
                pgno == self.prev_pgno + 1
            };
            if !ok {
                return Err(LtxError::PageOutOfOrder {
                    prev: self.prev_pgno,
                    pgno,
                });
            }
        } else if pgno <= self.prev_pgno {
            return Err(LtxError::PageOutOfOrder {
                prev: self.prev_pgno,
                pgno,
            });
        }

        let offset = self.n;

        // LZ4 *block* compression (raw block, no size prefix) — the inverse of
        // the reader's `decompress_into`.
        let compressed = lz4_flex::block::compress(data);

        let ph = PageHeader {
            pgno,
            flags: PAGE_HEADER_FLAG_SIZE,
        };
        self.write_hashed(&ph.encode())?;
        self.write_hashed(&(compressed.len() as u32).to_be_bytes())?;

        // Compressed bytes go to disk only; the checksum sees uncompressed data.
        self.w.write_all(&compressed)?;
        self.n += compressed.len() as u64;
        self.hash.update(data);

        self.index.push((pgno, offset, self.n - offset));
        self.prev_pgno = pgno;
        Ok(())
    }

    /// Writes the terminator, page index, and trailer (computing the file
    /// checksum), returning the finalized trailer.
    pub fn finish(&mut self) -> Result<Trailer, LtxError> {
        debug_assert_eq!(self.state, State::Page);

        // Zero page-header marks the end of the page block.
        self.write_hashed(&[0u8; PAGE_HEADER_SIZE])?;

        self.encode_page_index()?;

        // Trailer: post-apply is hashed; the file-checksum field is not.
        let mut trailer = Trailer {
            post_apply_checksum: self.post_apply,
            file_checksum: Checksum::ZERO,
        };
        self.hash
            .update(&trailer.post_apply_checksum.0.to_be_bytes());
        trailer.file_checksum = self.hash.checksum();

        self.w.write_all(&trailer.encode())?;
        self.n += TRAILER_SIZE as u64;
        self.w.flush()?;

        self.state = State::Closed;
        Ok(trailer)
    }

    fn encode_page_index(&mut self) -> Result<(), LtxError> {
        let mut index = std::mem::take(&mut self.index);
        index.sort_by_key(|&(pgno, _, _)| pgno);

        let index_start = self.n;
        let mut buf = Vec::with_capacity(3 * 10);
        for (pgno, offset, size) in &index {
            buf.clear();
            append_uvarint(&mut buf, *pgno as u64);
            append_uvarint(&mut buf, *offset);
            append_uvarint(&mut buf, *size);
            self.write_hashed(&buf)?;
        }
        // End marker, then the u64 big-endian index length (varints + marker).
        buf.clear();
        append_uvarint(&mut buf, 0);
        self.write_hashed(&buf)?;

        let index_len = self.n - index_start;
        self.write_hashed(&index_len.to_be_bytes())?;
        Ok(())
    }
}

/// Appends `v` as a Go-compatible unsigned varint (LEB128).
fn append_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Encodes a full database image as a single snapshot LTX file.
///
/// `min_txid` is 1 (this is a snapshot); `max_txid` is the transaction the
/// snapshot represents. The lock page (databases > 1 GiB) is skipped. Returns
/// the finalized trailer (with post-apply and file checksums).
pub fn write_snapshot<W: Write>(
    w: W,
    page_size: u32,
    db: &[u8],
    max_txid: u64,
    timestamp_ms: i64,
) -> Result<Trailer, LtxError> {
    if page_size == 0 || db.len() % page_size as usize != 0 {
        return Err(LtxError::WrongPageLength {
            expected: page_size,
            got: db.len(),
        });
    }
    let commit = (db.len() / page_size as usize) as u32;

    let header = Header {
        flags: 0,
        page_size,
        commit,
        min_txid: 1,
        max_txid,
        timestamp: timestamp_ms,
        pre_apply_checksum: Checksum::ZERO,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    };

    let lock = lock_pgno(page_size);
    let ps = page_size as usize;

    let mut enc = Encoder::new(w);
    enc.encode_header(header)?;

    let mut post = Checksum::ZERO;
    for pgno in 1..=commit {
        if pgno == lock {
            continue;
        }
        let start = (pgno as usize - 1) * ps;
        let data = &db[start..start + ps];
        enc.encode_page(pgno, data)?;
        post = Checksum(CHECKSUM_FLAG | (post.0 ^ checksum_page(pgno, data).0));
    }

    enc.set_post_apply_checksum(post);
    enc.finish()
}
