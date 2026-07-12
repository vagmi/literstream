//! On-disk LTX structures: [`Header`], [`PageHeader`], [`Trailer`], and the
//! constants that frame the format. All integers are big-endian.

use super::checksum::Checksum;
use super::error::LtxError;

/// First 4 bytes of every LTX file.
pub const MAGIC: [u8; 4] = *b"LTX1";
/// Format version implied by the magic. superfly/ltx is at version 3.
pub const VERSION: u32 = 3;

/// Fixed header size, in bytes.
pub const HEADER_SIZE: usize = 100;
/// Page header size, in bytes: `pgno: u32` + `flags: u16`.
pub const PAGE_HEADER_SIZE: usize = 6;
/// Trailer size, in bytes: `post_apply: u64` + `file_checksum: u64`.
pub const TRAILER_SIZE: usize = 16;

/// Header flag: checksum tracking is disabled (pre/post-apply DB checksums are
/// zero). Litestream sets this on every file it writes; the per-page LZ4 and
/// the trailer file checksum are still always present.
pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;

/// Page-header flag: a 4-byte size field follows and the page data is an LZ4
/// *block* (not frame). The current encoder sets this on every page.
pub const PAGE_HEADER_FLAG_SIZE: u16 = 1 << 0;

/// SQLite's PENDING_BYTE — the lock byte lives at the 1 GiB boundary.
pub const PENDING_BYTE: u64 = 0x4000_0000;

/// The 1-based page number containing SQLite's lock byte, for `page_size`.
/// This page is never stored in LTX files (only relevant for DBs > 1 GiB).
pub fn lock_pgno(page_size: u32) -> u32 {
    (PENDING_BYTE / page_size as u64) as u32 + 1
}

fn be_u16(b: &[u8], off: usize) -> u16 {
    u16::from_be_bytes(b[off..off + 2].try_into().unwrap())
}
fn be_u32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().unwrap())
}
fn be_u64(b: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(b[off..off + 8].try_into().unwrap())
}

/// Returns true if `sz` is a power of two in `512..=65536`.
pub fn is_valid_page_size(sz: u32) -> bool {
    sz >= 512 && sz <= 65536 && sz.is_power_of_two()
}

/// The header frame of an LTX file (100 bytes on disk).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub flags: u32,
    /// SQLite page size, in bytes.
    pub page_size: u32,
    /// Database size, in pages, after this transaction is applied.
    pub commit: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Rolling DB checksum before applying this file (zero on snapshots).
    pub pre_apply_checksum: Checksum,
    /// Byte offset within the source WAL (zero for snapshots).
    pub wal_offset: i64,
    /// Byte length of the source WAL segment (zero for snapshots).
    pub wal_size: i64,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub node_id: u64,
}

impl Header {
    /// Decodes a header from at least [`HEADER_SIZE`] bytes.
    pub fn decode(b: &[u8]) -> Result<Header, LtxError> {
        if b.len() < HEADER_SIZE {
            return Err(LtxError::ShortBuffer {
                need: HEADER_SIZE,
                got: b.len(),
            });
        }
        let magic: [u8; 4] = b[0..4].try_into().unwrap();
        if magic != MAGIC {
            return Err(LtxError::InvalidMagic(magic));
        }
        let header = Header {
            flags: be_u32(b, 4),
            page_size: be_u32(b, 8),
            commit: be_u32(b, 12),
            min_txid: be_u64(b, 16),
            max_txid: be_u64(b, 24),
            timestamp: be_u64(b, 32) as i64,
            pre_apply_checksum: Checksum(be_u64(b, 40)),
            wal_offset: be_u64(b, 48) as i64,
            wal_size: be_u64(b, 56) as i64,
            wal_salt1: be_u32(b, 64),
            wal_salt2: be_u32(b, 68),
            node_id: be_u64(b, 72),
        };
        if !is_valid_page_size(header.page_size) {
            return Err(LtxError::InvalidPageSize(header.page_size));
        }
        Ok(header)
    }

    /// A snapshot contains every page (its minimum TXID is 1).
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }

    /// True if checksum tracking is disabled ([`HEADER_FLAG_NO_CHECKSUM`]).
    pub fn no_checksum(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM != 0
    }

    /// The lock page number for this file's page size.
    pub fn lock_pgno(&self) -> u32 {
        lock_pgno(self.page_size)
    }
}

/// The header preceding each page frame (6 bytes on disk).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageHeader {
    pub pgno: u32,
    pub flags: u16,
}

impl PageHeader {
    /// Decodes a page header from at least [`PAGE_HEADER_SIZE`] bytes.
    pub fn decode(b: &[u8]) -> Result<PageHeader, LtxError> {
        if b.len() < PAGE_HEADER_SIZE {
            return Err(LtxError::ShortBuffer {
                need: PAGE_HEADER_SIZE,
                got: b.len(),
            });
        }
        Ok(PageHeader {
            pgno: be_u32(b, 0),
            flags: be_u16(b, 4),
        })
    }

    /// An all-zero page header marks the end of the page block.
    pub fn is_zero(&self) -> bool {
        self.pgno == 0 && self.flags == 0
    }

    /// True if the page data is an LZ4 block preceded by a 4-byte size.
    pub fn is_block_compressed(&self) -> bool {
        self.flags & PAGE_HEADER_FLAG_SIZE != 0
    }
}

/// The ending frame of an LTX file (16 bytes on disk).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Trailer {
    /// Rolling DB checksum after this file is applied.
    pub post_apply_checksum: Checksum,
    /// CRC64-ISO checksum of the whole file.
    pub file_checksum: Checksum,
}

impl Trailer {
    /// Decodes a trailer from at least [`TRAILER_SIZE`] bytes.
    pub fn decode(b: &[u8]) -> Result<Trailer, LtxError> {
        if b.len() < TRAILER_SIZE {
            return Err(LtxError::ShortBuffer {
                need: TRAILER_SIZE,
                got: b.len(),
            });
        }
        Ok(Trailer {
            post_apply_checksum: Checksum(be_u64(b, 0)),
            file_checksum: Checksum(be_u64(b, 8)),
        })
    }
}
