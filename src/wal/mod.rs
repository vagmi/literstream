//! SQLite Write-Ahead Log parsing.
//!
//! A WAL file is a 32-byte header followed by frames, each a 24-byte header
//! plus one `page_size` page of data:
//!
//! ```text
//! WAL header (32):  magic, version, page_size, seq, salt1, salt2, checksum1, checksum2
//! Frame (24+ps):    pgno, commit, salt1, salt2, checksum1, checksum2, <page data>
//! ```
//!
//! Fields are big-endian; the running checksum reads words in the byte order
//! the magic selects. This module is the pure-bytes counterpart to
//! [`crate::ltx`] — no SQLite dependency. It is exercised against real WAL
//! files produced by SQLite (see `scripts/gen-wal-fixtures.sh`).

mod error;
mod format;
mod reader;

pub use error::WalError;
pub use format::{
    ByteOrder, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WAL_MAGIC_BE, WAL_MAGIC_LE, WAL_VERSION,
    WalFrameHeader, WalHeader, wal_checksum,
};
pub use reader::{Frame, PageMap, WalReader};
