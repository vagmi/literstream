//! The LTX (Lite Transaction) file format — byte-compatible with
//! `github.com/superfly/ltx` v3.
//!
//! An LTX file is:
//!
//! ```text
//! Header (100 bytes)
//! Page block:  [PageHeader(6) [size(4) lz4-block]?]...  terminated by a zero PageHeader(6)
//! Page index:  varint(pgno, offset, size)... varint(0)  then u64 index-size
//! Trailer (16 bytes): post-apply checksum (u64) + file checksum (u64)
//! ```
//!
//! Everything is big-endian. Pages are LZ4-*block* compressed (a per-page flag
//! bit signals it). The file checksum is CRC64-ISO computed over the header,
//! the page frames *with their page data uncompressed*, the index, and the
//! trailer's post-apply field — deliberately independent of compression.
//!
//! Phase 0 implements both the **read path** ([`Decoder`], [`read_snapshot`])
//! and the **write path** ([`Encoder`], [`write_snapshot`]), each validated for
//! binary compatibility against the Go `superfly/ltx` tooling.

mod checksum;
mod compactor;
mod decoder;
mod encoder;
mod error;
mod format;

pub use checksum::{CHECKSUM_FLAG, Checksum, Hasher, checksum_page};
pub use compactor::{compact, compact_to_writer, merge_to_writer};
pub use decoder::{
    DecodedFile, Decoder, INDEX_FOOTER_SIZE, PageIndexElem, Snapshot, decode_page_frame,
    decode_page_index, page_index, read_file, read_snapshot,
};
pub use encoder::{Encoder, write_snapshot};
pub use error::LtxError;
pub use format::{
    HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE, Header, MAGIC, PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE,
    PageHeader, TRAILER_SIZE, Trailer, VERSION, lock_pgno,
};
