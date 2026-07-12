//! Streaming LTX reader.
//!
//! [`Decoder`] mirrors the framing of `github.com/superfly/ltx`'s Go decoder:
//! it feeds the running CRC64 exactly as the encoder did — header bytes, then
//! per page the `[page-header ‖ size ‖ *uncompressed* data]`, then the zero
//! page-header terminator, then the index and the trailer's post-apply field —
//! so [`Decoder::finish`] can confirm the file checksum bit-for-bit.

use std::io::Read;

use super::checksum::{CHECKSUM_FLAG, Checksum, Hasher, checksum_page};
use super::error::LtxError;
use super::format::{
    HEADER_SIZE, Header, PAGE_HEADER_SIZE, PageHeader, TRAILER_SIZE, Trailer, lock_pgno,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Header,
    Page,
    Close,
    Closed,
}

/// An entry in the page index: where a page's frame lives in the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageIndexElem {
    pub pgno: u32,
    pub offset: u64,
    pub size: u64,
}

/// A streaming decoder over an LTX file.
pub struct Decoder<R: Read> {
    r: R,
    state: State,
    hash: Hasher,
    header: Option<Header>,
    /// Rolling post-apply checksum, accumulated while decoding a snapshot.
    rolling: Checksum,
    page_index: Vec<PageIndexElem>,
    trailer: Option<Trailer>,
}

impl<R: Read> Decoder<R> {
    pub fn new(r: R) -> Self {
        Self {
            r,
            state: State::Header,
            hash: Hasher::new(),
            header: None,
            rolling: Checksum::ZERO,
            page_index: Vec::new(),
            trailer: None,
        }
    }

    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    pub fn trailer(&self) -> Option<&Trailer> {
        self.trailer.as_ref()
    }

    pub fn page_index(&self) -> &[PageIndexElem] {
        &self.page_index
    }

    /// Reads and validates the 100-byte header.
    pub fn decode_header(&mut self) -> Result<Header, LtxError> {
        debug_assert_eq!(self.state, State::Header);
        let mut b = [0u8; HEADER_SIZE];
        self.r.read_exact(&mut b)?;
        self.hash.update(&b);

        let header = Header::decode(&b)?;
        // Rolling checksum starts at the flag when tracking is enabled.
        if !header.no_checksum() {
            self.rolling = Checksum(CHECKSUM_FLAG);
        }
        self.header = Some(header);
        self.state = State::Page;
        Ok(header)
    }

    /// Reads the next page into `data` (which must be `page_size` long).
    ///
    /// Returns `Ok(Some(header))` for a page, or `Ok(None)` once the zero
    /// page-header terminator is reached (after which call [`Decoder::finish`]).
    pub fn decode_page(&mut self, data: &mut [u8]) -> Result<Option<PageHeader>, LtxError> {
        let header = self.header.expect("decode_header must be called first");
        debug_assert_eq!(data.len(), header.page_size as usize);
        if self.state != State::Page {
            return Ok(None);
        }

        // Page header — always hashed, even the zero terminator.
        let mut hb = [0u8; PAGE_HEADER_SIZE];
        self.r.read_exact(&mut hb)?;
        self.hash.update(&hb);
        let ph = PageHeader::decode(&hb)?;

        if ph.is_zero() {
            self.state = State::Close;
            return Ok(None);
        }
        if ph.pgno == 0 {
            return Err(LtxError::ZeroPageNumber);
        }

        if ph.is_block_compressed() {
            // 4-byte compressed size (hashed), then the raw LZ4 block (not hashed).
            let mut sb = [0u8; 4];
            self.r.read_exact(&mut sb)?;
            self.hash.update(&sb);
            let n = u32::from_be_bytes(sb) as usize;

            let mut compressed = vec![0u8; n];
            self.r.read_exact(&mut compressed)?;
            lz4_flex::block::decompress_into(&compressed, data)?;
        } else {
            // Old pre-block LZ4 *frame* format — not produced by current tools.
            return Err(LtxError::FrameFormatUnsupported);
        }

        // The uncompressed page data is what enters the file checksum.
        self.hash.update(data);

        // Accumulate the rolling post-apply checksum for snapshots.
        if header.is_snapshot() && !header.no_checksum() && ph.pgno != lock_pgno(header.page_size) {
            self.rolling =
                Checksum(CHECKSUM_FLAG | (self.rolling.0 ^ checksum_page(ph.pgno, data).0));
        }

        Ok(Some(ph))
    }

    /// Consumes the page index and trailer, verifying the file checksum (and,
    /// for tracked snapshots, the post-apply checksum). Returns the trailer.
    pub fn finish(&mut self) -> Result<Trailer, LtxError> {
        let header = self.header.expect("decode_header must be called first");
        if self.state == State::Closed {
            return Ok(self.trailer.expect("trailer set when closed"));
        }
        debug_assert_eq!(self.state, State::Close);

        // Everything after the zero page-header: index ‖ index-size(8) ‖ trailer(16).
        let mut rest = Vec::new();
        self.r.read_to_end(&mut rest)?;
        if rest.len() < TRAILER_SIZE {
            return Err(LtxError::ShortBuffer {
                need: TRAILER_SIZE,
                got: rest.len(),
            });
        }
        let trailer_at = rest.len() - TRAILER_SIZE;

        // The file checksum covers everything except its own 8 trailing bytes.
        self.hash.update(&rest[..rest.len() - 8]);

        self.page_index = decode_page_index(&rest[..trailer_at])?;
        let trailer = Trailer::decode(&rest[trailer_at..])?;

        let computed = self.hash.checksum();
        if computed != trailer.file_checksum {
            return Err(LtxError::FileChecksumMismatch {
                expected: trailer.file_checksum,
                actual: computed,
            });
        }
        if header.is_snapshot()
            && !header.no_checksum()
            && trailer.post_apply_checksum != self.rolling
        {
            return Err(LtxError::PostApplyChecksumMismatch {
                expected: trailer.post_apply_checksum,
                actual: self.rolling,
            });
        }

        self.trailer = Some(trailer);
        self.state = State::Closed;
        Ok(trailer)
    }
}

/// Parses the page index: `varint(pgno, offset, size)...` up to a `varint(0)`
/// end marker. (The trailing u64 index-size is not needed here.)
fn decode_page_index(mut b: &[u8]) -> Result<Vec<PageIndexElem>, LtxError> {
    let mut out = Vec::new();
    loop {
        let pgno = read_uvarint(&mut b)?;
        if pgno == 0 {
            break;
        }
        let offset = read_uvarint(&mut b)?;
        let size = read_uvarint(&mut b)?;
        out.push(PageIndexElem {
            pgno: pgno as u32,
            offset,
            size,
        });
    }
    Ok(out)
}

/// Reads a Go-compatible unsigned varint from the front of `b`.
fn read_uvarint(b: &mut &[u8]) -> Result<u64, LtxError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = 0;
    loop {
        let byte = *b.get(i).ok_or(LtxError::ShortBuffer {
            need: i + 1,
            got: b.len(),
        })?;
        if byte < 0x80 {
            value |= (byte as u64) << shift;
            i += 1;
            break;
        }
        value |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
        i += 1;
    }
    *b = &b[i..];
    Ok(value)
}

/// A fully-decoded snapshot: its header, the reconstructed database bytes, and
/// the verified trailer.
pub struct Snapshot {
    pub header: Header,
    pub db: Vec<u8>,
    pub trailer: Trailer,
}

/// Decodes a snapshot LTX file and reconstructs the full database image.
///
/// Errors with [`LtxError::NotASnapshot`] if the file is an incremental.
/// The lock page (for databases > 1 GiB) is written as zeros.
pub fn read_snapshot<R: Read>(r: R) -> Result<Snapshot, LtxError> {
    let mut dec = Decoder::new(r);
    let header = dec.decode_header()?;
    if !header.is_snapshot() {
        return Err(LtxError::NotASnapshot);
    }

    let page_size = header.page_size as usize;
    let lock = lock_pgno(header.page_size);
    let mut db = vec![0u8; page_size * header.commit as usize];
    let mut buf = vec![0u8; page_size];

    for pgno in 1..=header.commit {
        let start = (pgno as usize - 1) * page_size;
        if pgno == lock {
            // Leave the lock page zero-filled.
            continue;
        }
        match dec.decode_page(&mut buf)? {
            Some(ph) if ph.pgno == pgno => db[start..start + page_size].copy_from_slice(&buf),
            other => {
                return Err(LtxError::UnexpectedPage {
                    expected: pgno,
                    got: other.map(|p| p.pgno).unwrap_or(0),
                });
            }
        }
    }

    // One more read must hit the terminator so finish() can validate.
    if dec.decode_page(&mut buf)?.is_some() {
        return Err(LtxError::NotASnapshot);
    }
    let trailer = dec.finish()?;

    Ok(Snapshot {
        header,
        db,
        trailer,
    })
}
