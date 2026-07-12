//! A fail-safe reader over a SQLite WAL byte buffer.
//!
//! Iteration verifies the salt and running checksum of every frame and stops
//! (returns `None`) at the first torn tail, salt change (a new WAL generation),
//! or checksum failure — it never returns an uncommitted/garbage frame. It does
//! *not* enforce transaction boundaries; [`WalReader::page_map`] does that.

use std::collections::HashMap;

use super::error::WalError;
use super::format::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFrameHeader, WalHeader, wal_checksum,
};

/// One validated WAL frame.
#[derive(Clone, Copy, Debug)]
pub struct Frame<'a> {
    pub pgno: u32,
    /// Database size in pages after this frame's transaction; 0 = not a commit.
    pub commit: u32,
    /// Byte offset of this frame's *header* within the WAL.
    pub offset: u64,
    /// The page payload (`page_size` bytes).
    pub data: &'a [u8],
}

impl Frame<'_> {
    pub fn is_commit(&self) -> bool {
        self.commit != 0
    }
}

/// The committed contents of a WAL: the latest frame offset for each page.
#[derive(Clone, Debug, Default)]
pub struct PageMap {
    /// pgno → byte offset of the newest committed frame for that page.
    pub pages: HashMap<u32, u64>,
    /// Database size, in pages, at the final commit.
    pub commit: u32,
    /// End offset of the highest frame in the map (one past its page data).
    pub end_offset: u64,
}

/// A reader over an in-memory WAL buffer.
pub struct WalReader<'a> {
    data: &'a [u8],
    header: WalHeader,
    frame_n: usize,
    /// Running checksum carried from the previous frame (header seeds it).
    chksum: (u32, u32),
}

impl<'a> WalReader<'a> {
    /// Parses and verifies the header, positioning at the first frame.
    pub fn new(data: &'a [u8]) -> Result<WalReader<'a>, WalError> {
        let header = WalHeader::parse(data)?;
        Ok(WalReader {
            data,
            header,
            frame_n: 0,
            chksum: (header.checksum1, header.checksum2),
        })
    }

    pub fn header(&self) -> &WalHeader {
        &self.header
    }

    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    /// Returns the page payload for a frame whose header starts at `offset`.
    pub fn page_data_at(&self, offset: u64) -> &'a [u8] {
        let start = offset as usize + WAL_FRAME_HEADER_SIZE;
        &self.data[start..start + self.header.page_size as usize]
    }

    /// Reads and verifies the next frame, or returns `None` at the end of the
    /// valid WAL (torn tail, salt change, or checksum mismatch).
    pub fn read_frame(&mut self) -> Option<Frame<'a>> {
        let page_size = self.header.page_size as usize;
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let offset = WAL_HEADER_SIZE + self.frame_n * frame_size;

        // Copy the slice reference out so the returned frame borrows the
        // underlying buffer ('a), not `self`.
        let data = self.data;
        if offset + frame_size > data.len() {
            return None; // torn / no more frames
        }

        let hdr = WalFrameHeader::parse(&data[offset..offset + WAL_FRAME_HEADER_SIZE]);
        let page = &data[offset + WAL_FRAME_HEADER_SIZE..offset + frame_size];

        // A salt change marks a different WAL generation → end of this one.
        if hdr.salt1 != self.header.salt1 || hdr.salt2 != self.header.salt2 {
            return None;
        }

        // Running checksum: previous state, then the frame header's first 8
        // bytes (pgno + commit), then the page data.
        let bo = self.header.byte_order;
        let (mut c0, mut c1) = self.chksum;
        (c0, c1) = wal_checksum(bo, c0, c1, &data[offset..offset + 8]);
        (c0, c1) = wal_checksum(bo, c0, c1, page);
        if c0 != hdr.checksum1 || c1 != hdr.checksum2 {
            return None; // torn frame
        }

        self.chksum = (c0, c1);
        self.frame_n += 1;
        Some(Frame {
            pgno: hdr.pgno,
            commit: hdr.commit,
            offset: offset as u64,
            data: page,
        })
    }

    /// Walks all frames and returns the committed [`PageMap`].
    ///
    /// Per-transaction offsets are staged and only promoted to the committed
    /// map on a commit frame; trailing uncommitted frames are dropped, as are
    /// pages beyond the final commit size (e.g. after a `VACUUM` shrink).
    pub fn page_map(&mut self) -> PageMap {
        let mut pages: HashMap<u32, u64> = HashMap::new();
        let mut tx: HashMap<u32, u64> = HashMap::new();
        let mut commit = 0u32;

        while let Some(frame) = self.read_frame() {
            tx.insert(frame.pgno, frame.offset);
            if frame.is_commit() {
                for (&pgno, &offset) in &tx {
                    pages.insert(pgno, offset);
                }
                commit = frame.commit;
            }
        }

        // Drop pages past the final database size.
        pages.retain(|&pgno, _| pgno <= commit);

        let end_offset = pages
            .values()
            .copied()
            .max()
            .map(|max| max + WAL_FRAME_HEADER_SIZE as u64 + self.header.page_size as u64)
            .unwrap_or(0);

        PageMap {
            pages,
            commit,
            end_offset,
        }
    }
}
