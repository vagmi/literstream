//! SQLite WAL on-disk structures and the WAL checksum.
//!
//! Field integers are **big-endian**, but the running checksum reads its 4-byte
//! words in the byte order selected by the header magic — that split is the one
//! easy thing to get wrong.

use super::error::WalError;

/// Size of the WAL header, in bytes.
pub const WAL_HEADER_SIZE: usize = 32;
/// Size of each per-frame header, in bytes.
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

/// Magic for a WAL whose checksums are computed little-endian.
pub const WAL_MAGIC_LE: u32 = 0x377f_0682;
/// Magic for a WAL whose checksums are computed big-endian.
pub const WAL_MAGIC_BE: u32 = 0x377f_0683;
/// The only supported WAL file-format version.
pub const WAL_VERSION: u32 = 3_007_000;

/// The byte order in which the WAL checksum reads its 32-bit words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteOrder {
    Little,
    Big,
}

impl ByteOrder {
    #[inline]
    fn read_u32(self, b: &[u8]) -> u32 {
        let a = [b[0], b[1], b[2], b[3]];
        match self {
            ByteOrder::Little => u32::from_le_bytes(a),
            ByteOrder::Big => u32::from_be_bytes(a),
        }
    }
}

/// The SQLite WAL checksum: a Fibonacci-weighted running sum over 8-byte words,
/// seeded with `(s0, s1)` from the previous frame (or `(0, 0)` for the header).
///
/// `b.len()` must be a multiple of 8. All additions wrap (u32).
pub fn wal_checksum(bo: ByteOrder, mut s0: u32, mut s1: u32, b: &[u8]) -> (u32, u32) {
    debug_assert_eq!(b.len() % 8, 0, "misaligned checksum byte slice");
    let mut i = 0;
    while i + 8 <= b.len() {
        s0 = s0.wrapping_add(bo.read_u32(&b[i..]).wrapping_add(s1));
        s1 = s1.wrapping_add(bo.read_u32(&b[i + 4..]).wrapping_add(s0));
        i += 8;
    }
    (s0, s1)
}

fn be_u32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().unwrap())
}

fn is_valid_page_size(sz: u32) -> bool {
    (512..=65536).contains(&sz) && sz.is_power_of_two()
}

/// The 32-byte WAL header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalHeader {
    /// Byte order for checksum words (from the magic).
    pub byte_order: ByteOrder,
    pub page_size: u32,
    /// Checkpoint sequence number.
    pub seq: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WalHeader {
    /// Parses and verifies the WAL header from at least [`WAL_HEADER_SIZE`] bytes.
    pub fn parse(b: &[u8]) -> Result<WalHeader, WalError> {
        if b.len() < WAL_HEADER_SIZE {
            return Err(WalError::Incomplete {
                need: WAL_HEADER_SIZE,
                got: b.len(),
            });
        }

        let magic = be_u32(b, 0);
        let byte_order = match magic {
            WAL_MAGIC_LE => ByteOrder::Little,
            WAL_MAGIC_BE => ByteOrder::Big,
            _ => return Err(WalError::InvalidMagic(magic)),
        };

        let version = be_u32(b, 4);
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        let page_size = be_u32(b, 8);
        if !is_valid_page_size(page_size) {
            return Err(WalError::InvalidPageSize(page_size));
        }

        let checksum1 = be_u32(b, 24);
        let checksum2 = be_u32(b, 28);
        let (v0, v1) = wal_checksum(byte_order, 0, 0, &b[..24]);
        if v0 != checksum1 || v1 != checksum2 {
            return Err(WalError::HeaderChecksumMismatch);
        }

        Ok(WalHeader {
            byte_order,
            page_size,
            seq: be_u32(b, 12),
            salt1: be_u32(b, 16),
            salt2: be_u32(b, 20),
            checksum1,
            checksum2,
        })
    }
}

/// The 24-byte header preceding each WAL frame's page data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalFrameHeader {
    /// Page number this frame carries.
    pub pgno: u32,
    /// Database size in pages after this frame's transaction; 0 = not a commit.
    pub commit: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WalFrameHeader {
    /// Parses a frame header from at least [`WAL_FRAME_HEADER_SIZE`] bytes.
    pub fn parse(b: &[u8]) -> WalFrameHeader {
        WalFrameHeader {
            pgno: be_u32(b, 0),
            commit: be_u32(b, 4),
            salt1: be_u32(b, 8),
            salt2: be_u32(b, 12),
            checksum1: be_u32(b, 16),
            checksum2: be_u32(b, 20),
        }
    }

    /// True if this frame commits a transaction.
    pub fn is_commit(&self) -> bool {
        self.commit != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_algorithm() {
        // Empty/zero input leaves the seed unchanged.
        assert_eq!(wal_checksum(ByteOrder::Little, 0, 0, &[0u8; 8]), (0, 0));

        // One 8-byte word {word0=1, word1=0}: s0 += (1 + 0); s1 += (0 + s0).
        let le = [1u8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(wal_checksum(ByteOrder::Little, 0, 0, &le), (1, 1));

        // Big-endian reads the same word from the opposite byte order.
        let be = [0u8, 0, 0, 1, 0, 0, 0, 0];
        assert_eq!(wal_checksum(ByteOrder::Big, 0, 0, &be), (1, 1));

        // Additions wrap at u32.
        let max = [0xffu8, 0xff, 0xff, 0xff, 0, 0, 0, 0];
        assert_eq!(wal_checksum(ByteOrder::Little, 1, 0, &max), (0, 0));
    }
}
