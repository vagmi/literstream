//! CRC64 checksums, matching Go's `hash/crc64` with the `crc64.ISO` table.
//!
//! This is *not* the same as the `crc` crate's catalog "CRC-64/GO-ISO" (which
//! uses all-ones init/xorout). Go's `crc64.ISO` is a reflected CRC-64 with the
//! ISO 3309 polynomial (normal `0x1B`, reflected `0xD800000000000000`),
//! init 0, and no final XOR. We hand-roll it so binary compatibility is exact
//! and provable against Litestream-produced fixtures.

use core::fmt;

/// High bit OR'd into every LTX checksum so a valid checksum is never zero.
pub const CHECKSUM_FLAG: u64 = 1 << 63;

/// Reflected ISO 3309 polynomial (normal form `0x1B`), as Go's `crc64.ISO`.
const POLY_REFLECTED: u64 = 0xD800_0000_0000_0000;

/// Lookup table built at compile time from [`POLY_REFLECTED`].
static TABLE: [u64; 256] = build_table();

const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// A rolling CRC64 hasher matching Go's `hash/crc64` with the ISO table.
///
/// Go's `update` does `crc = ^crc` on entry and `return ^crc` on exit, so the
/// effective parameters are **init = all-ones, xorout = all-ones** (this is why
/// the reveng catalog's "CRC-64/GO-ISO" check value is `b90956c775a41001`, and
/// why init=0/xorout=0 is *wrong* here). We keep the un-XORed value internally
/// and apply the final XOR only in [`Hasher::sum64`], which keeps the hasher
/// streaming-composable across any chunk boundaries.
#[derive(Clone, Debug)]
pub struct Hasher {
    crc: u64,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Self {
        Self { crc: !0 }
    }

    /// Feeds `data` into the running checksum.
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        for &b in data {
            crc = TABLE[((crc ^ b as u64) & 0xff) as usize] ^ (crc >> 8);
        }
        self.crc = crc;
    }

    /// The CRC64 value (with the final all-ones XOR applied), without
    /// [`CHECKSUM_FLAG`].
    pub fn sum64(&self) -> u64 {
        self.crc ^ !0
    }

    /// The CRC64 value with [`CHECKSUM_FLAG`] set, as stored in LTX files.
    pub fn checksum(&self) -> Checksum {
        Checksum(CHECKSUM_FLAG | self.sum64())
    }
}

/// An LTX checksum: a CRC64 value with [`CHECKSUM_FLAG`] set. Zero means unset.
#[derive(Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Checksum(pub u64);

impl Checksum {
    pub const ZERO: Checksum = Checksum(0);

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checksum({:016x})", self.0)
    }
}

/// CRC64-ISO of a single page: `crc(be(pgno) ‖ data)`, with [`CHECKSUM_FLAG`].
///
/// The rolling database checksum is the XOR of every page's `checksum_page`
/// (order-independent), which makes incremental updates O(changed pages).
pub fn checksum_page(pgno: u32, data: &[u8]) -> Checksum {
    let mut h = Hasher::new();
    h.update(&pgno.to_be_bytes());
    h.update(data);
    h.checksum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_iso_check_vector() {
        // The reveng catalog check value for CRC-64/GO-ISO, i.e. Go's
        // `hash/crc64` with `crc64.ISO`. Guards the init/xorout=all-ones detail.
        let mut h = Hasher::new();
        h.update(b"123456789");
        assert_eq!(h.sum64(), 0xb909_56c7_75a4_1001);
    }

    #[test]
    fn crc64_streaming_matches_single_shot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let mut one = Hasher::new();
        one.update(data);

        let mut split = Hasher::new();
        split.update(&data[..10]);
        split.update(&data[10..]);

        assert_eq!(one.sum64(), split.sum64());
    }

    #[test]
    fn checksum_flag_is_always_set() {
        // Even if the raw CRC's high bit is clear, the flag forces it on.
        let c = checksum_page(1, &[0u8; 4096]);
        assert_ne!(c.0 & CHECKSUM_FLAG, 0);
    }
}
