use core::fmt;

use super::checksum::Checksum;

/// Errors produced while reading or writing LTX files.
#[derive(Debug)]
pub enum LtxError {
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// A fixed-size structure was shorter than required.
    ShortBuffer { need: usize, got: usize },
    /// Header magic was not `"LTX1"`.
    InvalidMagic([u8; 4]),
    /// Page size is not a power of two in `512..=65536`.
    InvalidPageSize(u32),
    /// A page header had page number 0 where a real page was expected.
    ZeroPageNumber,
    /// LZ4 block decompression failed.
    Lz4(lz4_flex::block::DecompressError),
    /// The old (pre-block) LZ4 *frame* page format is not yet supported.
    FrameFormatUnsupported,
    /// The computed file checksum disagreed with the trailer.
    FileChecksumMismatch {
        expected: Checksum,
        actual: Checksum,
    },
    /// The computed post-apply checksum disagreed with the trailer.
    PostApplyChecksumMismatch {
        expected: Checksum,
        actual: Checksum,
    },
    /// A non-snapshot file was passed where a snapshot was required.
    NotASnapshot,
    /// Pages arrived out of the expected snapshot order.
    UnexpectedPage { expected: u32, got: u32 },
    /// TXID range is invalid (zero or min > max).
    InvalidTxidRange { min: u64, max: u64 },
    /// A snapshot header carried a non-zero pre-apply checksum.
    SnapshotPreApplyChecksum,
    /// A page was encoded out of ascending order, or before page 1.
    PageOutOfOrder { prev: u32, pgno: u32 },
    /// A page number exceeded the header's commit size.
    PageBeyondCommit { pgno: u32, commit: u32 },
    /// The lock page was passed to the encoder (it must be skipped).
    LockPageEncoded(u32),
    /// A page buffer's length did not match the header page size.
    WrongPageLength { expected: u32, got: usize },
}

impl fmt::Display for LtxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LtxError::Io(e) => write!(f, "io: {e}"),
            LtxError::ShortBuffer { need, got } => {
                write!(f, "short buffer: need {need}, got {got}")
            }
            LtxError::InvalidMagic(m) => write!(f, "invalid magic: {m:02x?}"),
            LtxError::InvalidPageSize(sz) => write!(f, "invalid page size: {sz}"),
            LtxError::ZeroPageNumber => write!(f, "page number required"),
            LtxError::Lz4(e) => write!(f, "lz4 decompress: {e}"),
            LtxError::FrameFormatUnsupported => {
                write!(f, "old lz4 frame page format not supported")
            }
            LtxError::FileChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "file checksum mismatch: expected {expected}, got {actual}"
                )
            }
            LtxError::PostApplyChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "post-apply checksum mismatch: expected {expected}, got {actual}"
                )
            }
            LtxError::NotASnapshot => write!(f, "not a snapshot LTX file"),
            LtxError::UnexpectedPage { expected, got } => {
                write!(f, "unexpected page: expected pgno {expected}, got {got}")
            }
            LtxError::InvalidTxidRange { min, max } => {
                write!(f, "invalid txid range: ({min}, {max})")
            }
            LtxError::SnapshotPreApplyChecksum => {
                write!(f, "pre-apply checksum must be zero on snapshots")
            }
            LtxError::PageOutOfOrder { prev, pgno } => {
                write!(f, "page out of order: prev {prev}, pgno {pgno}")
            }
            LtxError::PageBeyondCommit { pgno, commit } => {
                write!(f, "page {pgno} out-of-bounds for commit size {commit}")
            }
            LtxError::LockPageEncoded(pgno) => {
                write!(f, "cannot encode lock page: pgno={pgno}")
            }
            LtxError::WrongPageLength { expected, got } => {
                write!(f, "wrong page length: expected {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for LtxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LtxError::Io(e) => Some(e),
            LtxError::Lz4(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LtxError {
    fn from(e: std::io::Error) -> Self {
        LtxError::Io(e)
    }
}

impl From<lz4_flex::block::DecompressError> for LtxError {
    fn from(e: lz4_flex::block::DecompressError) -> Self {
        LtxError::Lz4(e)
    }
}
