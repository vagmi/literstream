use core::fmt;

/// Errors produced while parsing a SQLite WAL.
#[derive(Debug)]
pub enum WalError {
    /// The buffer was too short to contain a WAL header.
    Incomplete { need: usize, got: usize },
    /// The WAL magic was neither `0x377f0682` nor `0x377f0683`.
    InvalidMagic(u32),
    /// The WAL file-format version was not `3007000`.
    UnsupportedVersion(u32),
    /// Page size is not a power of two in `512..=65536`.
    InvalidPageSize(u32),
    /// The 32-byte header's own checksum did not verify.
    HeaderChecksumMismatch,
    /// A start offset was not aligned to a frame boundary.
    UnalignedOffset(u64),
    /// The frame before a start offset had mismatched salts (WAL discontinuity).
    PrevFrameMismatch,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Incomplete { need, got } => {
                write!(f, "incomplete wal header: need {need}, got {got}")
            }
            WalError::InvalidMagic(m) => write!(f, "invalid wal magic: {m:#010x}"),
            WalError::UnsupportedVersion(v) => write!(f, "unsupported wal version: {v}"),
            WalError::InvalidPageSize(sz) => write!(f, "invalid page size: {sz}"),
            WalError::HeaderChecksumMismatch => write!(f, "wal header checksum mismatch"),
            WalError::UnalignedOffset(o) => write!(f, "unaligned wal offset: {o}"),
            WalError::PrevFrameMismatch => write!(f, "previous wal frame mismatch (discontinuity)"),
        }
    }
}

impl std::error::Error for WalError {}
