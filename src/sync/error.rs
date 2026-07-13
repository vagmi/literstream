use core::fmt;

use crate::db::DbError;
use crate::ltx::LtxError;
use crate::storage::StorageError;
use crate::wal::WalError;

/// Errors from the WAL→LTX sync engine.
#[derive(Debug)]
pub enum SyncError {
    Io(std::io::Error),
    Db(DbError),
    Ltx(LtxError),
    Wal(WalError),
    Storage(StorageError),
    /// An LTX filename in the replica directory was not `<min>-<max>.ltx`.
    BadLtxFilename(String),
    /// The replica has no snapshot to restore from.
    NoSnapshot,
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::Io(e) => write!(f, "io: {e}"),
            SyncError::Db(e) => write!(f, "db: {e}"),
            SyncError::Ltx(e) => write!(f, "ltx: {e}"),
            SyncError::Wal(e) => write!(f, "wal: {e}"),
            SyncError::Storage(e) => write!(f, "storage: {e}"),
            SyncError::BadLtxFilename(n) => write!(f, "bad ltx filename: {n}"),
            SyncError::NoSnapshot => write!(f, "no snapshot available to restore"),
        }
    }
}

impl std::error::Error for SyncError {}

impl From<std::io::Error> for SyncError {
    fn from(e: std::io::Error) -> Self {
        SyncError::Io(e)
    }
}
impl From<DbError> for SyncError {
    fn from(e: DbError) -> Self {
        SyncError::Db(e)
    }
}
impl From<LtxError> for SyncError {
    fn from(e: LtxError) -> Self {
        SyncError::Ltx(e)
    }
}
impl From<WalError> for SyncError {
    fn from(e: WalError) -> Self {
        SyncError::Wal(e)
    }
}
impl From<StorageError> for SyncError {
    fn from(e: StorageError) -> Self {
        SyncError::Storage(e)
    }
}
