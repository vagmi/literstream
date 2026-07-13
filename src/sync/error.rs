use core::fmt;

use crate::db::DbError;
use crate::lock::LockError;
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
    Lock(LockError),
    /// An LTX filename in the replica directory was not `<min>-<max>.ltx`.
    BadLtxFilename(String),
    /// The replica has no snapshot to restore from.
    NoSnapshot,
    /// Another writer produced a different LTX at this TXID (split-brain).
    Equivocation {
        txid: u64,
    },
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::Io(e) => write!(f, "io: {e}"),
            SyncError::Db(e) => write!(f, "db: {e}"),
            SyncError::Ltx(e) => write!(f, "ltx: {e}"),
            SyncError::Wal(e) => write!(f, "wal: {e}"),
            SyncError::Storage(e) => write!(f, "storage: {e}"),
            SyncError::Lock(e) => write!(f, "lock: {e}"),
            SyncError::BadLtxFilename(n) => write!(f, "bad ltx filename: {n}"),
            SyncError::NoSnapshot => write!(f, "no snapshot available to restore"),
            SyncError::Equivocation { txid } => {
                write!(
                    f,
                    "equivocation: another writer wrote a different LTX at txid {txid}"
                )
            }
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
impl From<LockError> for SyncError {
    fn from(e: LockError) -> Self {
        SyncError::Lock(e)
    }
}
