use core::fmt;

/// Errors from the SQLite management layer.
#[derive(Debug)]
pub enum DbError {
    /// An error from `rusqlite`/SQLite.
    Sqlite(rusqlite::Error),
    /// The database could not be put into WAL mode (reports the actual mode).
    NotWalMode(String),
    /// An `sqlite3_file_control` call failed.
    FileControl { op: &'static str, code: i32 },
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Sqlite(e) => write!(f, "sqlite: {e}"),
            DbError::NotWalMode(m) => write!(f, "database is not in WAL mode (got {m:?})"),
            DbError::FileControl { op, code } => {
                write!(f, "file control {op} failed with code {code}")
            }
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::Sqlite(e)
    }
}
