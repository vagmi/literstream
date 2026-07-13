use core::fmt;

/// Errors from the replica storage layer.
#[derive(Debug)]
pub enum StorageError {
    ObjectStore(object_store::Error),
}

impl StorageError {
    /// True if the underlying error is a "not found" for a missing object.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            StorageError::ObjectStore(object_store::Error::NotFound { .. })
        )
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::ObjectStore(e) => write!(f, "object store: {e}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::ObjectStore(e) => Some(e),
        }
    }
}

impl From<object_store::Error> for StorageError {
    fn from(e: object_store::Error) -> Self {
        StorageError::ObjectStore(e)
    }
}
