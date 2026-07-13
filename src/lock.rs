//! Host-local single-writer advisory lock.
//!
//! literstream must be the only writer of a given database's replica, or two
//! processes could produce conflicting LTX at the same TXID. This takes a
//! non-blocking exclusive `flock` on a `.lock` file beside the database; the
//! lock is released when the [`ProcessLock`] (and its file descriptor) drops —
//! including on crash, since the kernel releases `flock` on process exit.
//!
//! This guards same-host concurrency only. Cross-host safety comes from the CAS
//! equivocation guard on upload (`flock` doesn't span machines or most network
//! filesystems).

use core::fmt;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// An exclusive advisory lock held for the process's lifetime.
pub struct ProcessLock {
    _file: File,
    path: PathBuf,
}

/// Errors acquiring a [`ProcessLock`].
#[derive(Debug)]
pub enum LockError {
    Io(std::io::Error),
    /// Another process already holds the lock.
    Held(PathBuf),
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Io(e) => write!(f, "lock io: {e}"),
            LockError::Held(p) => write!(
                f,
                "another literstream process already holds {}: single writer per database",
                p.display()
            ),
        }
    }
}

impl std::error::Error for LockError {}

impl From<std::io::Error> for LockError {
    fn from(e: std::io::Error) -> Self {
        LockError::Io(e)
    }
}

impl ProcessLock {
    /// Acquires the single-writer lock for `db_path`, failing immediately if
    /// another process holds it.
    pub fn acquire(db_path: &Path) -> Result<ProcessLock, LockError> {
        let path = lock_path_for(db_path);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            return match err.raw_os_error() {
                Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => {
                    Err(LockError::Held(path))
                }
                _ => Err(LockError::Io(err)),
            };
        }
        Ok(ProcessLock { _file: file, path })
    }

    /// The lock file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn lock_path_for(db_path: &Path) -> PathBuf {
    let name = db_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "db".to_string());
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join(format!(".{name}.literstream.lock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_on_same_db_is_rejected() {
        let dir = std::env::temp_dir().join(format!("literstream-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");

        let held = ProcessLock::acquire(&db).unwrap();
        assert!(
            matches!(ProcessLock::acquire(&db), Err(LockError::Held(_))),
            "second lock should be rejected while the first is held"
        );

        drop(held);
        // Released — a fresh acquire now succeeds.
        let _reacquired = ProcessLock::acquire(&db).unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
