//! SQLite lifecycle control: opening a database the way literstream needs it,
//! holding a pinned read transaction, and driving checkpoints manually.
//!
//! This is the first module that links real SQLite (`rusqlite`, bundled). It
//! establishes the control model literstream relies on:
//!
//! - **`journal_mode = WAL`** — the mode we replicate.
//! - **`wal_autocheckpoint = 0`** — SQLite never checkpoints on its own; we do.
//! - **`busy_timeout`** — wait rather than fail on a locked database.
//! - **`SQLITE_FCNTL_PERSIST_WAL`** — keep the `-wal` file when connections close.
//! - a **pinned read transaction** over `_literstream_seq` that holds a WAL
//!   read-mark, so an external checkpoint can't reset the WAL under us.
//! - manual **PASSIVE/TRUNCATE** checkpoints (releasing/re-acquiring the lock).
//!
//! Notably we do *not* set `synchronous` — litestream inherits the application's
//! durability choice rather than silently weakening it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension};

mod error;
pub use error::DbError;

/// Default `busy_timeout`, matching litestream's default.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(1);

/// The name of the counter table whose row the pinned read transaction reads.
const SEQ_TABLE: &str = "_literstream_seq";

/// A SQLite checkpoint mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Checkpoint what it can without blocking; never resets the WAL.
    Passive,
    /// Wait for readers, checkpoint everything.
    Full,
    /// Like FULL, then restart the WAL at the beginning.
    Restart,
    /// Like RESTART, then truncate the `-wal` file to zero bytes.
    Truncate,
}

impl CheckpointMode {
    fn as_sql(self) -> &'static str {
        match self {
            CheckpointMode::Passive => "PASSIVE",
            CheckpointMode::Full => "FULL",
            CheckpointMode::Restart => "RESTART",
            CheckpointMode::Truncate => "TRUNCATE",
        }
    }
}

/// The result of `PRAGMA wal_checkpoint(...)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointResult {
    /// True if SQLite couldn't complete the checkpoint (e.g. a reader blocked it).
    pub busy: bool,
    /// Total frames in the WAL at checkpoint time.
    pub log_frames: i64,
    /// Frames actually moved into the database.
    pub checkpointed_frames: i64,
}

impl CheckpointResult {
    /// True if every WAL frame was checkpointed into the database.
    pub fn fully_checkpointed(&self) -> bool {
        !self.busy && self.log_frames == self.checkpointed_frames
    }
}

/// A literstream-managed handle to a SQLite database.
pub struct Db {
    conn: Connection,
    path: PathBuf,
    wal_path: PathBuf,
    page_size: u32,
    read_lock_held: bool,
}

impl Db {
    /// Opens `path` in WAL mode with autocheckpoint disabled, a busy timeout,
    /// persistent WAL, and the `_literstream_seq` bookkeeping table.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Db, DbError> {
        Self::open_with_busy_timeout(path, DEFAULT_BUSY_TIMEOUT)
    }

    /// Like [`Db::open`], with an explicit busy timeout.
    pub fn open_with_busy_timeout<P: AsRef<Path>>(
        path: P,
        busy_timeout: Duration,
    ) -> Result<Db, DbError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;
        conn.busy_timeout(busy_timeout)?;

        // Take control of checkpointing: SQLite must never do it for us.
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;

        // Enable WAL and confirm it took (a read-only fs, for instance, wouldn't).
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::NotWalMode(mode));
        }

        // Keep the -wal file around when the last connection closes, so it can
        // still be read/replicated.
        set_persist_wal(&conn)?;

        let page_size: u32 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;

        // A real, committed row to read so the pinned transaction holds a WAL
        // read-mark (reading sqlite_master alone leaves read-mark 0, which does
        // not block a WAL reset).
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {SEQ_TABLE} (id INTEGER PRIMARY KEY, seq INTEGER);
             INSERT OR IGNORE INTO {SEQ_TABLE} (id, seq) VALUES (1, 0);"
        ))?;

        let mut wal_path = path.clone().into_os_string();
        wal_path.push("-wal");

        Ok(Db {
            conn,
            path,
            wal_path: PathBuf::from(wal_path),
            page_size,
            read_lock_held: false,
        })
    }

    /// Bumps the `_literstream_seq` row — one WAL write. Used right after a
    /// PASSIVE checkpoint (with the read-mark released) to force a fully
    /// backfilled WAL to restart into a fresh generation now, and to seed a real
    /// frame for the next read-mark to pin. Mirrors litestream's approach.
    fn bump_seq(&self) -> Result<(), DbError> {
        self.conn.execute_batch(&format!(
            "INSERT INTO {SEQ_TABLE} (id, seq) VALUES (1, 1)
             ON CONFLICT(id) DO UPDATE SET seq = seq + 1"
        ))?;
        Ok(())
    }

    /// The database file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The database page size, in bytes.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Read-only access to the managed connection (for introspection/queries).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// True while the pinned read transaction is held.
    pub fn read_lock_held(&self) -> bool {
        self.read_lock_held
    }

    /// Current `-wal` file size in bytes (0 if it doesn't exist).
    pub fn wal_size(&self) -> u64 {
        std::fs::metadata(&self.wal_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Number of frames currently in the `-wal` file.
    pub fn wal_frame_count(&self) -> u64 {
        let size = self.wal_size();
        let frame_size = 24 + self.page_size as u64;
        if size <= 32 {
            0
        } else {
            (size - 32) / frame_size
        }
    }

    /// Acquires the pinned read transaction (a WAL read-mark). Idempotent.
    pub fn acquire_read_lock(&mut self) -> Result<(), DbError> {
        if self.read_lock_held {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN")?;
        // Read a real row to actually take a read-mark.
        let _count: i64 =
            self.conn
                .query_row(&format!("SELECT COUNT(1) FROM {SEQ_TABLE}"), [], |r| {
                    r.get(0)
                })?;
        self.read_lock_held = true;
        Ok(())
    }

    /// Releases the pinned read transaction. Idempotent.
    pub fn release_read_lock(&mut self) -> Result<(), DbError> {
        if !self.read_lock_held {
            return Ok(());
        }
        self.conn.execute_batch("ROLLBACK")?;
        self.read_lock_held = false;
        Ok(())
    }

    /// Runs `PRAGMA wal_checkpoint(mode)`, releasing and re-acquiring the read
    /// lock around it (our own read-mark must not block our own checkpoint).
    ///
    /// For a non-blocking PASSIVE checkpoint that fully backfilled the WAL, it
    /// then bumps `_literstream_seq` — while the read-mark is still released — so
    /// the WAL restarts into a fresh generation immediately (bounding it on disk)
    /// instead of growing until some future write triggers the restart.
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointResult, DbError> {
        let had_lock = self.read_lock_held;
        if had_lock {
            self.release_read_lock()?;
        }

        let result = self.conn.query_row(
            &format!("PRAGMA wal_checkpoint({})", mode.as_sql()),
            [],
            |row| {
                Ok(CheckpointResult {
                    busy: row.get::<_, i64>(0)? != 0,
                    log_frames: row.get(1)?,
                    checkpointed_frames: row.get(2)?,
                })
            },
        )?;

        // Force the restart of a fully-checkpointed WAL (PASSIVE doesn't reset the
        // WAL itself; a later write does — so trigger it now, while nothing holds
        // a read-mark to block it).
        if mode == CheckpointMode::Passive && !result.busy {
            self.bump_seq()?;
        }

        if had_lock {
            self.acquire_read_lock()?;
        }
        Ok(result)
    }

    /// Convenience: current `journal_mode` (for tests/introspection).
    pub fn journal_mode(&self) -> Result<String, DbError> {
        Ok(self
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))?)
    }

    /// Convenience: current `wal_autocheckpoint` threshold.
    pub fn wal_autocheckpoint(&self) -> Result<i64, DbError> {
        Ok(self
            .conn
            .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))?)
    }

    /// The last value written to the `_litestream_seq` counter, if present.
    pub fn seq(&self) -> Result<Option<i64>, DbError> {
        Ok(self
            .conn
            .query_row(
                &format!("SELECT seq FROM {SEQ_TABLE} WHERE id=1"),
                [],
                |r| r.get(0),
            )
            .optional()?)
    }
}

/// Sets `SQLITE_FCNTL_PERSIST_WAL` on the `main` database via a file control.
fn set_persist_wal(conn: &Connection) -> Result<(), DbError> {
    let schema = c"main";
    let mut enable: std::os::raw::c_int = 1;
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            schema.as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_PERSIST_WAL,
            (&mut enable as *mut std::os::raw::c_int).cast(),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(DbError::FileControl {
            op: "persist_wal",
            code: rc,
        });
    }
    Ok(())
}
