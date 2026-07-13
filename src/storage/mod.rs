//! The replica client: literstream's view of remote (or local) storage.
//!
//! It is a thin async wrapper over an [`object_store::ObjectStore`], which
//! already abstracts local disk, in-memory, S3/Garage, GCS, and Azure behind one
//! trait. We only add the LTX key layout (`<prefix>/ltx/<level>/<min>-<max>.ltx`)
//! and put/get/list/delete over it. Keys are zero-padded hex so the object
//! store's lexicographic listing is also TXID order.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjectPath};

mod error;
pub use error::StorageError;

/// Metadata about one stored LTX object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxFileInfo {
    pub level: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    pub size: u64,
}

/// An async handle to a replica's object storage.
#[derive(Clone)]
pub struct ReplicaClient {
    store: Arc<dyn ObjectStore>,
    /// Key prefix within the store (may be empty), without trailing slash.
    prefix: String,
}

impl ReplicaClient {
    /// Wraps an object store, placing all keys under `prefix` (use `""` for the
    /// store root).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> ReplicaClient {
        ReplicaClient {
            store,
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    fn level_dir(&self, level: u32) -> String {
        if self.prefix.is_empty() {
            format!("ltx/{level}")
        } else {
            format!("{}/ltx/{level}", self.prefix)
        }
    }

    fn ltx_key(&self, level: u32, min_txid: u64, max_txid: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/{min_txid:016x}-{max_txid:016x}.ltx",
            self.level_dir(level)
        ))
    }

    /// Uploads an LTX object.
    pub async fn put_ltx(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
        bytes: Bytes,
    ) -> Result<(), StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        self.store.put(&key, PutPayload::from_bytes(bytes)).await?;
        Ok(())
    }

    /// Downloads an LTX object.
    pub async fn get_ltx(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
    ) -> Result<Bytes, StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        let result = self.store.get(&key).await?;
        Ok(result.bytes().await?)
    }

    /// Deletes an LTX object.
    pub async fn delete_ltx(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
    ) -> Result<(), StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        self.store.delete(&key).await?;
        Ok(())
    }

    /// Lists the LTX objects at a level, sorted by TXID.
    pub async fn list_ltx(&self, level: u32) -> Result<Vec<LtxFileInfo>, StorageError> {
        let dir = ObjectPath::from(self.level_dir(level));
        let mut stream = self.store.list(Some(&dir));

        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let filename = meta.location.filename().unwrap_or_default();
            if let Some((min_txid, max_txid)) = parse_ltx_filename(filename) {
                out.push(LtxFileInfo {
                    level,
                    min_txid,
                    max_txid,
                    size: meta.size,
                });
            }
        }
        out.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(out)
    }
}

/// Parses `<min>-<max>.ltx` into its TXID range.
pub fn parse_ltx_filename(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".ltx")?;
    let (min, max) = stem.split_once('-')?;
    Some((
        u64::from_str_radix(min, 16).ok()?,
        u64::from_str_radix(max, 16).ok()?,
    ))
}
