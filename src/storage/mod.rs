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
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path as ObjectPath,
};

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

/// The result of a compare-and-swap [`ReplicaClient::put_ltx_cas`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object did not exist and was written.
    Created,
    /// The object already existed with byte-identical content — an idempotent
    /// retry of our own write. Safe to treat as success.
    AlreadyIdentical,
    /// The object already existed with *different* content — another writer
    /// produced a different LTX at this TXID (split-brain). Never overwrite.
    Conflict,
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

    /// Uploads an LTX object, unconditionally overwriting any existing one.
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

    /// Uploads an LTX object with an if-not-exists guard.
    ///
    /// If the object already exists, the existing bytes are fetched and
    /// compared: identical content is an idempotent retry ([`PutOutcome::AlreadyIdentical`]),
    /// differing content is a split-brain [`PutOutcome::Conflict`] and is never
    /// overwritten. This is the load-bearing multi-writer safety primitive.
    pub async fn put_ltx_cas(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
        bytes: Bytes,
    ) -> Result<PutOutcome, StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, PutPayload::from_bytes(bytes.clone()), opts)
            .await
        {
            Ok(_) => Ok(PutOutcome::Created),
            // AlreadyExists is the normal create-conflict; Precondition can
            // surface when a retry of a *succeeded* conditional PUT sees the
            // object it already wrote. Both mean "an object is already here".
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let existing = self.store.get(&key).await?.bytes().await?;
                if existing == bytes {
                    Ok(PutOutcome::AlreadyIdentical)
                } else {
                    Ok(PutOutcome::Conflict)
                }
            }
            Err(e) => Err(e.into()),
        }
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

    /// Downloads `len` bytes starting at `offset` of an LTX object (a ranged
    /// GET) — used to read just a header, page index, or single page frame.
    pub async fn get_ltx_range(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
        offset: u64,
        len: u64,
    ) -> Result<Bytes, StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        Ok(self.store.get_range(&key, offset..offset + len).await?)
    }

    /// The byte size of an LTX object (a HEAD request).
    pub async fn ltx_size(
        &self,
        level: u32,
        min_txid: u64,
        max_txid: u64,
    ) -> Result<u64, StorageError> {
        let key = self.ltx_key(level, min_txid, max_txid);
        Ok(self.store.head(&key).await?.size)
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn cas_guard_detects_idempotent_retry_and_conflict() {
        let client = ReplicaClient::new(Arc::new(InMemory::new()), "db");

        // First write creates the object.
        assert_eq!(
            client
                .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
                .await
                .unwrap(),
            PutOutcome::Created
        );
        // Re-writing identical bytes is a safe idempotent retry.
        assert_eq!(
            client
                .put_ltx_cas(0, 1, 1, Bytes::from_static(b"alpha"))
                .await
                .unwrap(),
            PutOutcome::AlreadyIdentical
        );
        // Different bytes at the same key is split-brain — never overwritten.
        assert_eq!(
            client
                .put_ltx_cas(0, 1, 1, Bytes::from_static(b"beta"))
                .await
                .unwrap(),
            PutOutcome::Conflict
        );
        // The original content is intact.
        assert_eq!(
            client.get_ltx(0, 1, 1).await.unwrap(),
            Bytes::from_static(b"alpha")
        );
    }
}
