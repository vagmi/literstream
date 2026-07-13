//! Direct page reads from the replica (the foundation of a remote-read VFS).
//!
//! Instead of downloading whole LTX files, [`ReplicaReader`] resolves a single
//! page by ranged GETs: it reads each candidate file's page index from the tail,
//! then fetches just that one page frame. A full SQLite VFS would build on this
//! (serving `xRead` from `read_page`); here it's exposed as a library API.

use std::collections::HashMap;

use crate::ltx::{HEADER_SIZE, Header, INDEX_FOOTER_SIZE, decode_page_frame, decode_page_index};
use crate::storage::ReplicaClient;

use super::{SyncError, list_all_levels, plan_restore, plan_restore_to};

type FileKey = (u32, u64, u64); // (level, min_txid, max_txid)

/// Reads individual database pages directly from a replica via ranged GETs,
/// as of the latest transaction (or a chosen TXID).
pub struct ReplicaReader<'a> {
    client: &'a ReplicaClient,
    /// Restore plan, earliest-first; a page resolves from the newest file back.
    plan: Vec<FileKey>,
    page_size: u32,
    /// Cached page index (pgno → (offset, size)) per file.
    indexes: HashMap<FileKey, HashMap<u32, (u64, u64)>>,
}

impl<'a> ReplicaReader<'a> {
    /// Opens a reader over the replica as of `at_txid` (or the latest if `None`).
    pub async fn open(
        client: &'a ReplicaClient,
        at_txid: Option<u64>,
    ) -> Result<ReplicaReader<'a>, SyncError> {
        let files = list_all_levels(client).await?;
        let plan = match at_txid {
            None => plan_restore(&files)?,
            Some(target) => match plan_restore_to(&files, target) {
                Ok(plan) if !plan.is_empty() => plan,
                Ok(_) | Err(SyncError::NoSnapshot) => {
                    return Err(SyncError::TxidTooOld { requested: target });
                }
                Err(e) => return Err(e),
            },
        };

        // Page size from the base file's header (one ranged GET of 100 bytes).
        let (level, min, max) = plan[0];
        let head = client
            .get_ltx_range(level, min, max, 0, HEADER_SIZE as u64)
            .await?;
        let page_size = Header::decode(&head)?.page_size;

        Ok(ReplicaReader {
            client,
            plan,
            page_size,
            indexes: HashMap::new(),
        })
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Reads the newest version of page `pgno` within this reader's window, or
    /// `None` if the page doesn't exist. Fetches only the page index and the one
    /// page frame — never a whole file.
    pub async fn read_page(&mut self, pgno: u32) -> Result<Option<Vec<u8>>, SyncError> {
        for i in (0..self.plan.len()).rev() {
            let key = self.plan[i];
            let entry = self.load_index(key).await?.get(&pgno).copied();
            if let Some((offset, size)) = entry {
                let (level, min, max) = key;
                let frame = self
                    .client
                    .get_ltx_range(level, min, max, offset, size)
                    .await?;
                let (_, data) = decode_page_frame(&frame, self.page_size as usize)?;
                return Ok(Some(data));
            }
        }
        Ok(None)
    }

    /// Loads (and caches) a file's page index via ranged GETs of its tail.
    async fn load_index(&mut self, key: FileKey) -> Result<&HashMap<u32, (u64, u64)>, SyncError> {
        if !self.indexes.contains_key(&key) {
            let (level, min, max) = key;
            let size = self.client.ltx_size(level, min, max).await?;

            // Last 24 bytes hold the u64 index length + the trailer.
            let footer = self
                .client
                .get_ltx_range(level, min, max, size - INDEX_FOOTER_SIZE, INDEX_FOOTER_SIZE)
                .await?;
            let index_len = u64::from_be_bytes(footer[0..8].try_into().unwrap());
            let index_start = size - INDEX_FOOTER_SIZE - index_len;

            let index_bytes = self
                .client
                .get_ltx_range(level, min, max, index_start, index_len)
                .await?;
            let map = decode_page_index(&index_bytes)?
                .into_iter()
                .map(|e| (e.pgno, (e.offset, e.size)))
                .collect();
            self.indexes.insert(key, map);
        }
        Ok(self.indexes.get(&key).unwrap())
    }
}
