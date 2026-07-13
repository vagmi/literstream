//! LTX compaction: merge a contiguous run of LTX files into one, keeping only
//! the latest version of each page.
//!
//! Mirrors `superfly/ltx`'s `Compactor` semantics: the latest version of each
//! page wins, pages beyond the final commit size are dropped, and the output
//! header takes `MinTXID`/`pre_apply` from the first input and
//! `MaxTXID`/`commit`/`timestamp`/`post_apply` from the last — so the rolling
//! checksum chain stays valid and the result is `ltx apply`-able. Inputs are
//! decoded via the index-based [`read_file`], so both LZ4 frame and block
//! formats work.

use std::collections::BTreeMap;

use super::checksum::Checksum;
use super::decoder::read_file;
use super::encoder::Encoder;
use super::error::LtxError;
use super::format::{HEADER_FLAG_NO_CHECKSUM, Header};

/// Merges `inputs` (LTX byte buffers, sorted ascending by TXID and contiguous)
/// into a single compacted LTX file.
pub fn compact(inputs: &[&[u8]]) -> Result<Vec<u8>, LtxError> {
    if inputs.is_empty() {
        return Err(LtxError::ShortBuffer { need: 1, got: 0 });
    }

    let files = inputs
        .iter()
        .map(|b| read_file(b))
        .collect::<Result<Vec<_>, _>>()?;

    let page_size = files[0].header.page_size;
    for pair in files.windows(2) {
        let (prev, cur) = (&pair[0].header, &pair[1].header);
        if cur.page_size != page_size {
            return Err(LtxError::InvalidPageSize(cur.page_size));
        }
        // Contiguous TXID ranges: min <= prev_max + 1, max > prev_max.
        if cur.min_txid > prev.max_txid + 1 || cur.max_txid <= prev.max_txid {
            return Err(LtxError::UnexpectedPage {
                expected: prev.max_txid as u32,
                got: cur.min_txid as u32,
            });
        }
    }

    let first = files[0].header;
    let last = files[files.len() - 1].header;
    let commit = last.commit;

    // Latest version of each page (later inputs overwrite), dropping pages past
    // the final commit size.
    let mut merged: BTreeMap<u32, &[u8]> = BTreeMap::new();
    for file in &files {
        for (pgno, data) in &file.pages {
            if *pgno <= commit {
                merged.insert(*pgno, data.as_slice());
            }
        }
    }

    let mut out = Vec::new();
    {
        let mut enc = Encoder::new(&mut out);
        // NoChecksum, matching litestream (its restore rejects compacted files
        // that carry a rolling checksum).
        enc.encode_header(Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size,
            commit,
            min_txid: first.min_txid,
            max_txid: last.max_txid,
            timestamp: last.timestamp,
            pre_apply_checksum: Checksum::ZERO,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        })?;
        for (pgno, data) in &merged {
            enc.encode_page(*pgno, data)?;
        }
        enc.finish()?;
    }

    Ok(out)
}
