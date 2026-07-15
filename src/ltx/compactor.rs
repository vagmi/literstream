//! LTX compaction: merge a contiguous run of LTX files into one, keeping only
//! the latest version of each page.
//!
//! Mirrors `superfly/ltx`'s `Compactor` semantics: the latest version of each
//! page wins, pages beyond the final commit size are dropped, and the output
//! header takes `MinTXID`/`pre_apply` from the first input and
//! `MaxTXID`/`commit`/`timestamp`/`post_apply` from the last, so the rolling
//! checksum chain stays valid and the result is `ltx apply`-able.
//!
//! The merge is streaming: it reads only each input's header and page index up
//! front, then walks the inputs in a k-way heap merge, decoding one page at a
//! time and encoding it straight to the output writer. Working memory is
//! O(number of inputs + page_size), not the total decompressed size, so a
//! whole-database base snapshot never has to be materialized in memory.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::Write;

use super::checksum::Checksum;
use super::decoder::{PageIndexElem, decode_page_frame, page_index};
use super::encoder::Encoder;
use super::error::LtxError;
use super::format::{HEADER_FLAG_NO_CHECKSUM, Header};

/// Merges `inputs` (LTX byte buffers, sorted ascending by TXID and contiguous)
/// into a single compacted LTX file in memory. A thin wrapper over
/// [`compact_to_writer`] for tests and small inputs.
pub fn compact(inputs: &[&[u8]]) -> Result<Vec<u8>, LtxError> {
    let mut out = Vec::new();
    compact_to_writer(inputs, &mut out)?;
    Ok(out)
}

/// Streams a compaction of `inputs` (sorted ascending by TXID, contiguous) into
/// `w`, keeping only the latest version of each page. Working memory is
/// O(inputs + page_size).
pub fn compact_to_writer<W: Write>(inputs: &[&[u8]], w: W) -> Result<(), LtxError> {
    if inputs.is_empty() {
        return Err(LtxError::ShortBuffer { need: 1, got: 0 });
    }

    // Read only the headers (not the pages) to validate and shape the output.
    let headers = inputs
        .iter()
        .map(|b| Header::decode(b))
        .collect::<Result<Vec<_>, _>>()?;

    let page_size = headers[0].page_size;
    for pair in headers.windows(2) {
        let (prev, cur) = (&pair[0], &pair[1]);
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

    let first = &headers[0];
    let last = &headers[headers.len() - 1];
    // NoChecksum, matching litestream (its restore rejects compacted files that
    // carry a rolling checksum).
    let header = Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: last.commit,
        min_txid: first.min_txid,
        max_txid: last.max_txid,
        timestamp: last.timestamp,
        pre_apply_checksum: Checksum::ZERO,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    };

    merge_to_writer(inputs, header, w)
}

/// Streams a k-way merge of `inputs` into `w` as a single LTX file with the given
/// `header`, keeping the latest version of each page (a later input wins) and
/// dropping any page beyond `header.commit`.
///
/// Unlike [`compact_to_writer`], contiguity is **not** checked and the header is
/// caller-supplied, so this also serves a restore plan whose ranges overlap (the
/// snapshot rebuild in `Syncer::snapshot`). Working memory is O(inputs +
/// page_size): one decompressed page at a time, plus a heap entry per input.
pub fn merge_to_writer<W: Write>(
    inputs: &[&[u8]],
    header: Header,
    w: W,
) -> Result<(), LtxError> {
    let page_size = header.page_size as usize;
    let commit = header.commit;

    // Each input's page index is pgno-ascending (the encoder sorts it).
    let indexes: Vec<Vec<PageIndexElem>> = inputs
        .iter()
        .map(|b| page_index(b))
        .collect::<Result<_, _>>()?;
    let mut cursor = vec![0usize; inputs.len()];

    // Min-heap over (pgno, input_idx). Same pgno entries come out in ascending
    // input order, so the last one seen for a pgno is the latest (it wins).
    let mut heap: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::new();
    for (i, idx) in indexes.iter().enumerate() {
        if let Some(e) = idx.first() {
            heap.push(Reverse((e.pgno, i)));
        }
    }

    let mut enc = Encoder::new(w);
    enc.encode_header(header)?;

    while let Some(&Reverse((pgno, _))) = heap.peek() {
        // Consume every input sitting at this pgno; the highest index wins.
        let mut winner = 0usize;
        let mut have = false;
        while let Some(&Reverse((p, i))) = heap.peek() {
            if p != pgno {
                break;
            }
            heap.pop();
            if !have || i > winner {
                winner = i;
                have = true;
            }
            cursor[i] += 1;
            if let Some(e) = indexes[i].get(cursor[i]) {
                heap.push(Reverse((e.pgno, i)));
            }
        }

        if pgno <= commit {
            let e = indexes[winner][cursor[winner] - 1];
            let start = e.offset as usize;
            let end = start + e.size as usize;
            if end > inputs[winner].len() {
                return Err(LtxError::ShortBuffer {
                    need: end,
                    got: inputs[winner].len(),
                });
            }
            let (_, data) = decode_page_frame(&inputs[winner][start..end], page_size)?;
            enc.encode_page(pgno, &data)?;
        }
    }

    enc.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::compact;
    use crate::ltx::{Checksum, Encoder, HEADER_FLAG_NO_CHECKSUM, Header, read_file};
    use std::collections::BTreeMap;

    /// Builds a small LTX file with `pages` as `(pgno, fill-byte)` at `page_size`.
    fn ltx(page_size: u32, commit: u32, min: u64, max: u64, pages: &[(u32, u8)]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut enc = Encoder::new(&mut out);
        enc.encode_header(Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size,
            commit,
            min_txid: min,
            max_txid: max,
            timestamp: 0,
            pre_apply_checksum: Checksum::ZERO,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        })
        .unwrap();
        for &(pgno, b) in pages {
            enc.encode_page(pgno, &vec![b; page_size as usize]).unwrap();
        }
        enc.finish().unwrap();
        out
    }

    #[test]
    fn merge_keeps_latest_and_drops_beyond_commit() {
        let ps = 4096u32;
        // Base snapshot: pages 1..3. Later incremental: rewrites page 1 and shrinks
        // the database to 2 pages (as a VACUUM would).
        let base = ltx(ps, 3, 1, 1, &[(1, b'a'), (2, b'a'), (3, b'a')]);
        let incr = ltx(ps, 2, 2, 2, &[(1, b'b')]);

        let merged = compact(&[&base, &incr]).unwrap();
        let f = read_file(&merged).unwrap();

        assert_eq!((f.header.min_txid, f.header.max_txid, f.header.commit), (1, 2, 2));
        let pages: BTreeMap<u32, u8> = f.pages.iter().map(|(p, d)| (*p, d[0])).collect();
        assert_eq!(pages.get(&1), Some(&b'b'), "later input wins page 1");
        assert_eq!(pages.get(&2), Some(&b'a'), "page 2 carried from the base");
        assert_eq!(pages.get(&3), None, "page 3 dropped beyond the new commit");
        assert_eq!(pages.len(), 2);
    }

    #[test]
    fn compact_rejects_non_contiguous_inputs() {
        let ps = 4096u32;
        let a = ltx(ps, 1, 1, 1, &[(1, b'a')]);
        let c = ltx(ps, 1, 3, 3, &[(1, b'c')]); // gap: missing txid 2
        assert!(compact(&[&a, &c]).is_err());
    }
}
