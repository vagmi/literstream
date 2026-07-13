//! LTX compaction: merge a contiguous run of LTX files into one, keeping only
//! the latest version of each page.
//!
//! Mirrors `superfly/ltx`'s `Compactor`: a k-way merge across the inputs' sorted
//! page streams (each input is ascending by page number). At each step the
//! lowest page number is written from the *latest* input that carries it; pages
//! beyond the final commit size are dropped. The output header takes
//! `MinTXID`/`pre_apply` from the first input and
//! `MaxTXID`/`commit`/`timestamp`/`post_apply` from the last — so the rolling
//! checksum chain stays valid and the result is `ltx apply`-able.

use super::checksum::Checksum;
use super::decoder::Decoder;
use super::encoder::Encoder;
use super::error::LtxError;
use super::format::Header;

/// Merges `inputs` (LTX byte buffers, sorted ascending by TXID and contiguous)
/// into a single compacted LTX file.
pub fn compact(inputs: &[&[u8]]) -> Result<Vec<u8>, LtxError> {
    if inputs.is_empty() {
        return Err(LtxError::ShortBuffer { need: 1, got: 0 });
    }

    let mut decoders: Vec<Decoder<&[u8]>> = inputs.iter().map(|b| Decoder::new(*b)).collect();
    let mut headers = Vec::with_capacity(decoders.len());
    for dec in &mut decoders {
        headers.push(dec.decode_header()?);
    }

    let page_size = headers[0].page_size;
    for h in &headers[1..] {
        if h.page_size != page_size {
            return Err(LtxError::InvalidPageSize(h.page_size));
        }
    }
    // Require contiguous TXID ranges: each min <= prev_max + 1, max > prev_max.
    for pair in headers.windows(2) {
        let (prev, cur) = (&pair[0], &pair[1]);
        if cur.min_txid > prev.max_txid + 1 || cur.max_txid <= prev.max_txid {
            return Err(LtxError::UnexpectedPage {
                expected: prev.max_txid as u32,
                got: cur.min_txid as u32,
            });
        }
    }

    let first = headers[0];
    let last = headers[headers.len() - 1];
    let commit = last.commit;

    let mut out = Vec::new();
    {
        let mut enc = Encoder::new(&mut out);
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: first.min_txid,
            max_txid: last.max_txid,
            timestamp: last.timestamp,
            pre_apply_checksum: first.pre_apply_checksum,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        })?;

        // Current buffered page for each input (None = needs refill / exhausted).
        let mut bufs: Vec<Option<(u32, Vec<u8>)>> = vec![None; decoders.len()];
        loop {
            // Refill empty buffers.
            for (i, dec) in decoders.iter_mut().enumerate() {
                if bufs[i].is_none() {
                    let mut data = vec![0u8; page_size as usize];
                    if let Some(ph) = dec.decode_page(&mut data)? {
                        bufs[i] = Some((ph.pgno, data));
                    }
                }
            }

            // Lowest page number across all buffers.
            let Some(pgno) = bufs
                .iter()
                .filter_map(|b| b.as_ref().map(|(p, _)| *p))
                .min()
            else {
                break;
            };

            // Write from the latest (highest-index) input carrying this page;
            // consume the page from every input that has it.
            let mut written = false;
            for i in (0..bufs.len()).rev() {
                if bufs[i].as_ref().map(|(p, _)| *p) == Some(pgno) {
                    let (_, data) = bufs[i].take().unwrap();
                    if !written && pgno <= commit {
                        enc.encode_page(pgno, &data)?;
                        written = true;
                    }
                }
            }
        }

        // The post-apply checksum of the merged range is the last input's.
        let last_idx = decoders.len() - 1;
        let mut post_apply = Checksum::ZERO;
        for (i, dec) in decoders.iter_mut().enumerate() {
            let trailer = dec.finish()?;
            if i == last_idx {
                post_apply = trailer.post_apply_checksum;
            }
        }
        enc.set_post_apply_checksum(post_apply);
        enc.finish()?;
    }

    Ok(out)
}
