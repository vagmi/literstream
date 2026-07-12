//! Encodes a raw SQLite database file into a single snapshot LTX file.
//!
//!     cargo run --example encode_db -- IN.db OUT.ltx
//!
//! This is literstream's counterpart to `ltx encode-db`, and the fixture the
//! cross-tool round-trip script feeds back to Go's `ltx verify`.

use std::process::ExitCode;

use literstream::ltx::write_snapshot;

// Fixed timestamp so output is reproducible and comparable to the Go fixture.
const TIMESTAMP_MS: i64 = 1_700_000_000_000;

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\x00";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: {} IN.db OUT.ltx", args[0]);
        return ExitCode::from(2);
    }

    match run(&args[1], &args[2]) {
        Ok((page_size, commit, trailer)) => {
            println!(
                "wrote {}: page_size={} commit={} post_apply={} file_checksum={}",
                args[2], page_size, commit, trailer.post_apply_checksum, trailer.file_checksum
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(
    db_path: &str,
    out_path: &str,
) -> Result<(u32, u32, literstream::ltx::Trailer), Box<dyn std::error::Error>> {
    let db = std::fs::read(db_path)?;
    if db.len() < 100 || &db[..16] != SQLITE_MAGIC {
        return Err("not a SQLite database".into());
    }

    // Page size: 2-byte big-endian at offset 16; the value 1 means 65536.
    let mut page_size = u16::from_be_bytes([db[16], db[17]]) as u32;
    if page_size == 1 {
        page_size = 65536;
    }

    let out = std::fs::File::create(out_path)?;
    let trailer = write_snapshot(out, page_size, &db, 1, TIMESTAMP_MS)?;
    let commit = (db.len() / page_size as usize) as u32;
    Ok((page_size, commit, trailer))
}
