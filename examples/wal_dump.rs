//! Dumps the structure of a SQLite WAL file: header, every validated frame, and
//! the committed page map.
//!
//!     cargo run --example wal_dump -- path/to/db.sqlite-wal
//!
//! Point it at a `-wal` sidecar (e.g. tests/fixtures/wal.db-wal) to see how WAL
//! frames, commit boundaries, and page-level deduplication actually look.

use std::process::ExitCode;

use literstream::wal::{ByteOrder, WalReader};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: {} PATH-TO-WAL", args[0]);
        return ExitCode::from(2);
    }

    let wal = match std::fs::read(&args[1]) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read {}: {e}", args[1]);
            return ExitCode::FAILURE;
        }
    };

    let mut reader = match WalReader::new(&wal) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("parse wal: {e}");
            return ExitCode::FAILURE;
        }
    };

    let h = *reader.header();
    let bo = match h.byte_order {
        ByteOrder::Little => "little-endian checksums",
        ByteOrder::Big => "big-endian checksums",
    };
    println!("# WAL HEADER");
    println!("byte order:  {bo}");
    println!("page size:   {}", h.page_size);
    println!("seq:         {}", h.seq);
    println!("salt:        {:08x} {:08x}", h.salt1, h.salt2);
    println!("checksum:    {:08x} {:08x}", h.checksum1, h.checksum2);
    println!();

    println!("# FRAMES (validated)");
    let mut frames = 0usize;
    while let Some(f) = reader.read_frame() {
        let marker = if f.is_commit() {
            format!("COMMIT (db size = {} pages)", f.commit)
        } else {
            String::new()
        };
        println!(
            "frame {frames:>4}: pgno={:<6} offset={:<8} {marker}",
            f.pgno, f.offset
        );
        frames += 1;
    }
    println!();

    // Fresh reader for the page map (the loop above consumed the frames).
    let mut reader = WalReader::new(&wal).expect("reparse");
    let pm = reader.page_map();
    println!("# PAGE MAP");
    println!("frames read:      {frames}");
    println!("final db size:    {} pages", pm.commit);
    println!("distinct pages:   {}", pm.pages.len());
    println!("wal end offset:   {}", pm.end_offset);

    ExitCode::SUCCESS
}
