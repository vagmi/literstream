#!/usr/bin/env bash
#
# Regenerates the WAL-reader test fixtures under tests/fixtures/:
#   wal.db          base database image (as of last checkpoint = ~empty)
#   wal.db-wal      populated, un-checkpointed WAL
#   wal.merged.db   SQLite's own checkpointed result = the expected reconstruction
#
# `cargo test` reads the committed bytes and needs neither python3 nor sqlite3.
# Salts/checksums are random per generation, so regenerating changes the bytes;
# the tests assert structural invariants + the merged-image match, not literals.
#
# Requires: python3, sqlite3.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
FIX="$ROOT/tests/fixtures"

WALDB="$FIX/wal.db"
MERGED="$FIX/wal.merged.db"

python3 "$ROOT/tests/fixtures/gen/gen_wal.py" "$WALDB"

# The expected reconstruction: copy the pair, checkpoint it, drop the WAL.
rm -f "$MERGED" "$MERGED-wal" "$MERGED-shm"
cp "$WALDB" "$MERGED"
cp "$WALDB-wal" "$MERGED-wal"
sqlite3 "$MERGED" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null
rm -f "$MERGED-wal" "$MERGED-shm"

echo "fixtures written:"
ls -l "$WALDB" "$WALDB-wal" "$MERGED"
