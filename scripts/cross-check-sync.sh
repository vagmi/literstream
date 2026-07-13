#!/usr/bin/env bash
#
# Cross-tool check for the Phase 3 sync engine: prove that Go's superfly/ltx
# tooling can verify and apply the incremental LTX chain literstream produces.
#
#   1. literstream replicates a live DB into <root>/ltx/0/*.ltx
#   2. `ltx verify` accepts every file (snapshot + incrementals)
#   3. `ltx apply` replays the whole chain into a database, byte-identical to
#      literstream's own restore and passing SQLite's integrity check
#
# Requires: cargo, go, references/ltx, sqlite3.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

DB="$WORK/app.db"
REPLICA="$WORK/replica"
RECON="$WORK/reconstructed.db"

echo "1. literstream replicates a live database"
cargo run --quiet --example replicate -- "$DB" "$REPLICA"
echo

LTXDIR="$REPLICA/ltx/0"
# Zero-padded hex filenames sort numerically under the default glob.
FILES=("$LTXDIR"/*.ltx)

echo "2. ltx verify each file"
for f in "${FILES[@]}"; do
  ( cd "$ROOT/references/ltx" && go run ./cmd/ltx verify "$f" ) | sed "s|^|   $(basename "$f"): |"
done
echo

echo "3. ltx apply the whole chain (in TXID order)"
( cd "$ROOT/references/ltx" && go run ./cmd/ltx apply -db "$RECON" "${FILES[@]}" )
echo "   integrity: $(sqlite3 "$RECON" 'PRAGMA integrity_check;')"
echo "   rows:      $(sqlite3 "$RECON" 'SELECT count(*) FROM items;')"

echo "OK: Go verified and applied literstream's incremental LTX chain"
