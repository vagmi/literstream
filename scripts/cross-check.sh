#!/usr/bin/env bash
#
# Cross-tool binary-compatibility check for the LTX serializer.
#
# `cargo test` already proves the READ direction (literstream decodes Go's
# fixtures). This script proves the WRITE direction — that Go's `superfly/ltx`
# tooling can read and apply files literstream *produces*:
#
#   1. literstream encodes tests/fixtures/simple.db -> our.ltx
#   2. `ltx verify` accepts our.ltx (checksums valid)
#   3. `ltx apply` reconstructs a DB from our.ltx, byte-identical to the source
#
# Requires: cargo, go, references/ltx, and (for the integrity check) sqlite3.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
SRC="$ROOT/tests/fixtures/simple.db"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

OUR_LTX="$WORK/our.ltx"
RECON="$WORK/reconstructed.db"

echo "1. literstream encodes simple.db -> our.ltx"
cargo run --quiet --example encode_db -- "$SRC" "$OUR_LTX"

echo "2. ltx verify (Go reads our output)"
( cd "$ROOT/references/ltx" && go run ./cmd/ltx verify "$OUR_LTX" )

echo "3. ltx apply -> reconstructed.db, compare to source"
( cd "$ROOT/references/ltx" && go run ./cmd/ltx apply -db "$RECON" "$OUR_LTX" )
if cmp -s "$RECON" "$SRC"; then
  echo "   IDENTICAL: Go reconstructed the exact source DB from our LTX"
else
  echo "   FAIL: reconstructed DB differs from source" >&2
  exit 1
fi

if command -v sqlite3 >/dev/null 2>&1; then
  echo "4. sqlite3 integrity_check on reconstructed DB"
  sqlite3 "$RECON" "PRAGMA integrity_check;"
fi

echo "OK: literstream <-> superfly/ltx are binary compatible (both directions)"
