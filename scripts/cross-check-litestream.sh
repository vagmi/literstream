#!/usr/bin/env bash
#
# The ultimate compatibility gate: round-trip against the REAL Go litestream
# binary over GCS, both directions.
#
#   A. litestream replicates  -> literstream restores
#   B. literstream replicates -> litestream restores
#
# Requires: cargo, go, sqlite3, and GCS credentials in the environment
# (GOOGLE_APPLICATION_CREDENTIALS). Run via direnv so those are set:
#
#   direnv exec . env LITESTREAM_GCS_BUCKET=literstream-test-bucket \
#     ./scripts/cross-check-litestream.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BUCKET="${LITESTREAM_GCS_BUCKET:-literstream-test-bucket}"
LS="${LITESTREAM_BIN:-/tmp/litestream}"

if [ ! -x "$LS" ]; then
  echo "building litestream -> $LS"
  ( cd references/litestream && go build -o "$LS" ./cmd/litestream )
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
STAMP="$(date +%s)"

summary() { # <db>  ->  "rows:total-note-length"
  sqlite3 "$1" "SELECT count(*) || ':' || coalesce(sum(length(note)),0) FROM items"
}

echo "=== Direction A: litestream replicates -> literstream restores ==="
PA="xcompat-a-$STAMP"
sqlite3 "$WORK/a.db" "PRAGMA journal_mode=WAL;
  CREATE TABLE items(id INTEGER PRIMARY KEY, note TEXT);
  WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<150)
  INSERT INTO items(note) SELECT 'row-'||i FROM c;
  PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null
"$LS" replicate -once -force-snapshot "$WORK/a.db" "gs://$BUCKET/$PA" >/dev/null 2>&1
cargo run --quiet --example gcs_restore -- "$BUCKET" "$PA" "$WORK/a_out.db" >/dev/null
echo "  integrity: $(sqlite3 "$WORK/a_out.db" 'PRAGMA integrity_check;')"
if [ "$(summary "$WORK/a.db")" = "$(summary "$WORK/a_out.db")" ]; then
  echo "  A OK: literstream restored litestream's backup ($(summary "$WORK/a_out.db"))"
else
  echo "  A FAIL: source=$(summary "$WORK/a.db") restored=$(summary "$WORK/a_out.db")" >&2
  exit 1
fi

echo "=== Direction B: literstream replicates -> litestream restores ==="
PB="xcompat-b-$STAMP"
cargo run --quiet --example gcs_replicate -- "$WORK/b.db" "$BUCKET" "$PB" >/dev/null
"$LS" restore -o "$WORK/b_out.db" "gs://$BUCKET/$PB" >/dev/null 2>&1
cargo run --quiet --example gcs_restore -- "$BUCKET" "$PB" "$WORK/b_ours.db" >/dev/null
echo "  integrity: $(sqlite3 "$WORK/b_out.db" 'PRAGMA integrity_check;')"
if [ "$(summary "$WORK/b_out.db")" = "$(summary "$WORK/b_ours.db")" ]; then
  echo "  B OK: litestream restored literstream's backup ($(summary "$WORK/b_out.db"))"
else
  echo "  B FAIL: litestream=$(summary "$WORK/b_out.db") ours=$(summary "$WORK/b_ours.db")" >&2
  exit 1
fi

echo
echo "OK: literstream <-> litestream are binary compatible over GCS, both directions."
