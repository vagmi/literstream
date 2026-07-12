#!/usr/bin/env bash
#
# Regenerates the LTX read-path test fixtures under tests/fixtures/.
#
# Requires:
#   - sqlite3            (to build a deterministic source database)
#   - go                 (to run the fixture generator)
#   - references/ltx     (the superfly/ltx checkout the generator links against)
#
# The generated .db + .ltx pair is committed; `cargo test` reads the committed
# bytes and needs NEITHER go NOR sqlite3. Only run this to refresh fixtures.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
FIX="$ROOT/tests/fixtures"
mkdir -p "$FIX"

DB="$FIX/simple.db"
LTX="$FIX/simple.ltx"

rm -f "$DB" "$DB-wal" "$DB-shm" "$LTX"

# Deterministic source DB: 300 rows, page_size 4096, rollback-journal mode so
# no -wal/-shm sidecars are left behind.
sqlite3 "$DB" <<'SQL'
PRAGMA page_size=4096;
PRAGMA journal_mode=DELETE;
CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL, note TEXT);
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < 300)
INSERT INTO items(id, name, note)
  SELECT i,
         'item-' || printf('%04d', i),
         'note for item ' || i || ' :: ' || substr('the quick brown fox jumps over the lazy dog', 1, 30)
  FROM c;
SQL

# Encode the DB into a single snapshot LTX file (Version 3) via the local
# superfly/ltx library. Fixed timestamp keeps regeneration byte-reproducible.
( cd "$ROOT/tests/fixtures/gen" && go mod tidy >/dev/null 2>&1 && go run . -o "$LTX" "$DB" )

echo "fixtures written:"
ls -l "$DB" "$LTX"
