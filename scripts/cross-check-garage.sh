#!/usr/bin/env bash
# Cross-tool LTX compatibility check on Garage (S3), both directions:
#   A) literstream (Rust) replicates -> litestream (Go) restores
#   B) litestream (Go)   replicates -> literstream (Rust) restores
#
# Both tools use the same remote object-store layout (<prefix>/<level:04x>/…),
# so this verifies the LTX files round-trip across implementations.
#
# Requires: Garage up + docker/garage/.garage.env, /tmp/litestream, and
#   cargo build --release --features cli --bin literstream
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
# shellcheck disable=SC1091
source "$ROOT/docker/garage/.garage.env"
LITE=/tmp/litestream
RUST="$ROOT/target/release/literstream"
WORK=$(mktemp -d /tmp/xcheck.XXXXXX)
STAMP=$(date +%s)

# write_rows <db> <n>: WAL-mode DB, N rows over a long-lived connection, then hold
# it open a few seconds so the replicator can drain before the WAL can reset.
write_rows() {
  python3 - "$1" "$2" <<'PY'
import sqlite3, sys, os, time
db, n = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(db, isolation_level=None)
c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA wal_autocheckpoint=0"); c.execute("PRAGMA busy_timeout=5000")
c.execute("CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY AUTOINCREMENT, v BLOB)")
for _ in range(n // 20):
    c.execute("BEGIN"); c.executemany("INSERT INTO kv(v) VALUES (?)", [(os.urandom(200),) for _ in range(20)]); c.execute("COMMIT")
    time.sleep(0.05)
print("source rows:", c.execute("SELECT COUNT(*) FROM kv").fetchone()[0], flush=True)
time.sleep(6); c.close()
PY
}

count() { python3 -c "import sqlite3,sys;print(sqlite3.connect(sys.argv[1]).execute('select count(*) from kv').fetchone()[0])" "$1" 2>/dev/null || echo ERR; }
integ() { python3 -c "import sqlite3,sys;print(sqlite3.connect(sys.argv[1]).execute('pragma integrity_check').fetchone()[0])" "$1" 2>/dev/null || echo ERR; }

litestream_cfg() { # <db> <prefix> -> writes a config file, echoes its path
  local cfg="$WORK/lite-$2.yml"
  cat >"$cfg" <<YML
dbs:
  - path: $1
    replica:
      type: s3
      bucket: $LITESTREAM_S3_BUCKET
      path: $2
      region: $LITESTREAM_S3_REGION
      endpoint: $LITESTREAM_S3_ENDPOINT
      force-path-style: true
      access-key-id: $LITESTREAM_S3_ACCESS_KEY
      secret-access-key: $LITESTREAM_S3_SECRET
YML
  echo "$cfg"
}

echo "work dir: $WORK"; echo

# ---------------------------------------------------------------------------
echo "### A) literstream (Rust) replicates -> litestream (Go) restores"
DB_A="$WORK/a.db"; PFX_A="xcheck-rust-$STAMP"
rm -f "$DB_A" "$DB_A"-wal "$DB_A"-shm
"$RUST" replicate "$DB_A" "s3://$PFX_A" >"$WORK/a-rep.log" 2>&1 &
RP=$!; sleep 2
write_rows "$DB_A" 1000 | sed 's/^/  /'
sleep 2; kill -INT $RP; wait $RP 2>/dev/null || true
CFG_A=$(litestream_cfg "$DB_A" "$PFX_A")
"$LITE" restore -config "$CFG_A" -o "$WORK/a-out.db" "$DB_A" >"$WORK/a-res.log" 2>&1 \
  && echo "  litestream restored: $(count "$WORK/a-out.db") rows, integrity=$(integ "$WORK/a-out.db") [want $(count "$DB_A")]" \
  || { echo "  litestream restore FAILED:"; sed 's/^/    /' "$WORK/a-res.log"; }
echo

# ---------------------------------------------------------------------------
echo "### B) litestream (Go) replicates -> literstream (Rust) restores"
DB_B="$WORK/b.db"; PFX_B="xcheck-go-$STAMP"
rm -f "$DB_B" "$DB_B"-wal "$DB_B"-shm
python3 -c "import sqlite3,sys;c=sqlite3.connect(sys.argv[1],isolation_level=None);c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA wal_autocheckpoint=0');c.execute('CREATE TABLE kv(id INTEGER PRIMARY KEY AUTOINCREMENT, v BLOB)');c.close()" "$DB_B"
CFG_B=$(litestream_cfg "$DB_B" "$PFX_B")
"$LITE" replicate -config "$CFG_B" >"$WORK/b-rep.log" 2>&1 &
LP=$!; sleep 2
write_rows "$DB_B" 1000 | sed 's/^/  /'
sleep 2; kill -INT $LP; wait $LP 2>/dev/null || true
"$RUST" restore "$WORK/b-out.db" "s3://$PFX_B" >"$WORK/b-res.log" 2>&1 \
  && echo "  literstream restored: $(count "$WORK/b-out.db") rows, integrity=$(integ "$WORK/b-out.db") [want $(count "$DB_B")]" \
  || { echo "  literstream restore FAILED:"; sed 's/^/    /' "$WORK/b-res.log"; }
echo
echo "artifacts in $WORK"
