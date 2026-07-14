#!/usr/bin/env bash
# litestream (Go) vs literstream (Rust): CPU / memory while continuously
# replicating an identical SQLite write workload to a LOCAL directory replica
# (litestream `file` type / object_store LocalFileSystem) — no network in the
# loop, so this measures each tool's own overhead.
#
# Sequential runs (no cross-tool contention), identical deterministic workload
# over a long-lived writer connection, same 1s monitor interval. The replicator
# is signalled to drain *while the writer is still connected*, then verified by
# restoring to the correct row count.
#
#   ./scripts/bench/run.sh [duration_s] [batches_per_s] [rows_per_batch] [payload_bytes]
#
# Requires: /tmp/litestream and
#   cargo build --release --features cli --bin literstream
set -euo pipefail

DURATION=${1:-60}
BPS=${2:-10}
RPB=${3:-50}
PAYLOAD=${4:-200}
SETTLE=6

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
LITE_BIN=${LITE_BIN:-/tmp/litestream}
RUST_BIN=${RUST_BIN:-$ROOT/target/release/literstream}
WORK=${BENCH_WORK:-$(mktemp -d /tmp/litbench.XXXXXX)}

echo "workload : ${DURATION}s, ${BPS} batches/s x ${RPB} rows x ${PAYLOAD}B  (~$((BPS*RPB)) rows/s)"
echo "replica  : local directory  |  work dir: $WORK"
echo

# sample <pid> <outfile>: append "rss_kb cpu_time" each second until pid exits.
sample() {
  local pid=$1 out=$2
  while kill -0 "$pid" 2>/dev/null; do
    ps -o rss=,time= -p "$pid" 2>/dev/null | awk 'NF>=2{print $1, $2}' >>"$out" || true
    sleep 1
  done
}

summarize() { # <samples> <wall-seconds>
  awk -v wall="$2" '
    function tosec(t,  n,a){ n=split(t,a,":"); return (n==3)?a[1]*3600+a[2]*60+a[3]:a[1]*60+a[2] }
    { if($1>peak)peak=$1; sum+=$1; n++; last=$2 }
    END{
      if(n==0){print "  (no samples)"; exit}
      printf "  peak RSS : %8.1f MB\n", peak/1024
      printf "  avg  RSS : %8.1f MB   (%d samples)\n", (sum/n)/1024, n
      printf "  CPU time : %8.2f s    (%.1f%% of one core over %ss wall)\n", tosec(last), (wall>0?100*tosec(last)/wall:0), wall
    }' "$1"
}

count() { python3 -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute('select count(*) from kv').fetchone()[0])" "$1" 2>/dev/null || echo ERR; }

# run_tool <name> <db> <replica> <start-cmd...>
run_tool() {
  local name=$1 db=$2 replica=$3; shift 3
  local log="$WORK/$name.log" samples="$WORK/$name.samples"
  : >"$samples"; rm -rf "$db" "$db-wal" "$db-shm" "$replica"; mkdir -p "$replica"

  # Pre-create the DB (WAL, table) so the replicator has something to attach to.
  python3 - "$db" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA wal_autocheckpoint=0")
c.execute("CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY AUTOINCREMENT, val BLOB)")
c.close()
PY

  echo "### $name"
  "$@" >"$log" 2>&1 &
  local pid=$!
  sleep 3
  if ! kill -0 "$pid" 2>/dev/null; then echo "  !! exited early:"; sed 's/^/     /' "$log"; return 1; fi

  sample "$pid" "$samples" & local spid=$!
  local t0=$SECONDS

  # Writer runs for DURATION then holds the connection open for SETTLE seconds.
  python3 "$HERE/workload.py" "$db" "$DURATION" "$BPS" "$RPB" "$PAYLOAD" "$SETTLE" | sed 's/^/  /' &
  local wpid=$!

  # Signal the replicator to drain while the writer is still connected.
  sleep $((DURATION + 2))
  kill -INT "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  local wall=$((SECONDS - t0))
  wait "$wpid" 2>/dev/null || true
  kill "$spid" 2>/dev/null || true; wait "$spid" 2>/dev/null || true

  summarize "$samples" "$wall"

  # Verify with each tool's OWN restore (their replica key layouts differ).
  local out="$WORK/restore-$name.db" expected got ok
  expected=$(count "$db")
  case "$name" in
    literstream-rust) "$RUST_BIN" restore "$out" "$replica" >>"$WORK/restore.log" 2>&1 ;;
    litestream-go)    "$LITE_BIN" restore -config "$WORK/litestream.yml" -o "$out" "$db" >>"$WORK/restore.log" 2>&1 ;;
  esac
  if [ -f "$out" ]; then
    got=$(count "$out"); ok=$([ "$got" = "$expected" ] && echo OK || echo MISMATCH)
    echo "  restore  : $got rows (source $expected) [$ok]"
  else
    echo "  restore  : FAILED (see $WORK/restore.log)"
  fi
  echo
}

run_tool "literstream-rust" "$WORK/rust.db" "$WORK/rust-replica" \
  "$RUST_BIN" replicate "$WORK/rust.db" "$WORK/rust-replica"

cat >"$WORK/litestream.yml" <<YML
dbs:
  - path: $WORK/go.db
    replica:
      type: file
      path: $WORK/go-replica
YML
run_tool "litestream-go" "$WORK/go.db" "$WORK/go-replica" \
  "$LITE_BIN" replicate -config "$WORK/litestream.yml"

echo "artifacts in $WORK"
