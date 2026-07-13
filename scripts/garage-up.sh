#!/usr/bin/env bash
#
# Brings up a single-node Garage (S3-compatible) object store and bootstraps a
# bucket + access key for the literstream integration tests. Writes credentials
# to docker/garage/.garage.env; source it before running the S3 test:
#
#   ./scripts/garage-up.sh
#   source docker/garage/.garage.env
#   cargo test --test s3_garage -- --ignored --nocapture
#
# Requires: docker + docker-compose.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DIR="$HERE/../docker/garage"
cd "$DIR"

docker-compose up -d

G() { docker-compose exec -T garage /garage -c /etc/garage.toml "$@"; }

echo "waiting for garage..."
for _ in $(seq 1 60); do
  if G status >/dev/null 2>&1; then break; fi
  sleep 1
done

NODE_ID="$(G node id -q | cut -d@ -f1)"
echo "node id: $NODE_ID"

# Assign the node to the cluster layout (once).
if ! G layout show 2>/dev/null | grep -q "$NODE_ID"; then
  G layout assign -z dc1 -c 1G "$NODE_ID"
  G layout apply --version 1
fi

# Bucket + key (idempotent).
G bucket create literstream >/dev/null 2>&1 || true
if ! G key list 2>/dev/null | grep -q literstream-key; then
  G key create literstream-key >/dev/null
fi
G bucket allow literstream --read --write --owner --key literstream-key >/dev/null 2>&1 || true

KEYINFO="$(G key info literstream-key --show-secret)"
ACCESS="$(echo "$KEYINFO" | grep -oiE 'GK[0-9a-f]{24,}' | head -1)"
SECRET="$(echo "$KEYINFO" | grep -oiE '[0-9a-f]{64}' | head -1)"

if [ -z "$ACCESS" ] || [ -z "$SECRET" ]; then
  echo "failed to read Garage credentials; key info was:" >&2
  echo "$KEYINFO" >&2
  exit 1
fi

ENVFILE="$DIR/.garage.env"
cat >"$ENVFILE" <<EOF
export LITESTREAM_S3_ENDPOINT=http://127.0.0.1:3900
export LITESTREAM_S3_REGION=garage
export LITESTREAM_S3_BUCKET=literstream
export LITESTREAM_S3_ACCESS_KEY=$ACCESS
export LITESTREAM_S3_SECRET=$SECRET
EOF

echo "wrote $ENVFILE"
echo
echo "next:"
echo "  source docker/garage/.garage.env"
echo "  cargo test --test s3_garage -- --ignored --nocapture"
