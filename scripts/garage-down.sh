#!/usr/bin/env bash
# Tears down the local Garage stack and removes its data volumes.
set -euo pipefail
cd "$(dirname "$0")/../docker/garage"
docker-compose down -v
rm -f .garage.env
