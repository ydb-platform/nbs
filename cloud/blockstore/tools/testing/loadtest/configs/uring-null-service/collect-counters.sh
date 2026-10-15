#!/usr/bin/env bash
# Usage: collect-counters.sh <host> <mon-port> <component> [out-file]
#   component: uring_server (nbsd, mon-port 8766) | uring_client (loadtest, 8767)
# Dumps the blockstore counter subtree for the component as JSON.
set -euo pipefail
HOST=${1:?host}; PORT=${2:?mon port}; COMP=${3:?component}; OUT=${4:-/dev/stdout}
curl -s "http://$HOST:$PORT/counters/counters=blockstore/component=$COMP/json" > "$OUT"
