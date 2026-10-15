#!/usr/bin/env bash
# Usage: start-nbsd.sh <nbsd-binary> <variant>
#   variant: grpc | baseline | zc-off | oob-off | inline-1m | staging
# grpc      -> no --uring-file (gRPC-only baseline on :9769)
# others    -> --uring-file nbsd/uring-<variant>.txt (uring target on :9770)
set -euo pipefail
BIN=${1:?nbsd binary}
VARIANT=${2:?variant}
DIR=$(cd "$(dirname "$0")" && pwd)
MON_PORT=${MON_PORT:-8766}
ARGS=(
    --service null
    --server-file "$DIR/server.txt"
    --mon-port "$MON_PORT"
    --profile-file "$DIR/profile-$VARIANT.log"
)
if [[ "$VARIANT" != grpc ]]; then
    ARGS+=(--uring-file "$DIR/uring-$VARIANT.txt")
fi
echo "+ $BIN ${ARGS[*]}"
exec "$BIN" "${ARGS[@]}"
