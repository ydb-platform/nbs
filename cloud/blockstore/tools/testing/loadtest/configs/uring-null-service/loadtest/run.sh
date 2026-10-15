#!/usr/bin/env bash
# Usage: run.sh <loadtest-binary> <nbs-host> <ipc> <workload> [client-variant]
#   ipc:            grpc | uring
#   workload:       read-256k | write-256k | mixed-256k | read-4k | read-4k-256k
#   client-variant: baseline | zc-off | oob-off | inline-1m | staging  (uring only)
# Control plane always goes over gRPC :9769; with ipc=uring the data plane
# goes over uring :9770 (UringHost/UringPort in the client config).
set -euo pipefail
BIN=${1:?loadtest binary}
NBS_HOST=${2:?nbs host}
IPC=${3:?grpc|uring}
WORKLOAD=${4:?workload}
VARIANT=${5:-baseline}
DIR=$(cd "$(dirname "$0")" && pwd)
OUT="$DIR/../results"
MON_PORT=${MON_PORT:-8767}
mkdir -p "$OUT"
if [[ "$IPC" == grpc ]]; then
    CLIENT_SRC="$DIR/client-grpc.txt"; TAG="grpc-$WORKLOAD"
else
    CLIENT_SRC="$DIR/client-uring-$VARIANT.txt"; TAG="uring-$VARIANT-$WORKLOAD"
fi
CLIENT="$OUT/client-$TAG.txt"
sed "s/@NBS_HOST@/$NBS_HOST/g" "$CLIENT_SRC" > "$CLIENT"
echo "+ $BIN --host $NBS_HOST --port 9769 --config workloads/$WORKLOAD-$IPC.txt --client-config $CLIENT"
"$BIN" \
    --host "$NBS_HOST" --port 9769 \
    --config "$DIR/workloads/$WORKLOAD-$IPC.txt" \
    --client-config "$CLIENT" \
    --mon-port "$MON_PORT" \
    --results "$OUT/$TAG.results.txt" \
    --verbose info 2>&1 | tee "$OUT/$TAG.log"
echo "results: $OUT/$TAG.results.txt"
