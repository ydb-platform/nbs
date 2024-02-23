#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}
IC_PORT=${IC_PORT:-29012}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8769}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

if [ -z "$1" ]; then echo "Agent number is required"; exit 1; fi

start_nbs_agent() {
  if [ -z "$1" ]; then echo "Agent number is required"; exit 1; fi

  diskagentd \
    --domain             Root \
    --node-broker        localhost:$GRPC_PORT \
    --ic-port            $(( $IC_PORT + $1 * 100 )) \
    --mon-port           $(( $MON_PORT + $1 * 100 )) \
    --domains-file       $BIN_DIR/nbs/nbs-domains.txt \
    --ic-file            $BIN_DIR/nbs/nbs-ic.txt \
    --log-file           $BIN_DIR/nbs/nbs-log.txt \
    --sys-file           $BIN_DIR/nbs/nbs-sys.txt \
    --server-file        $BIN_DIR/nbs/nbs-server.txt \
    --storage-file       $BIN_DIR/nbs/nbs-storage.txt \
    --naming-file        $BIN_DIR/nbs/nbs-names.txt \
    --diag-file          $BIN_DIR/nbs/nbs-diag.txt \
    --auth-file          $BIN_DIR/nbs/nbs-auth.txt \
    --dr-proxy-file      $BIN_DIR/nbs/nbs-dr-proxy.txt \
    --load-configs-from-cms \
    --location-file      $BIN_DIR/nbs/nbs-location-$1.txt \
    --disk-agent-file    $BIN_DIR/nbs/nbs-disk-agent-$1.txt 2>&1 &
}

start_nbs_agent $1
pid=$!
echo "Agent 1 started with pid $pid"

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    kill $pid
    exit 0
}

while true; do
    sleep 5
    echo -n "."
done
