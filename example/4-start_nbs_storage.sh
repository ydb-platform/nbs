#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}
IC_PORT=${IC_PORT:-29012}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8769}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
<<<<<<< HEAD
CLIENT="blockstore-client"
DAGENT="diskagentd"
export LD_LIBRARY_PATH=$(dirname $(readlink $BIN_DIR/diskagentd))
=======
source ./prepare_binaries.sh || exit 1
>>>>>>> 53ebe7b85 (Updating build documentation)

blockstore-client ExecuteAction --action DiskRegistrySetWritableState --verbose error --input-bytes '{"State":true}'
if [ $? -ne 0 ]; then
    echo "Can't set writable state for disk registry"
    exit 1
fi

blockstore-client UpdateDiskRegistryConfig --verbose error --input $BIN_DIR/nbs/nbs-disk-registry.txt --proto
if [ $? -ne 0 ]; then
    echo "Disk registry config is not updated. Do you apply in second time? " \
      "Consider update version value or add IgnoreVersion to $BIN_DIR/nbs/nbs-disk-registry.txt"
    exit 1
fi

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
    --disk-agent-file    $BIN_DIR/nbs/nbs-disk-agent-$1.txt >logs/remote-da$1.log 2>&1 &
}

start_nbs_agent 1
pid1=$!
echo "Agent 1 started with pid $pid1"

start_nbs_agent 2
pid2=$!
echo "Agent 2 started with pid $pid2"

start_nbs_agent 3
pid3=$!
echo "Agent 3 started with pid $pid3"

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    kill $pid1
    kill $pid2
    kill $pid3
    exit 0
}


while true; do
    sleep 5
    echo -n "."
done
