#!/usr/bin/env bash

IC_PORT=${IC_PORT:-29012}
MON_PORT=${MON_PORT:-9100}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

blockstore-client ExecuteAction --action DiskRegistrySetWritableState --verbose error --input-bytes '{"State":true}'
if [ $? -ne 0 ]; then
    echo "Can't set writable state for disk registry"
    exit 1
fi

blockstore-client UpdateDiskRegistryConfig --verbose error --input $BIN_DIR/nbs/nbs-disk-registry.txt --proto
if [ $? -ne 0 ]; then
    echo "Disk registry config was not updated. Did you try to update it for the second time? " \
        "Consider updating the Version value or adding IgnoreVersion to $BIN_DIR/nbs/nbs-disk-registry.txt"
fi

source ./prepare_disk-agent.sh || exit 1
start_nbs_agent() {
    if [ -z "$1" ]; then
        echo "Agent number is required"
        exit 1
    fi

    THIS_MON_PORT=$(($MON_PORT + $1))

    IC_PORT=$(($IC_PORT + $1 * 100)) \
    MON_PORT=$THIS_MON_PORT \
    start_disk-agent \
        --location-file $BIN_DIR/nbs/nbs-location-$1.txt \
        --disk-agent-file $BIN_DIR/nbs/nbs-disk-agent-$1.txt >logs/remote-da$1.log 2>&1 &
}

start_nbs_agent 0
pid0=$!
echo "Agent 0 started with pid $pid0 http://localhost:$THIS_MON_PORT/blockstore/disk_agent"

start_nbs_agent 1
pid1=$!
echo "Agent 1 started with pid $pid1 http://localhost:$THIS_MON_PORT/blockstore/disk_agent"

start_nbs_agent 2
pid2=$!
echo "Agent 2 started with pid $pid2 http://localhost:$THIS_MON_PORT/blockstore/disk_agent"

start_nbs_agent 3
pid3=$!
echo "Agent 3 started with pid $pid3 http://localhost:$THIS_MON_PORT/blockstore/disk_agent"

start_nbs_agent 4
pid4=$!
echo "Agent 4 started with pid $pid4 http://localhost:$THIS_MON_PORT/blockstore/disk_agent"

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    kill $pid0
    kill $pid1
    kill $pid2
    kill $pid3
    kill $pid4
    exit 0
}

while true; do
    sleep 5
    echo -n "."
done
