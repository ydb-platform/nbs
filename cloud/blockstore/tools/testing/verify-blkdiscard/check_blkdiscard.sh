#!/bin/bash

logrun() {
    echo "`date +%d_%m_%Y-%H_%M_%S`: $*"
    "$@"
}

overwrite_disk() {
    echo "overwriting disk $DISK"
    logrun time fio --name=overwrite --filename=$DISK --ioengine=libaio --direct=1 --bs=1M --rw=write --iodepth=32 --numjobs=1 --buffered=0 --size=$DISK_SIZE --loops=1 --randrepeat=0 --norandommap --refill_buffers
}

verify_disk() {
    echo "verifying $DISK is zeroed"
    logrun time fio --name=test --filename=$DISK --ioengine=libaio --direct=1 --bs=1M --rw=read --iodepth=32 --numjobs=1 --buffered=0 --size=$DISK_SIZE --loops=1 --verify=pattern --verify_pattern=0
}

discard_disk() {
    echo "discarding $DISK"
    offset=0
    while (( offset < $DISK_SIZE)); do
        size=$DISCARD_SIZE
        if ((offset + size > DISK_SIZE)); then
            size=$((DISK_SIZE - offset))
        fi

        logrun blkdiscard --offset $offset --length $size $DISK
        offset=$((offset + size))
    done
}

for util in blockdev fio blkdiscard; do
    if ! which $util > /dev/null; then
        echo "$util util missing"
        exit 1
    fi
done


DISK=${2-/dev/disk/by-partlabel/NVMENBS02}
DISCARD_SIZE=$((93 << 30))
DISK_SIZE=${3-`blockdev --getsize64 $DISK`}
ITERATIONS=${1-100}

echo "
DISK=$DISK
DISCARD_SIZE=$DISCARD_SIZE
DISK_SIZE=$DISK_SIZE
ITERATIONS=$ITERATIONS
"

read -p "WARNING $DISK will be erased. Write $DISK to approve: " response
if [ "$response" != "$DISK" ]; then
    echo "Execution aborted"
    exit 1
fi

set -e
for i in `seq $ITERATIONS`; do
    echo "ITERATION #$i:"
    overwrite_disk

    set +e
    if verify_disk; then
        echo "unexpectedly disk is zeroed after overwrite"
        exit 1
    fi
    set -e

    discard_disk
    verify_disk
done

echo SUCCESS
