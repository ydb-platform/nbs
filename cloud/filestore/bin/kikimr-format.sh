#!/usr/bin/env bash

DISK_KEY=2748 # 0xABC

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
DATA_DIR=$BIN_DIR/data

format_disk() {
    DISK_GUID=$1
    DISK_SIZE=$2

    DISK_FILE=$DATA_DIR/pdisk-${DISK_GUID}.data

    rm -f $DISK_FILE
    dd if=/dev/zero of=$DISK_FILE bs=1 count=0 seek=$DISK_SIZE
}

format_disk ssd 256G
format_disk rot 64G
