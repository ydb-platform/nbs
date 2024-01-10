#!/usr/bin/env bash

set +e

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
STORAGE_DIR=$BIN_DIR/../../storage
QEMU_DIR=${QEMU_DIR:-$STORAGE_DIR/core/tools/testing/qemu}

: ${DISK_IMAGE:=$QEMU_DIR/win-image/win.img}

if [ ! -e $DISK_IMAGE ]; then
    echo "no win image found; please run 'ya make $QEMU_DIR/win-image'"
    exit 1
fi

export DISK_IMAGE
exec ./run_test_qemu.sh
