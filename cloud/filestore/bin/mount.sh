#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

SERVER_PORT=${SERVER_PORT:-9021}
FS=${FS:-"nfs"}
MOUNT_POINT=${MOUNT_POINT:-"$HOME/$FS"}

[ -d "$MOUNT_POINT" ] || mkdir "$MOUNT_POINT"

$BIN_DIR/filestore-client mount \
    --server-port        $SERVER_PORT \
    --filesystem        "$FS" \
    --mount-path        "$MOUNT_POINT" \
    --verbose            trace \
    2>&1 | grep -v PingSession
