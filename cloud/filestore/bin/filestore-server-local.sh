#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
CONFIG_DIR=`readlink -e $BIN_DIR/nfs`

SERVER_PORT=${SERVER_PORT:-9021}
MON_PORT=${MON_PORT:-8767}

# ya tool gdb --args \
$BIN_DIR/filestore-server \
    --mon-port           $MON_PORT \
    --server-port        $SERVER_PORT \
    --app-config         $CONFIG_DIR/nfs-server.txt \
    --service            local \
    $@
