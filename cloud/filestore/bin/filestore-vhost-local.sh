#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
CONFIG_DIR=`readlink -e $BIN_DIR/nfs`

IC_PORT=${IC_PORT:-29012}
VHOST_PORT=${VHOST_PORT:-9022}
KIKIMR_PORT=${KIKIMR_PORT:-9001}
MON_PORT=${MON_PORT:-8768}
VERBOSE=${VERBOSE:-info}

# ya tool gdb --args \
$BIN_DIR/filestore-vhost \
    --app-config        $CONFIG_DIR/nfs-vhost.txt \
    --diag-file         $CONFIG_DIR/nfs-diag.txt \
    --mon-port          $MON_PORT \
    --verbose           $VERBOSE \
    --service           local \
    $@
