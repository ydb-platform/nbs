#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

VIRTIOFSD=${VIRTIOFSD:-$BIN_DIR/virtiofsd}
SOCKET_PATH=${SOCKET_PATH:-/tmp/vhost.sock}

# ya tool gdb --args \
$VIRTIOFSD \
    --socket-path=$SOCKET_PATH \
    -o source=/home/vskipin \
    -o cache=always \
    -o debug \
    $@
