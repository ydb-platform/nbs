#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

blockstore-client "$@"
