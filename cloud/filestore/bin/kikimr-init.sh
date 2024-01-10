#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
DATA_DIR=$BIN_DIR/data

set -e
bash $BIN_DIR/dynamic/init_storage.bash
bash $BIN_DIR/dynamic/init_compute.bash
bash $BIN_DIR/dynamic/init_root_storage.bash
bash $BIN_DIR/dynamic/init_databases.bash
bash $BIN_DIR/dynamic/init_cms.bash
