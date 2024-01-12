#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
DATA_DIR=$BIN_DIR/data

CLUSTER=${CLUSTER:-local}
CONFIG_DIR=$BIN_DIR/../clusters/$CLUSTER

set -e
$BIN_DIR/kikimr_configure cfg --static $CONFIG_DIR/cluster.yaml $BIN_DIR/kikimr $BIN_DIR/static
$BIN_DIR/kikimr_configure cfg --dynamic $CONFIG_DIR/cluster.yaml $BIN_DIR/kikimr $BIN_DIR/dynamic
$BIN_DIR/kikimr_configure cfg --nfs $CONFIG_DIR/cluster.yaml $BIN_DIR/kikimr $BIN_DIR/nfs
