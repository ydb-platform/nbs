#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}

find_bin_dir() {
    readlink -e `dirname $0`
}

abspath() {
    path=$1

    if [ ! -d $path ]; then
        echo $path
        return
    fi

    (cd $1; pwd)
}

BIN_DIR=`find_bin_dir`
DATA_DIR=`readlink -e $BIN_DIR/data`
CONFIG_DIR=`readlink -e $BIN_DIR/nfs`
CLUSTER_DIR=`abspath $BIN_DIR/../clusters/$CLUSTER`

MON_PORT=${MON_PORT:-8769}
NFS_PORT=${NFS_PORT:-2049}

# prepare ganesha config
cp $CLUSTER_DIR/ganesha.conf $CONFIG_DIR/ganesha.conf
sed -i $CONFIG_DIR/ganesha.conf \
    -e "s/{ganesha_nfs_port}/$NFS_PORT/" \
    -e "s#{config_path}#$CLUSTER_DIR/client.txt#" \
    -e "s/{filesystem}/test/" \
    -e "s/{client}/test/"

FILESTORE_NFS=`readlink -e $BIN_DIR/filestore-nfs`

# ya tool gdb --args \
$FILESTORE_NFS \
    --config-file       $CONFIG_DIR/ganesha.conf \
    --pid-file          $DATA_DIR/ganesha.pid \
    --recovery-dir      $DATA_DIR/recovery \
    --mon-port          $MON_PORT \
    $@
