#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
CONFIG_DIR=`readlink -e $BIN_DIR/nfs`
LOG_DIR=`readlink -e $BIN_DIR/data`

IC_PORT=${IC_PORT:-29012}
VHOST_PORT=${VHOST_PORT:-9022}
KIKIMR_PORT=${KIKIMR_PORT:-9001}
MON_PORT=${MON_PORT:-8768}
VERBOSE=${VERBOSE:-info}

# ya tool gdb --args \
$BIN_DIR/filestore-vhost \
    --app-config             $CONFIG_DIR/nfs-vhost.txt \
    --diag-file              $CONFIG_DIR/nfs-diag.txt \
    --mon-port               $MON_PORT \
    --service                kikimr \
    --domain                 Root \
    --scheme-shard-dir       /Root/NFS \
    --node-broker            localhost:$KIKIMR_PORT \
    --ic-port                $IC_PORT \
    --sys-file               $CONFIG_DIR/nfs-sys.txt \
    --log-file               $CONFIG_DIR/nfs-log.txt \
    --domains-file           $CONFIG_DIR/nfs-domains.txt \
    --naming-file            $CONFIG_DIR/nfs-names.txt \
    --ic-file                $CONFIG_DIR/nfs-ic.txt \
    --storage-file           $CONFIG_DIR/nfs-storage.txt \
    --profile-file           $LOG_DIR/filestore-vhost-profile-log.bin \
    --disable-local-service  \
    $@
