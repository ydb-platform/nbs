#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}
IC_PORT=${IC_PORT:-29012}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8769}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

diskagentd \
    --domain             Root \
    --node-broker        localhost:$GRPC_PORT \
    --ic-port            $(( $IC_PORT + $1 * 100 )) \
    --mon-port           $(( $MON_PORT + $1 * 100 )) \
    --domains-file       $BIN_DIR/nbs/nbs-domains.txt \
    --ic-file            $BIN_DIR/nbs/nbs-ic.txt \
    --log-file           $BIN_DIR/nbs/nbs-log.txt \
    --sys-file           $BIN_DIR/nbs/nbs-sys.txt \
    --server-file        $BIN_DIR/nbs/nbs-server.txt \
    --storage-file       $BIN_DIR/nbs/nbs-storage.txt \
    --naming-file        $BIN_DIR/nbs/nbs-names.txt \
    --diag-file          $BIN_DIR/nbs/nbs-diag.txt \
    --auth-file          $BIN_DIR/nbs/nbs-auth.txt \
    --dr-proxy-file      $BIN_DIR/nbs/nbs-dr-proxy.txt \
    --load-configs-from-cms \
    $@
