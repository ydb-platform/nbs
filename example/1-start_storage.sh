#!/usr/bin/env bash

NODE=${NODE:-1}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8765}
<<<<<<< HEAD
YDBD="./ydbd"
export LD_LIBRARY_PATH=$(dirname $(readlink ydbd))
$YDBD server \
=======

source ./prepare_binaries.sh || exit 1

ydbd server \
>>>>>>> 53ebe7b85 (Updating build documentation)
    --tcp \
    --node              $NODE \
    --grpc-port         $GRPC_PORT \
    --mon-port          $MON_PORT \
    --bootstrap-file    static/boot.txt \
    --bs-file           static/bs.txt \
    --channels-file     static/channels.txt \
    --domains-file      static/domains.txt \
    --log-file          static/log.txt \
    --naming-file       static/names.txt \
    --sys-file          static/sys.txt \
    --ic-file           static/ic.txt \
    --vdisk-file        static/vdisks.txt \
    --suppress-version-check \
