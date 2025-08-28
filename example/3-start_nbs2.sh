#!/usr/bin/env bash

IC_PORT=${IC_PORT:-29522}
GRPC_PORT=${GRPC_PORT:-9001}
SERVER_PORT=${SERVER_PORT:-9786}
DATA_SERVER_PORT=${DATA_SERVER_PORT:-9787}
SECURE_SERVER_PORT=${SECURE_SERVER_PORT:-9788}
MON_PORT=${MON_PORT:-8786}
source ./prepare_binaries.sh || exit 1

nbsd \
    --domain             Root \
    --node-broker        localhost:$GRPC_PORT \
    --ic-port            $IC_PORT \
    --mon-port           $MON_PORT \
    --server-port        $SERVER_PORT \
    --data-server-port   $DATA_SERVER_PORT \
    --secure-server-port $SECURE_SERVER_PORT \
    --discovery-file     nbs/nbs-discovery.txt \
    --domains-file       nbs/nbs-domains.txt \
    --ic-file            nbs/nbs-ic.txt \
    --log-file           nbs/nbs-log.txt \
    --sys-file           nbs/nbs-sys.txt \
    --server-file        nbs/nbs-server2.txt \
    --storage-file       nbs/nbs-storage2.txt \
    --naming-file        nbs/nbs-names.txt \
    --diag-file          nbs/nbs-diag.txt \
    --features-file      nbs/nbs-features.txt \
    --auth-file          nbs/nbs-auth.txt \
    --dr-proxy-file      nbs/nbs-dr-proxy.txt \
    --rdma-file          nbs/nbs-rdma.txt \
    --service            kikimr \
    --load-configs-from-cms \
    --profile-file       logs/profile-log2.bin \
    $@ > logs/nbs.2.log 2>&1
