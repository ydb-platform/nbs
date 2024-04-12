#!/usr/bin/env bash

CLUSTER=${CLUSTER:-local}
DOMAIN=${DOMAIN:-Root}
IC_PORT=${IC_PORT:-29012}
GRPC_HOST=${GRPC_HOST:-localhost}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8769}

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

DOMAINS_FILE=${DOMAINS_FILE:-$BIN_DIR/nbs/nbs-domains.txt}
IC_FILE=${IC_FILE:-$BIN_DIR/nbs/nbs-ic.txt}
LOG_FILE=${LOG_FILE:-$BIN_DIR/nbs/nbs-log.txt}
SYS_FILE=${SYS_FILE:-$BIN_DIR/nbs/nbs-sys.txt}
SERVER_FILE=${SERVER_FILE:-$BIN_DIR/nbs/nbs-server.txt}
STORAGE_FILE=${STORAGE_FILE:-$BIN_DIR/nbs/nbs-storage.txt}
NAMING_FILE=${NAMING_FILE:-$BIN_DIR/nbs/nbs-names.txt}
DIAG_FILE=${DIAG_FILE:-$BIN_DIR/nbs/nbs-diag.txt}
AUTH_FILE=${AUTH_FILE:-$BIN_DIR/nbs/nbs-auth.txt}
DR_PROXY_FILE=${DR_PROXY_FILE:-$BIN_DIR/nbs/nbs-dr-proxy.txt}

if [[ -z $LOCATION_FILE ]]; then
    echo "LOCATION_FILE variable is not set"
    exit 1
fi

if [[ -z $DISK_AGENT_FILE ]]; then
    echo "DISK_AGENT_FILE variable is not set"
    exit 1
fi

source ./prepare_binaries.sh || exit 1

diskagentd \
    --domain $DOMAIN \
    --node-broker $GRPC_HOST:$GRPC_PORT \
    --ic-port $IC_PORT \
    --mon-port $MON_PORT \
    --domains-file $DOMAINS_FILE \
    --ic-file $IC_FILE \
    --log-file $LOG_FILE \
    --sys-file $SYS_FILE \
    --server-file $SERVER_FILE \
    --storage-file $STORAGE_FILE \
    --naming-file $NAMING_FILE \
    --diag-file $DIAG_FILE \
    --auth-file $AUTH_FILE \
    --dr-proxy-file $DR_PROXY_FILE \
    --location-file $LOCATION_FILE \
    --disk-agent-file $DISK_AGENT_FILE \
    $@
