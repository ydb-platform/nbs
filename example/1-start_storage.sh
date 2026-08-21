#!/usr/bin/env bash

NODE=${NODE:-1}
GRPC_PORT=${GRPC_PORT:-9001}
MON_PORT=${MON_PORT:-8765}

source ./prepare_binaries.sh || exit 1

ydbd server \
    --tcp \
    --node              $NODE \
    --grpc-port         $GRPC_PORT \
    --mon-port          $MON_PORT \
    --yaml-config       static/config.yaml \
    --suppress-version-check
