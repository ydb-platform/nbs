#!/usr/bin/env bash

EXAMPLE_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
REPO_ROOT=$(cd -- "$EXAMPLE_DIR/.." && pwd)
# Override BUILD_ROOT with the repository root for directly built targets.
BUILD_ROOT=${BUILD_ROOT:-$REPO_ROOT/cloud/blockstore/buildall}
BUILD_ROOT=$(realpath -m -- "$BUILD_ROOT")

YDBD_BIN="$BUILD_ROOT/contrib/ydb/apps/ydbd/ydbd"
YDB_BIN="$BUILD_ROOT/contrib/ydb/apps/ydb/ydb"
NBSD_BIN="$BUILD_ROOT/cloud/blockstore/apps/server/nbsd"
BLOCKSTORE_CLIENT_BIN="$BUILD_ROOT/cloud/blockstore/apps/client/blockstore-client"
BLOCKSTORE_VHOST_SERVER_BIN="$BUILD_ROOT/cloud/blockstore/vhost-server/blockstore-vhost-server"
DISK_AGENT_BIN="$BUILD_ROOT/cloud/blockstore/apps/disk_agent/diskagentd"
BLOCKSTORE_NBD_BIN="$BUILD_ROOT/cloud/blockstore/tools/nbd/blockstore-nbd"
QEMU_TAR="$BUILD_ROOT/cloud/storage/core/tools/testing/qemu/bin/qemu-bin.tar.gz"

# Only require the binaries used by the selected script.
require_bins() {
  local bin
  for bin in "$@"; do
    if [[ ! -x "$bin" ]]; then
      echo "$bin not found or not executable; build cloud/blockstore/buildall first" >&2
      return 1
    fi
  done
}

run_in_cell2() {
  if [[ ${NBS_CELL:-} != cell-2 ]]; then
    exec "$EXAMPLE_DIR/run-cell2.sh" "$(basename -- "$0")" "$@"
  fi
}

function ydb {
  require_bins "$YDB_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$YDB_BIN")" "$YDB_BIN" "$@"
}

function ydbd {
  require_bins "$YDBD_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$YDBD_BIN")" "$YDBD_BIN" "$@"
}

function nbsd {
  require_bins "$NBSD_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$NBSD_BIN")" "$NBSD_BIN" "$@"
}

function blockstore-client {
  require_bins "$BLOCKSTORE_CLIENT_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$BLOCKSTORE_CLIENT_BIN")" "$BLOCKSTORE_CLIENT_BIN" "$@" ${NBS_CLIENT_PORT:+--port="$NBS_CLIENT_PORT"}
}

function blockstore-vhost-server {
  require_bins "$BLOCKSTORE_VHOST_SERVER_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$BLOCKSTORE_VHOST_SERVER_BIN")" "$BLOCKSTORE_VHOST_SERVER_BIN" "$@"
}

function diskagentd {
  require_bins "$DISK_AGENT_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$DISK_AGENT_BIN")" "$DISK_AGENT_BIN" "$@"
}

function blockstore-nbd {
  require_bins "$BLOCKSTORE_NBD_BIN" || return 1
  LD_LIBRARY_PATH="$(dirname "$BLOCKSTORE_NBD_BIN")" "$BLOCKSTORE_NBD_BIN" "$@" ${NBS_CLIENT_PORT:+--port="$NBS_CLIENT_PORT"}
}

function sudo-blockstore-nbd {
  require_bins "$BLOCKSTORE_NBD_BIN" || return 1
  sudo LD_LIBRARY_PATH="$(dirname "$BLOCKSTORE_NBD_BIN")" "$BLOCKSTORE_NBD_BIN" "$@" ${NBS_CLIENT_PORT:+--port="$NBS_CLIENT_PORT"}
}

function qemu_bin_dir {
  dirname -- "$QEMU_TAR"
}
