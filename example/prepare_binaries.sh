BUILD_ROOT="../cloud/blockstore/buildall"
YDBD_BIN="$BUILD_ROOT/contrib/ydb/apps/ydbd/ydbd"
YDB_BIN="$BUILD_ROOT/contrib/ydb/apps/ydb/ydb"
NBSD_BIN="$BUILD_ROOT/cloud/blockstore/apps/server/nbsd"
BLOCKSTORE_CLIENT_BIN="$BUILD_ROOT/cloud/blockstore/apps/client/blockstore-client"
BLOCKSTORE_VHOST_SERVER_BIN="$BUILD_ROOT/cloud/blockstore/vhost-server/blockstore-vhost-server"
DISK_AGENT_BIN="$BUILD_ROOT/cloud/blockstore/apps/disk_agent/diskagentd"
BLOCKSTORE_NBD_BIN="$BUILD_ROOT/cloud/blockstore/tools/nbd/blockstore-nbd"
QEMU_TAR="$BUILD_ROOT/cloud/storage/core/tools/testing/qemu/bin/qemu-bin.tar.gz"

for bin in $YDBD_BIN $NBSD_BIN $BLOCKSTORE_CLIENT_BIN $BLOCKSTORE_VHOST_SERVER_BIN $DISK_AGENT_BIN $BLOCKSTORE_NBD_BIN $QEMU_TAR
do
  if ! test -f $bin; then
    echo "$bin not found, build all targets first"
    return 1
  fi
done

function ydb {
  LD_LIBRARY_PATH=$(dirname $YDB_BIN) $YDB_BIN "$@"
}

function ydbd {
  LD_LIBRARY_PATH=$(dirname $YDBD_BIN) $YDBD_BIN "$@"
}

function nbsd {
  LD_LIBRARY_PATH=$(dirname $NBSD_BIN) $NBSD_BIN "$@"
}

function blockstore-client {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_CLIENT_BIN) $BLOCKSTORE_CLIENT_BIN "$@"
}

function blockstore-vhost-server {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_VHOST_SERVER_BIN) $BLOCKSTORE_VHOST_SERVER_BIN "$@"
}

function diskagentd {
  LD_LIBRARY_PATH=$(dirname $DISK_AGENT_BIN) $DISK_AGENT_BIN "$@"
}

function blockstore-nbd {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_NBD_BIN) $BLOCKSTORE_NBD_BIN "$@"
}

function sudo-blockstore-nbd {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_NBD_BIN)
  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH $BLOCKSTORE_NBD_BIN "$@"
}

function qemu_bin_dir {
  echo $(dirname $QEMU_TAR)
}
