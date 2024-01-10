export NBS_ROOT=".."
YDBD_BIN="$NBS_ROOT/contrib/ydb/apps/ydbd/ydbd"
NBSD_BIN="$NBS_ROOT/cloud/blockstore/apps/server/nbsd"
BLOCKSTORE_CLIENT_BIN="$NBS_ROOT/cloud/blockstore/apps/client/blockstore-client"
DISK_AGENT_BIN="$NBS_ROOT/cloud/blockstore/apps/disk_agent/diskagentd"
BLOCKSTORE_NBD_BIN="$NBS_ROOT/cloud/blockstore/tools/nbd/blockstore-nbd"

for bin in $YDBD_BIN $NBSD_BIN $BLOCKSTORE_CLIENT_BIN $DISK_AGENT_BIN $BLOCKSTORE_NBD_BIN
do
  if ! test -f $bin; then
    echo "$bin not found, build all targets first"
    return 1
  fi
done

function ydbd () {
  LD_LIBRARY_PATH=$(dirname $YDBD_BIN) $YDBD_BIN $@
}
function nbsd () {
  LD_LIBRARY_PATH=$(dirname $NBSD_BIN) $NBSD_BIN $@
}
function blockstore-client () {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_CLIENT_BIN) $BLOCKSTORE_CLIENT_BIN $@
}
function diskagentd () {
  LD_LIBRARY_PATH=$(dirname $DISK_AGENT_BIN) $DISK_AGENT_BIN $@
}
function blockstore-nbd () {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_NBD_BIN) $BLOCKSTORE_NBD_BIN $@
}
function sudo-blockstore-nbd () {
  LD_LIBRARY_PATH=$(dirname $BLOCKSTORE_NBD_BIN)
  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH $BLOCKSTORE_NBD_BIN $@
}
