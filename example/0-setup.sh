#!/usr/bin/env bash

DATA_DIRS=" \
    certs \
    data \
    logs \
    "
find_bin_dir() {
    readlink -e `dirname $0`
}

find_blockstore_all_dir() {
    readlink -e `dirname $0`/../cloud/blockstore/buildall
}

BIN_DIR=`find_bin_dir`
BLOCKSTORE_ALL_DIR=`find_blockstore_all_dir`
PERSISTENT_TMP_DIR=${PERSISTENT_TMP_DIR:-$HOME/tmp/nbs}

BUILD_FILES=" \
    contrib/ydb/apps/ydbd/ydbd \
    cloud/blockstore/apps/server/nbsd \
    cloud/blockstore/apps/disk_agent/diskagentd \
    cloud/blockstore/apps/client/blockstore-client \
    cloud/blockstore/tools/nbd/blockstore-nbd \
    "

# create symlinks
for file in $BUILD_FILES; do
    ln -svf $BLOCKSTORE_ALL_DIR/$file $BIN_DIR/
done

for dir in $DATA_DIRS; do
    mkdir -p "$PERSISTENT_TMP_DIR/$dir"
    ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$BIN_DIR/$dir"
done

# check symlinks
for bin in ydbd nbsd blockstore-nbd blockstore-client diskagentd
do
  if ! test -f "$BIN_DIR/$bin"; then
    echo "$bin not found"
    exit 1
  fi
done

generate_cert() {
    local pass="pass123"
    local name="$1"
    local fallback_name="$name-fallback"
    local dir="$2"
    openssl genrsa -passout pass:$pass -des3 -out $dir/$name.key 4096
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$name.csr -subj "/C=RU/L=MyCity/O=MyCloud/OU=Infrastructure/CN=$(hostname --fqdn)"
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$fallback_name.csr -subj "/C=RU/L=MyCity/O=MyCloud/OU=Infrastructure/CN=localhost"
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$name.crt
    openssl x509 -req -passin pass:$pass -days 365 -in $dir/$fallback_name.csr -signkey $dir/$name.key -set_serial 01 -out $dir/$fallback_name.crt
    openssl rsa -passin pass:$pass -in $dir/$name.key -out $dir/$name.key
    rm $dir/$name.csr $dir/$fallback_name.csr
}

generate_cert "server" "certs"

format_disk() {
    DISK_GUID=$1
    DISK_SIZE=$2

    REAL_DISK_FILE=/var/tmp/pdisk-${DISK_GUID}.data
    DISK_FILE=data/pdisk-${DISK_GUID}.data

    rm -f $DISK_FILE
    rm -f $REAL_DISK_FILE
    dd if=/dev/zero of=$REAL_DISK_FILE bs=1 count=0 seek=$DISK_SIZE
    ln -s $REAL_DISK_FILE $DISK_FILE
}

setup_nonrepl_disk_agent() {
    if [ -z "$1" ]; then echo "Agent number is required"; exit 1; fi

    truncate -s  1G "$BIN_DIR/data/remote$1-1024-1.bin"
    truncate -s  1G "$BIN_DIR/data/remote$1-1024-2.bin"
    truncate -s  1G "$BIN_DIR/data/remote$1-1024-3.bin"

    cat > $BIN_DIR/nbs/nbs-disk-agent-$1.txt <<EOF
AgentId: "remote$1.cloud-example.net"
Enabled: true
DedicatedDiskAgent: true
Backend: DISK_AGENT_BACKEND_AIO

FileDevices: {
    Path: "$BIN_DIR/data/remote$1-1024-1.bin"
    DeviceId: "remote$1-1024-1"
    BlockSize: 4096
}

FileDevices: {
    Path: "$BIN_DIR/data/remote$1-1024-2.bin"
    DeviceId: "remote$1-1024-2"
    BlockSize: 4096
}

FileDevices: {
    Path: "$BIN_DIR/data/remote$1-1024-3.bin"
    DeviceId: "remote$1-1024-3"
    BlockSize: 4096
}

EOF

cat > $BIN_DIR/nbs/nbs-location-$1.txt <<EOF
DataCenter: "local-dc"
Rack: "rack-$1"
EOF

}


setup_nonrepl_disk_registry() {

    setup_nonrepl_disk_agent 1
    setup_nonrepl_disk_agent 2
    setup_nonrepl_disk_agent 3

    cat > "$BIN_DIR"/nbs/nbs-disk-registry.txt <<EOF
KnownAgents {
    AgentId: "remote1.cloud-example.net"
    Devices: "remote1-1024-1"
    Devices: "remote1-1024-2"
    Devices: "remote1-1024-3"
}

KnownAgents {
    AgentId: "remote2.cloud-example.net"
    Devices: "remote2-1024-1"
    Devices: "remote2-1024-2"
    Devices: "remote2-1024-3"
}

KnownAgents {
    AgentId: "remote3.cloud-example.net"
    Devices: "remote3-1024-1"
    Devices: "remote3-1024-2"
    Devices: "remote3-1024-3"
}

EOF
}

format_disk ssd 64G
format_disk rot 64G

setup_nonrepl_disk_registry

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
test -f static && ln -s "${SCRIPT_DIR}/static" static
test -f dynamic && ln -s "${SCRIPT_DIR}/dynamic" dynamic
test -f nbs && ln -s "${SCRIPT_DIR}/nbs" nbs
