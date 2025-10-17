#!/usr/bin/env bash

DATA_DIRS=" \
    certs \
    data \
    logs \
    "
find_bin_dir() {
    readlink -e `dirname $0`
}

LOCALHOST=$(hostname --fqdn)
BIN_DIR=`find_bin_dir`
PERSISTENT_TMP_DIR=${PERSISTENT_TMP_DIR:-$HOME/tmp/nbs}
source ./prepare_binaries.sh || exit 1

for dir in $DATA_DIRS; do
    mkdir -p "$PERSISTENT_TMP_DIR/$dir"
    ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$BIN_DIR/$dir"
done

generate_cert() {
    local pass="pass123"
    local name="$1"
    local fallback_name="$name-fallback"
    local dir="$2"
    openssl genrsa -passout pass:$pass -des3 -out $dir/$name.key 4096
    openssl req -passin pass:$pass -new -key $dir/$name.key -out $dir/$name.csr -subj "/C=RU/L=MyCity/O=MyCloud/OU=Infrastructure/CN=$LOCALHOST"
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

    DISK_FILE=data/pdisk-${DISK_GUID}.data

    rm -f $DISK_FILE
    dd if=/dev/zero of=$DISK_FILE bs=1 count=0 seek=$DISK_SIZE
}

setup_remote_disk_agent() {
    if [ -z "$1" ]; then echo "Agent number is required"; exit 1; fi

    truncate -s  1G "$BIN_DIR/data/remote$1-1024-1.bin"
    truncate -s  1G "$BIN_DIR/data/remote$1-1024-2.bin"
    truncate -s  1G "$BIN_DIR/data/remote$1-1024-3.bin"

    cat > $BIN_DIR/nbs/nbs-disk-agent-$1.txt <<EOF
AgentId: "remote$1.cloud-example.net"
Enabled: true
DedicatedDiskAgent: true
Backend: DISK_AGENT_BACKEND_AIO
AcquireRequired: true

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

ThrottlingConfig: {
    InfraThrottlingConfigPath: "nbs/nbs-throttling.json"
    DefaultNetworkMbitThroughput: 100
    DirectCopyBandwidthFraction: 0.5
    MaxDeviceBandwidthMiB: 15
}

ChaosConfig: {
    ChaosProbability: 0.01
    ChaosErrorCodes: 0x80000002 # E_REJECTED
    ChaosErrorCodes: 0x80000005 # E_TIMEOUT
    ChaosErrorCodes: 0x80000007 # E_UNAUTHORIZED
    ChaosDataDamageProbability: 0.5
}

EOF

cat > $BIN_DIR/nbs/nbs-location-$1.txt <<EOF
DataCenter: "local-dc"
Rack: "rack-$1"
EOF

} # setup_remote_disk_agent

setup_local_disk_agent() {
    truncate -s  1G "$BIN_DIR/data/local-1024-1.bin"
    truncate -s  1G "$BIN_DIR/data/local-1024-2.bin"
    truncate -s  1G "$BIN_DIR/data/local-1024-3.bin"

    cat > $BIN_DIR/nbs/nbs-disk-agent-0.txt <<EOF
AgentId: "$LOCALHOST"
Enabled: true
DedicatedDiskAgent: true
Backend: DISK_AGENT_BACKEND_AIO
AcquireRequired: true

FileDevices: {
    Path: "$BIN_DIR/data/local-1024-1.bin"
    DeviceId: "local-1024-1"
    BlockSize: 512
    PoolName: "local-ssd"
}

FileDevices: {
    Path: "$BIN_DIR/data/local-1024-2.bin"
    DeviceId: "local-1024-2"
    BlockSize: 512
    PoolName: "local-ssd"
}

FileDevices: {
    Path: "$BIN_DIR/data/local-1024-3.bin"
    DeviceId: "local-1024-3"
    BlockSize: 512
    PoolName: "local-ssd"
}

ThrottlingConfig: {
    InfraThrottlingConfigPath: "nbs/nbs-throttling.json"
    DefaultNetworkMbitThroughput: 100
    DirectCopyBandwidthFraction: 0.5
}

EOF

cat > $BIN_DIR/nbs/nbs-location-0.txt <<EOF
DataCenter: "local-dc"
Rack: "rack-local"
EOF

} # setup_local_disk_agent


setup_nonrepl_disk_registry() {

    setup_local_disk_agent
    setup_remote_disk_agent 1
    setup_remote_disk_agent 2
    setup_remote_disk_agent 3
    setup_remote_disk_agent 4

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

KnownAgents {
    AgentId: "remote4.cloud-example.net"
    Devices: "remote4-1024-1"
    Devices: "remote4-1024-2"
    Devices: "remote4-1024-3"
}

KnownAgents {
    AgentId: "$LOCALHOST"
    Devices: "local-1024-1"
    Devices: "local-1024-2"
    Devices: "local-1024-3"
}

KnownDevicePools: <
  Name: "local-ssd"
  AllocationUnit: 1073741824
  Kind: DEVICE_POOL_KIND_LOCAL
>

EOF
}

format_disk ssd-1 64G
format_disk ssd-2 64G
format_disk rot 32G

setup_nonrepl_disk_registry

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
test -d static || ln -s "${SCRIPT_DIR}/static" static
test -d dynamic || ln -s "${SCRIPT_DIR}/dynamic" dynamic
test -d nbs || ln -s "${SCRIPT_DIR}/nbs" nbs
