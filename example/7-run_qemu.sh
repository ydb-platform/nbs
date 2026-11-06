#!/usr/bin/env bash

source ./prepare_binaries.sh || exit 1
original_args=("$@")

show_help() {
    cat << EOF
Usage: ./7-run_qemu.sh [--cell2] [-hkds]
Run qemu
-h, --help                     Display help
-d, --diskid                   NBS Disk ID
--cell2                        Use NBS and QEMU ports of cell-2 (default: cell-1)
-s, --socket                   Socket path
-k, --encryption-key-path      Encryption key path
-e, --encrypted                Use default encryption key
EOF
}

#defaults
encryption=""
diskid=""
socket=""
options=$(getopt -l "help,cell2,key:,diskid:,socket:,encryption-key-path:,encrypted" -o "hk:d:s:e" -a -- "$@")

if [ $? != 0 ] ; then
    echo "Incorrect options provided"
    exit 1
fi
eval set -- "$options"

while true
do
    case "$1" in
    -h | --help )
        show_help
        exit 0
        ;;
    --cell2 )
        run_in_cell2 "${original_args[@]}"
        shift
        ;;
    -k | --encryption-key-path )
        encryption="--encryption-mode=aes-xts --encryption-key-path=${2}"
        shift 2
        ;;
    -e | --encrypted )
        encryption="--encryption-mode=aes-xts --encryption-key-path=encryption-key.txt"
        shift 1
        ;;
    -d | --diskid )
        diskid=${2}
        shift 2
        ;;
    -s | --socket )
        socket=${2}
        shift 2
        ;;
    --)
        shift
        break;;
    esac
done

if [ -z "$diskid" ] ; then
    echo "Disk id shouldn't be empty"
    exit 1
fi

if [ -z "$socket" ] ; then
    socket="/tmp/${diskid}${NBS_CELL_SUFFIX:-}.sock"
fi

require_bins "$BLOCKSTORE_CLIENT_BIN" || exit 1

# prepare qemu image

QEMU_BIN_DIR=$(qemu_bin_dir)
QEMU_BIN_TAR=$QEMU_BIN_DIR/qemu-bin.tar.gz
QEMU=$QEMU_BIN_DIR/usr/bin/qemu-system-x86_64
QEMU_FIRMWARE=$QEMU_BIN_DIR/usr/share/qemu
DISK_IMAGE=$QEMU_BIN_DIR/../image-noble/rootfs.img

if [[ ! -x "$QEMU" ]]; then
    echo "expand qemu tar from [$QEMU_BIN_TAR]"
    if [[ ! -f "$QEMU_BIN_TAR" ]]; then
        echo "$QEMU_BIN_TAR not found; build cloud/blockstore/buildall first" >&2
        exit 1
    fi
    tar -xzf "$QEMU_BIN_TAR" -C "$QEMU_BIN_DIR"
fi

# start endpoint for disk
echo "starting endpoint [${socket}] for disk [${diskid}]"
blockstore-client stopendpoint --socket $socket
blockstore-client startendpoint --ipc-type vhost --socket $socket --client-id client-1 --instance-id localhost --disk-id $diskid --persistent $encryption
sleep 1

# run qemu with secondary disk
qmp_port=${QEMU_QMP_PORT:-8678}
ssh_port=${QEMU_SSH_PORT:-8679}
gdb_port=${QEMU_GDB_PORT:-1234}

MACHINE_ARGS=" \
    -L $QEMU_FIRMWARE \
    -snapshot \
    -nodefaults
    -cpu host \
    -smp 4,sockets=1,cores=4,threads=1 \
    -enable-kvm \
    -m 16G \
    -name debug-threads=on \
    -qmp tcp:127.0.0.1:${qmp_port},server,nowait \
    "

MEMORY_ARGS=" \
    -object memory-backend-memfd,id=mem,size=16G,share=on \
    -numa node,memdev=mem \
    "

NET_ARGS=" \
    -netdev user,id=netdev0,hostfwd=tcp::${ssh_port}-:22 \
    -device virtio-net-pci,netdev=netdev0,id=net0 \
    "

DISK_ARGS=" \
    -object iothread,id=iot0 \
    -drive format=qcow2,file=$DISK_IMAGE,id=lbs0,if=none,aio=native,cache=none,discard=unmap \
    -device virtio-blk-pci,scsi=off,drive=lbs0,id=virtio-disk0,iothread=iot0,bootindex=1 \
    "

NBS_ARGS=" \
    -chardev socket,id=vhost0,path=$socket \
    -device vhost-user-blk-pci,chardev=vhost0,id=vhost-user-blk0,num-queues=1 \
    "

echo "Running qemu with disk [$diskid]"
$QEMU \
    $MACHINE_ARGS \
    $MEMORY_ARGS \
    $NET_ARGS \
    $DISK_ARGS \
    $NBS_ARGS \
    -nographic \
    -serial stdio \
    -gdb tcp:127.0.0.1:${gdb_port}
