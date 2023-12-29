#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

show_help() {
    cat << EOF
Usage: ./6-run_qemu.sh [-hids]
Run qemu
-h, --help         Display help
-i, --image        Image path
-d, --diskid       NBS Disk ID
-s, --socket       Socket path
EOF
}

#defaults
image=""
diskid=""
socket=""
options=$(getopt -l "help,image:,diskid:,socket:" -o "hid:s:" -a -- "$@")

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
    -i | --image )
        image=${2}
        shift 2
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

if [ -z "$diskid" ] then
    echo "Disk id shouldn't be empty"
    exit 1
fi

if [ -z "$socket" ] then
    echo "Socket path shouldn't be empty"
    exit 1
fi

CLIENT="./blockstore-client"

# start endpoint for disk
echo "starting endpoint ${socket} for disk ${diskid}"
$BIN_DIR/$CLIENT stopendpoint --socket $socket
$BIN_DIR/$CLIENT startendpoint --ipc-type vhost --socket $socket --disk-id $diskid --persistent

# run qemu with secondary disk
qmp_port=8678
ssh_port=8679

MACHINE_ARGS=" \
    -L /usr/share/qemu \
    -snapshot \
    -cpu host \
    -smp 16,sockets=1,cores=16,threads=1 \
    -enable-kvm \
    -m 16G \
    -name debug-threads=on \
    -qmp tcp:localhost:${qmp_port},server,nowait \
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
    -drive format=qcow2,file=$image,id=lbs0,if=none,aio=native,cache=none,discard=unmap \
    -device virtio-blk-pci,scsi=off,drive=lbs0,id=virtio-disk0,iothread=iot0,bootindex=1 \
    "

NBS_ARGS=" \
    -chardev socket,id=vhost0,path=$socket \
    -device vhost-user-blk-pci,chardev=vhost0,id=vhost-user-blk0,num-queues=1 \
    "

echo "Running qemu with disk $diskid"
$QEMU \
    $MACHINE_ARGS \
    $MEMORY_ARGS \
    $NET_ARGS \
    $DISK_ARGS \
    $NBS_ARGS \
    -nographic \
    -s & ps aux | grep "$diskid" | grep -v grep & echo "started qemu"

# TODO: wait for guest startup properly
sleep 30
