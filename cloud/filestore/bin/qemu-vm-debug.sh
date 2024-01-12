#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
DATA_DIR=`readlink -e $BIN_DIR/data`

QEMU=${QEMU:-$BIN_DIR/qemu-system-x86_64}
KERNEL_IMAGE=${KERNEL_IMAGE:-$DATA_DIR/vmlinux}
DISK_IMAGE=${DISK_IMAGE:-$DATA_DIR/rootfs.img}
VHOST_SOCKET_PATH=${VHOST_SOCKET_PATH:-$DATA_DIR/vhost.sock}
MEM_SIZE=${MEM_SIZE:-2G}
SSH_PORT=${SSH_PORT:-2222}

MACHINE_ARGS=" \
    -cpu host \
    -smp 2,sockets=2,cores=1,threads=1 \
    -enable-kvm \
    -m $MEM_SIZE \
    -name debug-threads=on \
    "

MEMORY_ARGS=" \
    -object memory-backend-memfd,id=mem,size=$MEM_SIZE,share=on \
    -numa node,memdev=mem \
    "

NET_ARGS=" \
    -netdev user,id=net0,hostfwd=tcp::$SSH_PORT-:22 \
    -device e1000,netdev=net0 \
    "


DISK_ARGS=" \
    -drive file=$DISK_IMAGE,index=0,media=disk,format=qcow2 \
    "

FS_ARGS=" \
    -chardev socket,path=$VHOST_SOCKET_PATH,id=vhost0 \
    -device vhost-user-fs-pci,chardev=vhost0,id=vhost-user-fs0,tag=fs0 \
    "

# ya tool gdb --args \
$QEMU \
    $MACHINE_ARGS \
    $MEMORY_ARGS \
    $NET_ARGS \
    $DISK_ARGS \
    $FS_ARGS \
    -nographic \
    -kernel $KERNEL_IMAGE \
    -append "console=ttyS0 root=/dev/sda rw" \
    -s \
    $@
