#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
DATA_DIR=`readlink -e $BIN_DIR/data`

QEMU=${QEMU:-$BIN_DIR/qemu-system-x86_64}
SETUP_IMAGE=${SETUP_IMAGE:-$DATA_DIR/ubuntu-16.04.7-server-amd64.iso}
DISK_IMAGE=${DISK_IMAGE:-$DATA_DIR/disk.img}
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

SERIAL_ARGS=" \
    -serial file:$DATA_DIR/console.log \
    "

USB_ARGS=" \
    -usb \
    -device usb-tablet,bus=usb-bus.0 \
    "

VGA_ARGS=" \
    -device VGA,id=vga0,vgamem_mb=16 \
    "

VNC_ARGS=" \
    -vnc 127.0.0.1:0 \
    "

CDROM_ARGS=" \
    -cdrom $SETUP_IMAGE \
    "

# ya tool gdb --args \
$QEMU \
    $MACHINE_ARGS \
    $MEMORY_ARGS \
    $SERIAL_ARGS \
    $USB_ARGS \
    $VGA_ARGS \
    $NET_ARGS \
    $VNC_ARGS \
    $DISK_ARGS \
    $FS_ARGS \
    $@
