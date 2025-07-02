#!/usr/bin/env bash

set +e

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
STORAGE_DIR=$BIN_DIR/../../storage
QEMU_DIR=$STORAGE_DIR/core/tools/testing/qemu

: ${QEMU_BIN_DIR:=$QEMU_DIR/bin}

QEMU_BIN_TAR=$QEMU_BIN_DIR/qemu-bin.tar.gz
QEMU_REL=/usr/bin/qemu-system-x86_64
QEMU_FIRMWARE_REL=/usr/share/qemu

: ${QEMU:=$QEMU_BIN_DIR$QEMU_REL}
: ${QEMU_FIRMWARE:=$QEMU_BIN_DIR$QEMU_FIRMWARE_REL}

[[ ( ! -x $QEMU && ${QEMU%$QEMU_REL} == $QEMU_BIN_DIR ) ||
    ( ! -d $QEMU_FIRMWARE &&
      ${QEMU_FIRMWARE%$QEMU_FIRMWARE_REL} == $QEMU_BIN_DIR ) ]] &&
      tar -xzf $QEMU_BIN_TAR -C $QEMU_BIN_DIR

: ${QMP_PORT:=4444}
: ${DISK_IMAGE:=$QEMU_DIR/image-noble/rootfs.img}
: ${VHOST_SOCKET_PATH:=/tmp/vhost.sock}

: ${MEM_SIZE:=4G}
: ${NCORES:=16}

args=(
    -L $QEMU_FIRMWARE
    -snapshot
    -nodefaults
    -cpu host
    -smp $NCORES,sockets=1,cores=$NCORES,threads=1
    -enable-kvm
    -m $MEM_SIZE
    -name debug-threads=on
    # so far static qemu built for ubuntu 16 crashes under ubuntu-22+
    # if needed you may use real ip instead of localhost, e.g. 127.0.0.1
    # -qmp tcp:localhost:$QMP_PORT,server,nowait

    -object memory-backend-memfd,id=mem,size=$MEM_SIZE,share=on
    -numa node,memdev=mem

    -netdev user,id=netdev0,hostfwd=tcp::3389-:3389
    -device virtio-net-pci,netdev=netdev0,id=net0

    -drive format=qcow2,file="$DISK_IMAGE",id=hdd0,if=none,aio=native,cache=none,discard=unmap
    -device virtio-blk-pci,id=vblk0,drive=hdd0,num-queues=$NCORES,bootindex=1

    -chardev socket,path=$VHOST_SOCKET_PATH,id=vhost0,reconnect=1
    -device vhost-user-fs-pci,chardev=vhost0,id=vhost-user-fs0,tag=fs0,queue-size=512

    -serial stdio
    -nographic
    ${KERNEL_IMAGE:+-kernel $KERNEL_IMAGE}
    ${KCMDLINE:+-append "$KCMDLINE"}
    -s
)

# QEMU="ya tool gdb --args $QEMU"
exec $QEMU "${args[@]}" "$@"

# to configure fs via QMP
# nc localhost 4444 <<END
# { "execute": "qmp_capabilities" }
# {"execute":"chardev-add","arguments":{"id":"vhost0","backend":{"type":"socket","data":{"addr":{"type":"unix","data":{"path":"/tmp/vhost.sock"}},"server":false,"reconnect":1}}}}
# {"execute":"device_add","arguments":{"id":"fs0","driver":"vhost-user-fs-pci","chardev":"vhost0","tag":"fs0","queue-size":512}}
# END
