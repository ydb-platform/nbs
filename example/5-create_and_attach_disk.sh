#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

show_help() {
    cat << EOF
Usage: ./5-create_and_attach_disk.sh [-hkd]
Creates disk with requested kind and attach it to device
-h, --help         Display help
-k, --kind         Kind of disk ssd|nonreplicated|mirror2|mirror3 (default: ssd)
-d, --device       Device name to attach to (default: /dev/nbd0)
EOF
}

#defaults
kind="ssd"
device="/dev/nbd0"
options=$(getopt -l "help,kind:,device:" -o "hk:d:" -a -- "$@")

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
    -k | --kind )
        kind=${2}
        shift 2
        ;;
    -d | --device )
        device=${2}
        shift 2
        ;;
    --)
        shift
        break;;
    esac
done

case $kind in
"ssd")
    id="vol0"; blocks_count=8388608;; # 32GiB
"nonreplicated")
    id="nbr0"; blocks_count=262144;;
"mirror2")
    id="mrr0"; blocks_count=262144;;
"mirror3")
    id="mrr1"; blocks_count=262144;;
*)
    echo "$kind mode is not supported"
    show_help
    exit 1
    ;;
esac

if ! [[ $device == /dev/nbd[0-9] ]]; then
    echo "Device name should match pattern '/dev/nbd[0-9]', provided: $device"
    exit 1
fi

# create disk
echo "Creating disk $id in $kind mode"
<<<<<<< HEAD
CLIENT="./blockstore-client"
export LD_LIBRARY_PATH=$(dirname $(readlink $BIN_DIR/$CLIENT))
$BIN_DIR/$CLIENT createvolume \
=======
blockstore-client createvolume \
>>>>>>> 53ebe7b85 (Updating build documentation)
    --storage-media-kind $kind --blocks-count $blocks_count --disk-id $id

if [ $? -ne 0 ]; then
    echo "Disk $id creation failed"
    exit 1
else
    echo "Disk $id successfully created"
fi

# attach disk
echo "Attaching disk $id to $device"
SOCK="$BIN_DIR/$id.sock"

sudo modprobe nbd
touch $SOCK
<<<<<<< HEAD
cmd="LD_LIBRARY_PATH=$(dirname $(readlink $BIN_DIR/$NBD)) \
    $BIN_DIR/${NBD} --device-mode endpoint --disk-id $id --access-mode rw \
    --mount-mode local --connect-device $device --listen-path $SOCK"
 
sudo $cmd
=======
sudo-blockstore-nbd --device-mode endpoint --disk-id $id --access-mode rw \
    --mount-mode local --connect-device $device --listen-path $SOCK
>>>>>>> 53ebe7b85 (Updating build documentation)

if [ $? -ne 0 ]; then
    echo "Attaching disk $id to $device failed"
    exit 1
fi
