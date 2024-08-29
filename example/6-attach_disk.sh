#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

show_help() {
    cat << EOF
Usage: ./6-attach_disk.sh [-hvd]
Creates disk with requested kind and attach it to device
-h, --help         Display help
-v, --disk-id      Disk id
-d, --device       Device name to attach to (default: /dev/nbd0)
-e, --encrypted    Encrypt disk with default encryption key
EOF
}

#defaults
disk_id="nrd0"
device="/dev/nbd0"
options=$(getopt -l "help,disk-id:,device:,encrypted" -o "hv:d:e" -a -- "$@")
encryption=""

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
    -v | --disk-id )
        disk_id=${2}
        shift 2
        ;;
    -d | --device )
        device=${2}
        shift 2
        ;;
    -e | --encrypted )
        encryption="--encryption-mode=aes-xts --encryption-key-path=encryption-key.txt"
        shift 1
        ;;
    --)
        shift
        break;;
    esac
done

if ! [[ $device == /dev/nbd[0-9] ]]; then
    echo "Device name should match pattern '/dev/nbd[0-9]', provided: $device"
    exit 1
fi

# attach disk
echo "Attaching disk $disk_id to $device"
SOCK="$BIN_DIR/$disk_id.sock"

sudo modprobe nbd
touch $SOCK
sudo-blockstore-nbd --device-mode endpoint --disk-id $disk_id --access-mode rw \
    --mount-mode local --connect-device $device --listen-path $SOCK \
    $encryption

if [ $? -ne 0 ]; then
    echo "Attaching disk $disk_id to $device failed"
    exit 1
fi
