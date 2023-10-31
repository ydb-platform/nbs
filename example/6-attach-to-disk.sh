#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

show_help() {
    cat << EOF
Usage: ./6-attach-to-disk.sh [-hk]
Attaches to disk with requested kind and allows to use it. Should be used with the same kind as 5-create-disk.sh.
-h, --help         Display help
-n, --kind         Kind of disk ssd|nonreplicated|mirror2|mirror3 (default: ssd)
EOF
}

#defaults
kind="ssd"
options=$(getopt -l "help,kind:" -o "hk:" -a -- "$@")

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
--)
    shift
    break;;
esac
done

case $kind in
"ssd")
    id="vol0"
    # 32GiB
    blocks_count=8388608
    ;;
"nonreplicated")
    id="nbr0"
    blocks_count=262144
    ;;
"mirror2")
    id="mrr0"
    blocks_count=262144
    ;;
"mirror3")
    id="mrr1"
    blocks_count=262144
    ;;
*)
    echo "$kind mode is not supported \n"
    show_help
    exit 1
    ;;
esac


#attach disk
echo "Attaching to disk in $kind mode"
NBD="./blockstore-nbd"
SOCK="$BIN_DIR/$id.sock"

sudo modprobe nbd
touch $SOCK
sudo $NBD --device-mode endpoint --disk-id $id --access-mode rw --mount-mode local --connect-device /dev/$id --listen-path $SOCK >$BIN_DIR/logs/nbd.log 2>&1
