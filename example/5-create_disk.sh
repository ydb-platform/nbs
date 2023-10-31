#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`

show_help() {
    cat << EOF
Usage: ./5-create_disk.sh [-hk]
Creates disk with requested kind
-h, --help         Display help
-k, --kind         Kind of disk ssd|nonreplicated|mirror2|mirror3 (default: ssd)
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


# create disk
echo "Creating disk in $kind mode"
CLIENT="./blockstore-client"
$BIN_DIR/$CLIENT createvolume --storage-media-kind $kind --blocks-count $blocks_count --disk-id $id

if [ $? -ne 0 ]; then
    echo "Disk $id creation failed"
    exit 1
fi
