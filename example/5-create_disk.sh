#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
source ./prepare_binaries.sh || exit 1

show_help() {
    cat << EOF
Usage: ./5-create_disk.sh [-hkd]
Creates disk with requested kind and attach it to device
-h, --help         Display help
-k, --kind         Kind of disk ssd|nonreplicated|mirror2|mirror3|local (default: ssd)
-d, --disk-id      disk-id
EOF
}

#defaults
kind="ssd"
disk_id=""
options=$(getopt -l "help,kind:,disk-id:" -o "hk:d:" -a -- "$@")
block_size=4096

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
    -d | --disk-id )
        disk_id=${2}
        shift 2
        ;;
    --)
        shift
        break;;
    esac
done

case $kind in
"ssd")
    default_id="vol0"; blocks_count=8388608;; # 32GiB
"nonreplicated")
    default_id="nbr0"; blocks_count=262144;;
"mirror2")
    default_id="mrr0"; blocks_count=262144;;
"mirror3")
    default_id="mrr1"; blocks_count=262144;;
"local")
    default_id="lcl1"; blocks_count=2097152; block_size=512;;
*)
    echo "$kind mode is not supported"
    show_help
    exit 1
    ;;
esac

if [ $disk_id == ""] ; then
  disk_id=$default_id
fi

# create disk
echo "Creating disk $disk_id in $kind mode"
blockstore-client createvolume \
    --storage-media-kind $kind \
    --blocks-count $blocks_count \
    --block-size $block_size \
    --disk-id $disk_id \
    --config nbs/nbs-client.txt

if [ $? -ne 0 ]; then
    echo "Disk $disk_id creation failed"
    exit 1
else
    echo "Disk $disk_id successfully created"
fi
