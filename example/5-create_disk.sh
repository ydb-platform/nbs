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
-h, --help                     Display help
-k, --kind                     Kind of disk ssd|hdd|nonreplicated|mirror2|mirror3|local (default: ssd)
-d, --disk-id                  disk-id
--cloud-id                     cloud-id
--folder-id                    folder-id
-b, --base-disk-id             the base disk if you want to create an overlay disk. Should use together with the base-disk-checkpoint-id
-c, --base-disk-checkpoint-id  the checkpoint-id if you want to create an overlay disk. Should use together with the base-disk-id
-e, --encrypted                Encrypt disk with default encryption key
EOF
}

#defaults
kind="ssd"
disk_id=""
cloud_id="test-cloud"
folder_id="test-folder"
options=$(getopt -l "help,kind:,disk-id:,encrypted,base-disk-id:,base-disk-checkpoint-id:,cloud-id:,folder-id:" -o "hk:d:eb:c:" -a -- "$@")
block_size=4096
encryption=""
base_disk_id=""
base_disk_checkpoint_id=""

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
    -e | --encrypted )
        encryption="--encryption-mode=aes-xts --encryption-key-path=encryption-key.txt"
        shift 1
        ;;
    -b | --base-disk-id )
        base_disk_id=${2}
        shift 2
        ;;
    -c | --base-disk-checkpoint-id )
        base_disk_checkpoint_id=${2}
        shift 2
        ;;
    --cloud-id )
        cloud_id=${2}
        shift 2
        ;;
    --folder-id )
        folder_id=${2}
        shift 2
        ;;
    --)
        shift
        break;;
    esac
done

case $kind in
"ssd")
    default_id="vol0"; blocks_count=262144;;
"hdd")
    default_id="hdd0"; blocks_count=262144;;
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

if [ ! -z "$base_disk_id" ] ; then
  echo base_disk_id=$base_disk_id
  base_disk="--base-disk-id $base_disk_id --base-disk-checkpoint-id $base_disk_checkpoint_id"
  echo $base_disk
fi

# create disk
echo "Creating disk $disk_id in $kind mode"
blockstore-client createvolume \
    --storage-media-kind $kind \
    --blocks-count $blocks_count \
    --block-size $block_size \
    --disk-id $disk_id \
    --cloud-id $cloud_id \
    --folder-id $folder_id \
    $encryption $base_disk \

if [ $? -ne 0 ]; then
    echo "Disk $disk_id creation failed"
    exit 1
else
    echo "Disk $disk_id successfully created"
fi
