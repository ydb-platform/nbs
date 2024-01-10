#!/usr/bin/env bash

DISK_ID=$1
DEV_PATH=/dev/nbd0
SOCK_PATH=/var/tmp/tmp.socket

DUMPE2FS=e2fsprogs-1.46.4/misc/dumpe2fs
XFS_DB=xfsprogs-5.3.0/db/xfs_db

download () {
    name=`basename $1`
    wget $1 -O $name &&
    tar xf $name &&
    rm $name
}

e2fs_size () {
    awk '
    /^Block size/ {size = int($3)}
    /^Block count/ {count = int($3)}
    END {print count * size}'
}

xfs_size () {
    awk '
    /^blocksize/ {bs = int($3)}
    /^dblocks/ {db = int($3)}
    /^rblocks/ {rb = int($3)}
    END {print (db + rb) * bs}'
}

check_fs_size () {
    blk_size=`sudo blockdev --getsize64 $1`

    if [ $2 -ne $blk_size ]; then
        echo "$1: filesystem size doesn't match underlying blockdev: $blk_size - $2 = $(($blk_size - $2))"
    fi
}

check_e2fs () {
    info=`sudo $DUMPE2FS -h $dev 2>&1`

    if [ $? -eq 0 ]; then
        echo "$dev: e2fs detected"

        fs_size=`echo "$info" | e2fs_size`
        check_fs_size $1 $fs_size
    fi
}

# requires xfsprogs

check_xfs () {
    info=`sudo $XFS_DB $1 -c sb -c p 2>&1`

    if [ $? -eq 0 ]; then
        echo "$1: xfs detected"

        fs_size=`echo "$info" | xfs_size`
        check_fs_size $1 $fs_size
    fi
}

check_fs () {
    for dev in $DEV_PATH ${DEV_PATH}p*; do
        check_e2fs $dev
        check_xfs $dev
    done
}

if [ ! -x $DUMPE2FS ]; then
    download https://proxy.sandbox.yandex-team.ru/2524949840
fi

if [ ! -x $XFS_DB ]; then
    download https://proxy.sandbox.yandex-team.ru/2524943180
fi

sudo modprobe nbd

echo "Process disk $1 | $DEV_PATH"

sudo blockstore-nbd            \
    --verbose error            \
    --connect-device $DEV_PATH \
    --disk-id $DISK_ID         \
    --mount-mode remote        \
    --throttling-disabled 1    \
    --device-mode endpoint     \
    --listen-path $SOCK_PATH   \
    --access-mode ro &

while [ `sudo blockdev --getsize64 $DEV_PATH` -eq "0" ]
do
    echo "wait for device..."
    sleep 1

    if [ "`pgrep blockstore-nbd | wc -l`" -eq "0" ]
    then
       echo "fail"
       exit 1
    fi
done
echo ""

echo "=== blkid ==="
sudo blkid $DEV_PATH
echo ""

# проверяем наличие RAID

echo "=== RAID ==="
sudo ./detect-raid $DEV_PATH
echo ""

# разметка на партиции

echo "=== Partitions ==="
sudo sgdisk -p $DEV_PATH
echo ""

echo "=== Filesystems ==="
check_fs
echo ""

sudo kill `pgrep blockstore-nbd`

while [ `sudo blockdev --getsize64 $DEV_PATH` -ne "0" ]
do
    echo "wait for detach ..."
    sleep 1
done
