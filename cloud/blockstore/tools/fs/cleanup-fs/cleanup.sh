#!/usr/bin/env bash

# ./cleanup.sh disk_id fs-type [part num]

DISK_ID=$1
FS_TYPE=$2
DEV_PATH=/dev/nbd0
TARGET_PATH=$DEV_PATH
MNT_POINT=/mnt/maintainance

if [ -z "$DISK_ID" ]
then
    echo "[ERROR] empty DISK_ID"
    exit 1
fi

if [ -z "$FS_TYPE" ]
then
    echo "[ERROR] empty FS_TYPE"
    exit 1
fi

case "$FS_TYPE" in
    "ext4" ) ;;
    "xfs"  ) ;;
    "auto" ) ;;
    *      )
        echo "[ERROR] not implemented yet: $FS_TYPE"
        exit 1
    ;;
esac

if [ -n "$3" ]
then
    TARGET_PATH="${DEV_PATH}p$3"
fi

sudo mkdir -p $MNT_POINT

echo "Target device: $DEV_PATH"

sudo blockstore-nbd            \
    --verbose error            \
    --connect-device $DEV_PATH \
    --disk-id $DISK_ID         \
    --mount-mode remote        \
    --throttling-disabled 1    \
    --device-mode endpoint     \
    --listen-path /var/tmp/NBS-2636.socket \
    --access-mode rw &

while [ `sudo blockdev --getsize64 $DEV_PATH` -eq "0" ]
do
    echo "wait for device..."
    sleep 1

    if [ -z "`pgrep blockstore-nbd`" ]
    then
        echo "something goes wrong"
        exit 1
    fi
done

function try_detect_fs {

    sudo dumpe2fs $TARGET_PATH &> /dev/null
    if [ "$?" = "0" ]
    then
        echo "ext4"
        return
    fi

    sudo ./xfs_db -c sb -c p $TARGET_PATH &> /dev/null
    if [ "$?" = "0" ]
    then
        echo "xfs"
        return
    fi

    echo "Can't detect filesystem type"
    exit 1
}

if [ "$FS_TYPE" = "auto" ]
then
    FS_TYPE=`try_detect_fs`
    echo "detected filesystem: $FS_TYPE"
fi

if [ "$FS_TYPE" = "ext4" ]
then
    sudo dumpe2fs $TARGET_PATH > "dumpe2fs.$DISK_ID.before.txt"
fi

if [ "$FS_TYPE" = "xfs" ]
then
    sudo ./xfs_db -c sb -c p $TARGET_PATH > "xfs_db.$DISK_ID.before.txt"
fi

echo "Target path: $TARGET_PATH"

sudo mount -lv $TARGET_PATH $MNT_POINT

if [ "$FS_TYPE" = "ext4" ] || [ "$FS_TYPE" = "xfs" ]
then
    AVAIL_SPACE="`sudo df -lh $MNT_POINT | tail -n 1  | sed -r 's/[ ]+/ /g' | cut -f4 -d' '`"
    APPROX_SIZE=" -s $AVAIL_SPACE "
else
    AVAIL_SPACE="?"
    APPROX_SIZE=""
fi

while true
do
    BUBBLE_NAME=$MNT_POINT/`tr -dc A-Za-z0-9 </dev/urandom | head -c 32`

    if [ ! -e "$BUBBLE_NAME" ]
    then
        break
    fi

done

echo "Fill $BUBBLE_NAME ($AVAIL_SPACE) ..."

sudo dd if=/dev/zero bs=1M | pv $APPROX_SIZE | sudo dd bs=1M of=$BUBBLE_NAME oflag=direct,sync iflag=fullblock 2> err.txt

if [ "$FS_TYPE" = "ext4" ]
then
    sudo dumpe2fs $TARGET_PATH > "dumpe2fs.$DISK_ID.after.txt"
fi

if [ `grep "No space left on device" err.txt | wc -l` != "1" ]
then
    echo "Unexpected error #1"
    cat err.txt

    exit 1
fi

sudo dd if=/dev/zero | pv | sudo dd of="$BUBBLE_NAME.small" oflag=direct,sync iflag=fullblock 2> err.txt

if [ `grep "No space left on device" err.txt | wc -l` != "1" ]
then
    echo "Unexpected error #2"
    cat err.txt

    exit 1
fi

echo "OK"

sudo df -lh $MNT_POINT

sync

sudo rm $BUBBLE_NAME

sync

sudo rm "$BUBBLE_NAME.small"

sudo umount $MNT_POINT

if [ "$FS_TYPE" = "xfs" ]
then
    sudo ./xfs_db -c sb -c p $TARGET_PATH > "xfs_db.$DISK_ID.after.txt"
fi

sudo kill `pgrep blockstore-nbd`

while [ `sudo blockdev --getsize64 $DEV_PATH` -ne "0" ]
do
    echo "wait for detach ..."
    sleep 1
done

echo "SUCCESS"

exit 0
