#!/usr/bin/env bash

set -e

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
LOGS_DIR=$BIN_DIR/data

SERVER_PORT=${SERVER_PORT:-9021}
VHOST_PORT=${VHOST_PORT:-9022}

FS=${FS:-"nfs"}
BLOCK_SIZE=${BLOCK_SIZE:-4096}
BLOCK_COUNT=${BLOCK_COUNT:-1048576} # 4GiB
MOUNT_POINT=${MOUNT_POINT:-"$HOME/$FS"}
VHOST_SOCKET_PATH=${VHOST_SOCKET_PATH:-/tmp/vhost.sock}

PID_FILE=$BIN_DIR/pids.txt

################################################################################
# STOP/KILL
if [[ "$1" == "stop" ]]; then
    if [[ -e "$PID_FILE" ]]; then
        for pid in `cat $PID_FILE`; do
            echo "stopping $pid"
            pids=$(ps -o pid= --ppid $pid) || continue

            echo "stopping $pids"
            kill -SIGTERM $pids || true
        done
    fi

    shift
fi

if [[ "$1" == "kill" ]]; then
    if [[ -e "$PID_FILE" ]]; then
        for pid in `cat $PID_FILE`; do
            pids=$(ps -o pid= --ppid $pid) || continue

            echo "killing $pids"
            kill -SIGKILL $pids
        done
    fi

    shift
fi

if [[ "$1" == "format" ]]; then
    $BIN_DIR/kikimr-format.sh
    shift
fi

################################################################################
# START

if [[ "$1" == "startnull" ]]; then
    rm -f $LOGS_DIR/*log.txt
    rm -f $PID_FILE

    $BIN_DIR/filestore-server-null.sh &>$LOGS_DIR/filestore-server-log.txt &
    echo $! >> $PID_FILE
    echo "started filestore server w null service $!"

    $BIN_DIR/filestore-vhost-local.sh &>$LOGS_DIR/filestore-vhost-log.txt &
    echo $! >> $PID_FILE
    echo "started vhost server $!"

    shift
elif [[ "$1" == "startlocal" ]]; then
    rm -f $LOGS_DIR/*log.txt
    rm -f $PID_FILE

    $BIN_DIR/filestore-server-local.sh &>$LOGS_DIR/filestore-server-log.txt &
    echo $! >> $PID_FILE
    echo "started filestore server w local service $!"

    $BIN_DIR/filestore-vhost-local.sh &>$LOGS_DIR/filestore-vhost-log.txt &
    echo $! >> $PID_FILE
    echo "started vhost server $!"

    shift
elif [[ "$1" == "start" || "$1" == "initialize" ]]; then
    if [[ "$1" == "initialize" ]]; then
        rm -f $LOGS_DIR/*log.txt
        rm -f $PID_FILE
    fi

    $BIN_DIR/kikimr-server.sh &>$LOGS_DIR/kikimr-server-log.txt &
    echo $! >> $PID_FILE
    echo "started kikimr server $!"

    if [[ "$1" == "initialize" ]]; then
        sleep 5
        echo "initing kikimr server"
        $BIN_DIR/kikimr-init.sh
        echo "inited kikimr server"
    fi

    $BIN_DIR/filestore-server.sh &>>$LOGS_DIR/filestore-server-log.txt &
    echo $! >> $PID_FILE
    echo "started filestore server $!"

    $BIN_DIR/filestore-vhost.sh &>>$LOGS_DIR/filestore-vhost-log.txt &
    echo $! >> $PID_FILE
    echo "started vhost server $!"

    shift
fi

################################################################################
# CREATE

if [[ "$1" == "create" ]]; then
    $BIN_DIR/filestore-client create        \
        --server-port       $SERVER_PORT    \
        --filesystem        "$FS"           \
        --cloud             "cloud"         \
        --folder            "folder"        \
        --blocks-count      "$BLOCK_COUNT"  \
        --block-size        "$BLOCK_SIZE"   \
        nfs
    shift
fi

################################################################################
# MOUNT

if [[ "$1" == "mount" ]]; then
    [ -d "$MOUNT_POINT" ] || mkdir "$MOUNT_POINT"

    $BIN_DIR/filestore-client mount         \
        --server-port       $SERVER_PORT    \
        --filesystem        "$FS"           \
        --mount-path        "$MOUNT_POINT"  \
        --verbose           trace           \
        2>&1 | grep -v PingSession          \
        2>&1 &>$LOGS_DIR/nfs-mount.txt      \
        &

    echo $! >> $PID_FILE
    echo "$FS mounted at $MOUNT_POINT"

    shift
fi

################################################################################
# ENDPOINTS

if [[ "$1" == "startendpoint" ]]; then
    echo "creating endpoint"
    $BIN_DIR/filestore-client startendpoint         \
        --server-port       $VHOST_PORT             \
        --filesystem        "$FS"                   \
        --socket-path       "$VHOST_SOCKET_PATH"    \
        --client-id         "local-qemu"
    echo "started endpoint at $VHOST_SOCKET_PATH"

    shift
fi

if [[ "$1" == "stopendpoint" ]]; then
    echo "stopping endpoint"
    $BIN_DIR/filestore-client stopendpoint  \
        --server-port       $VHOST_PORT     \
        --socket-path       "$VHOST_SOCKET_PATH"

    echo "stopped endpoint at $VHOST_SOCKET_PATH"

    shift
fi

if [ -n "$1" ]; then
    echo "warning unparsed arguments: $@"
fi
