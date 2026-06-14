#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
    exit 2
fi

LOG_DIR=/home/github/tmp_test/logs
SYSTEM_LOGS_DIR=$LOG_DIR/system_logs
KERN_LOG_PATH=$LOG_DIR/kern.log
TMP_TEST_DIR=/home/github/tmp_test

RUNNER_UID=${SUDO_UID:-}
RUNNER_GID=${SUDO_GID:-}
if [ -z "$RUNNER_UID" ] || [ -z "$RUNNER_GID" ]; then
    RUNNER_UID=$(id -u github)
    RUNNER_GID=$(id -g github)
fi

TMP_DIR=$(mktemp -d /tmp/actions-runner-system-logs.XXXXXX)
TMP_SYSTEM_LOGS_DIR=$TMP_DIR/system_logs
TMP_KERN_LOG_PATH=$TMP_DIR/kern.log

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$TMP_SYSTEM_LOGS_DIR"

if dmesg -T > "$TMP_SYSTEM_LOGS_DIR/dmesg.log" 2> /dev/null; then
    chmod 0644 "$TMP_SYSTEM_LOGS_DIR/dmesg.log"
fi

if [ -r /var/log/kern.log ]; then
    install -m 0644 /var/log/kern.log "$TMP_KERN_LOG_PATH"
fi

if [ -r /var/log/syslog ]; then
    install -m 0644 /var/log/syslog "$TMP_SYSTEM_LOGS_DIR/syslog.log"
fi

for atop_log in /var/log/atop/atop_*; do
    if [ -r "$atop_log" ]; then
        install -m 0644 "$atop_log" "$TMP_SYSTEM_LOGS_DIR/$(basename "$atop_log")"
    fi
done

for dir in "$TMP_TEST_DIR" "$LOG_DIR"; do
    if [ -L "$dir" ] || { [ -e "$dir" ] && [ ! -d "$dir" ]; }; then
        rm -f "$dir"
    fi
    mkdir -p "$dir"
    if [ -L "$dir" ] || [ ! -d "$dir" ]; then
        echo "Refusing to publish system logs through unsafe path: $dir" >&2
        exit 1
    fi
    chown "$RUNNER_UID:$RUNNER_GID" "$dir"
    chmod 0755 "$dir"
done

rm -rf "$SYSTEM_LOGS_DIR"
rm -f "$KERN_LOG_PATH"

find "$TMP_SYSTEM_LOGS_DIR" -type d -exec chmod 0755 {} +
find "$TMP_SYSTEM_LOGS_DIR" -type f -exec chmod 0644 {} +
chown -R "$RUNNER_UID:$RUNNER_GID" "$TMP_SYSTEM_LOGS_DIR"
mv -T "$TMP_SYSTEM_LOGS_DIR" "$SYSTEM_LOGS_DIR"

if [ -e "$TMP_KERN_LOG_PATH" ]; then
    chown "$RUNNER_UID:$RUNNER_GID" "$TMP_KERN_LOG_PATH"
    chmod 0644 "$TMP_KERN_LOG_PATH"
    mv -T "$TMP_KERN_LOG_PATH" "$KERN_LOG_PATH"
fi
