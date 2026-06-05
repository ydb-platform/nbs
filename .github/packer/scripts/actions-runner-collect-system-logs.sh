#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
    exit 2
fi

LOG_DIR=/home/github/tmp_test/logs
SYSTEM_LOGS_DIR=$LOG_DIR/system_logs
KERN_LOG_PATH=$LOG_DIR/kern.log

mkdir -p "$SYSTEM_LOGS_DIR"

if dmesg -T > "$SYSTEM_LOGS_DIR/dmesg.log" 2> /dev/null; then
    chmod 0644 "$SYSTEM_LOGS_DIR/dmesg.log"
fi

if [ -r /var/log/kern.log ]; then
    install -m 0644 /var/log/kern.log "$KERN_LOG_PATH"
fi

if [ -r /var/log/syslog ]; then
    install -m 0644 /var/log/syslog "$SYSTEM_LOGS_DIR/syslog.log"
fi

for atop_log in /var/log/atop/atop_*; do
    if [ -r "$atop_log" ]; then
        install -m 0644 "$atop_log" "$SYSTEM_LOGS_DIR/$(basename "$atop_log")"
    fi
done
