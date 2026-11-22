#!/bin/bash

# Soft-RoCE cleanup script for systemd
# This script is called by systemd service on shutdown

set -e

LOG_FILE="${SOFT_ROCE_LOG_FILE:-/var/log/soft-roce.log}"
CONFIG_DIR="${SOFT_ROCE_CONFIG_DIR:-/etc/soft-roce}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

if [[ ! -f "$CONFIG_DIR/config.json" ]]; then
    log "Error: Configuration file not found: $CONFIG_DIR/config.json"
    exit 1
fi

DEVICE_NAME=$(jq -r '.device_name' "$CONFIG_DIR/config.json")

log "Starting Soft-RoCE cleanup..."

if rdma link show | grep -q "^link $DEVICE_NAME/"; then
    rdma link delete "$DEVICE_NAME" || log "Failed to delete $DEVICE_NAME"
else
    log "$DEVICE_NAME does not exist"
fi

log "Soft-RoCE cleanup completed"
