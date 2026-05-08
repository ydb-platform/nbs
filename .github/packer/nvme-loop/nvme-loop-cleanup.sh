#!/bin/bash

# Persistent NVMe-loop cleanup script for systemd
# This script is called by systemd service on shutdown

set -e

LOG_FILE="${NVME_LOOP_LOG_FILE:-/var/log/nvme-loop.log}"
CONFIG_DIR="${NVME_LOOP_CONFIG_DIR:-/etc/nvme-loop}"
DEVICES_DIR="${NVME_LOOP_DEVICES_DIR:-/opt/nvme-loop/devices}"
IMAGES_DIR="${NVME_LOOP_IMAGES_DIR:-/opt/nvme-loop/images}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

if [[ ! -f "$CONFIG_DIR/config.json" ]]; then
    log "Error: Configuration file not found: $CONFIG_DIR/config.json"
    exit 1
fi

TARGET_NAME=$(jq -r '.target_name' "$CONFIG_DIR/config.json")
PORT_ID=$(jq -r '.port_id' "$CONFIG_DIR/config.json")

log "Starting NVMe-loop cleanup..."

# Disconnect all NVMe-fabrics connections
log "Disconnecting NVMe-fabrics connections..."
if command -v nvme > /dev/null 2>&1; then
    nvme disconnect-all 2> /dev/null || log "nvme disconnect-all failed or no connections"
fi

# Clean up configfs
log "Cleaning up configfs..."
if [[ -d /sys/kernel/config/nvmet ]]; then
    # Remove subsystem links from ports
    for link in /sys/kernel/config/nvmet/ports/"$PORT_ID"/subsystems/*; do
        if [[ -L $link ]]; then
            log "Removing link: $link"
            rm -f "$link" || true
        fi
    done

    # Disable namespaces
    for ns in /sys/kernel/config/nvmet/subsystems/"$TARGET_NAME"/namespaces/*/enable; do
        if [[ -f $ns ]]; then
            log "Disabling namespace: $(dirname $ns)"
            echo 0 > "$ns" 2> /dev/null || true
        fi
    done

    # Remove namespaces
    for ns in /sys/kernel/config/nvmet/subsystems/"$TARGET_NAME"/namespaces/*; do
        if [[ -d $ns ]]; then
            log "Removing namespace: $ns"
            rmdir "$ns" 2> /dev/null || true
        fi
    done

    rmdir "/sys/kernel/config/nvmet/subsystems/$TARGET_NAME" 2> /dev/null || true
    rmdir "/sys/kernel/config/nvmet/ports/$PORT_ID" 2> /dev/null || true
fi

log "Detaching loop devices..."
if [[ -d $IMAGES_DIR ]]; then
    for img in "$IMAGES_DIR"/*.img; do
        if [[ -f $img ]]; then
            LOOP_DEVICE=$(losetup -j "$img" | cut -d: -f1)
            if [[ -n $LOOP_DEVICE ]]; then
                log "Detaching: $LOOP_DEVICE"
                losetup -d "$LOOP_DEVICE" 2> /dev/null || true
            fi
        fi
    done
fi

rm ${DEVICES_DIR}/* 2> /dev/null || true

log "NVMe-loop cleanup completed"

exit 0
