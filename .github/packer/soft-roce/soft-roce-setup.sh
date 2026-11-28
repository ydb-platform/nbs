#!/bin/bash

set -e

LOG_FILE="${SOFT_ROCE_LOG_FILE:-/var/log/soft-roce.log}"
CONFIG_DIR="${SOFT_ROCE_CONFIG_DIR:-/etc/soft-roce}"
NETDEV_WAIT_SECONDS=30

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

if [[ ! -f "$CONFIG_DIR/config.json" ]]; then
    log "Error: Configuration file not found: $CONFIG_DIR/config.json"
    exit 1
fi

# Load configuration variables

DEVICE_NAME=$(jq -r '.device_name' "$CONFIG_DIR/config.json")
NETDEV=$(jq -r '.netdev | values // ""' "$CONFIG_DIR/config.json")

log "Configuration loaded: device=$DEVICE_NAME, netdev=${NETDEV:-<default route>}"

log ""
log "=== Setting up Soft-RoCE device $DEVICE_NAME ==="

log "Loading kernel modules..."
modprobe rdma_rxe || {
    log "Failed to load rdma_rxe"
    exit 1
}

if rdma link show | grep -q "^link $DEVICE_NAME/"; then
    log "$DEVICE_NAME already exists"
else
    # Without a configured network device use the one of the default route
    if [[ "$NETDEV" == "" ]]; then
        for _ in $(seq 1 "$NETDEV_WAIT_SECONDS"); do
            NETDEV=$(ip -o route show default | awk '{print $5; exit}')
            if [[ "$NETDEV" != "" ]]; then
                break
            fi
            sleep 1
        done
    fi

    if [[ "$NETDEV" == "" ]]; then
        log "Error: no network device with a default route"
        exit 1
    fi

    log "Adding $DEVICE_NAME on $NETDEV..."
    rdma link add "$DEVICE_NAME" type rxe netdev "$NETDEV" || {
        log "Failed to add $DEVICE_NAME"
        exit 1
    }
fi

LINK_STATE=$(rdma link show | grep "^link $DEVICE_NAME/")
log "$LINK_STATE"

if [[ "$LINK_STATE" != *"state ACTIVE"* ]]; then
    log "Warning: $DEVICE_NAME is not active"
fi

log "Soft-RoCE setup completed"
