#!/bin/bash

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

# Load configuration variables

TARGET_NAME=$(jq -r '.target_name' "$CONFIG_DIR/config.json")
NUM_NAMESPACES=$(jq -r '.num_namespaces' "$CONFIG_DIR/config.json")
DISK_SIZE=$(jq -r '.disk_size' "$CONFIG_DIR/config.json")
PORT_ID=$(jq -r '.port_id' "$CONFIG_DIR/config.json")
AUTO_CONNECT=$(jq -r '.auto_connect' "$CONFIG_DIR/config.json")
USER_NAME=$(jq -r '.user_name | values // ""' "$CONFIG_DIR/config.json")
IMAGE_PREFIX="nvme-disk"

log "Configuration loaded: target=$TARGET_NAME, namespaces=$NUM_NAMESPACES, size=$DISK_SIZE"

log ""
log "=== Setting up single NVMe-loop target with $NUM_NAMESPACES namespaces ==="

mkdir -p $DEVICES_DIR
chmod a+rw $DEVICES_DIR

mkdir -p $IMAGES_DIR

log "Loading kernel modules..."
modprobe nvme-loop || {
    log "Failed to load nvme-loop"
    exit 1
}
modprobe nvme-fabrics || {
    log "Failed to load nvme-fabrics"
    exit 1
}
modprobe nvmet || {
    log "Failed to load nvmet"
    exit 1
}

if ! lsmod | grep -q nvme_loop; then
    log "Error: nvme-loop module not loaded"
    exit 1
fi

log "Creating disk images and loop devices..."

LOOP_DEVICES=()

for i in $(seq 1 $NUM_NAMESPACES); do
    IMAGE_FILE="$IMAGES_DIR/${IMAGE_PREFIX}${i}.img"

    log "Processing namespace $i..."

    # Create disk image file if it doesn't exist
    log "  Creating image $IMAGE_FILE with size $DISK_SIZE..."
    fallocate -l "$DISK_SIZE" "$IMAGE_FILE"

    # Create loop device
    LOOP_DEVICE=$(losetup --find --show "$IMAGE_FILE")
    LOOP_DEVICES+=("$LOOP_DEVICE")
    log "Created loop device: $LOOP_DEVICE for $IMAGE_FILE"
done

log "Setting up NVMe target '$TARGET_NAME' with $NUM_NAMESPACES namespaces..."

# Create subsystem
SUBSYS_PATH="/sys/kernel/config/nvmet/subsystems/$TARGET_NAME"
log "Creating subsystem: $SUBSYS_PATH"
mkdir -p "$SUBSYS_PATH"

# Configure subsystem
log "Configuring subsystem..."
echo 1 > "$SUBSYS_PATH/attr_allow_any_host"

# Create namespaces
for i in $(seq 1 $NUM_NAMESPACES); do
    log "Creating namespace $i..."

    NS_PATH="$SUBSYS_PATH/namespaces/$i"
    mkdir -p "$NS_PATH"

    # Bind loop device to namespace
    LOOP_DEVICE="${LOOP_DEVICES[$((i - 1))]}"
    log "  Binding $LOOP_DEVICE to namespace $i..."
    echo "$LOOP_DEVICE" > "$NS_PATH/device_path"
    echo 1 > "$NS_PATH/enable"

    # Verify namespace
    NS_STATUS=$(cat "$NS_PATH/enable")
    if [[ $NS_STATUS == "1" ]]; then
        log "  ✓ Namespace $i enabled successfully"
    else
        log "  ✗ Warning: Namespace $i not enabled"
    fi
done

# Create port
PORT_PATH="/sys/kernel/config/nvmet/ports/$PORT_ID"
log "Creating port: $PORT_PATH"
mkdir -p "$PORT_PATH"
echo loop > "$PORT_PATH/addr_trtype"

# Link subsystem to port
LINK_PATH="$PORT_PATH/subsystems/$TARGET_NAME"
log "Linking subsystem to port..."
ln -sf "$SUBSYS_PATH" "$LINK_PATH"

# Verify port linkage
if [[ -L $LINK_PATH ]]; then
    log "✓ Port linkage verified"
else
    log "✗ Warning: Port linkage failed"
fi

# Wait for kernel to process the configuration
sleep 2

log ""
log "=== Configuration Complete ==="
log "Created NVMe-loop target: $TARGET_NAME"
log "Number of namespaces: $NUM_NAMESPACES"
log "Disk images stored in: $IMAGES_DIR"
log ""
log "=== Configuration Summary ==="
log "Subsystem: $TARGET_NAME"
for i in $(seq 1 $NUM_NAMESPACES); do
    NS_PATH="$SUBSYS_PATH/namespaces/$i"
    if [[ -f "$NS_PATH/device_path" ]] && [[ -f "$NS_PATH/enable" ]]; then
        DEVICE_PATH=$(cat "$NS_PATH/device_path")
        NS_ENABLED=$(cat "$NS_PATH/enable")
        log "  Namespace $i: $DEVICE_PATH (enabled: $NS_ENABLED)"
    fi
done

log ""
log "Port $PORT_ID configured for loop transport"

if [[ $AUTO_CONNECT == "true" ]]; then
    log "Connecting to target $TARGET_NAME ..."

    if echo "transport=loop,nqn=$TARGET_NAME" > /dev/nvme-fabrics; then
        log "  Successfully connected to $TARGET_NAME"
    else
        log "  Warning: Failed to connect to $TARGET_NAME"
    fi

    log ""
    log "=== Connection Status ==="

    sleep 2
fi

# Create symbolic links to added devices

NVME_PATH="$(grep -l -H nvme-loop /sys/class/nvme/*/subsysnqn 2> /dev/null)"

if [[ $NVME_PATH != "" ]]; then
    NVME_NAME=$(basename "$(dirname $NVME_PATH)")

    if [[ -e "/dev/$NVME_NAME" ]]; then
        for path in /dev/"$NVME_NAME"n*; do
            ln -s -t $DEVICES_DIR $path
        done
    fi
fi

log "Symbolic links stored in: $DEVICES_DIR"

if [[ "$USER_NAME" != "" ]]; then
    log "Configuring access for '$USER_NAME'..."

    GROUP=disk

    usermod -a -G $GROUP "$USER_NAME"

    chown $USER_NAME:$GROUP "/dev/$NVME_NAME"

    chown $USER_NAME:$GROUP "$DEVICES_DIR"
    chmod 755 "$DEVICES_DIR"

    for link in "$DEVICES_DIR"/*; do
        chown $USER_NAME:$GROUP "$link"

        DEV=$(realpath $link)
        chown $USER_NAME:$GROUP "$DEV"
    done
fi

exit 0
