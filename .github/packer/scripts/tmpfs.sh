#!/bin/bash

set -e

# Default values if not set
TMPFS_SIZE=${TMPFS_SIZE:-0}
TMPFS_MOUNT_POINT=${TMPFS_MOUNT_POINT:-/mnt/ya_make_tmpfs}

echo "Setup tmpfs"
echo "Requested size: ${TMPFS_SIZE}%"
echo "Target path: ${TMPFS_MOUNT_POINT}"

# Check if disabled
if [[ "${TMPFS_SIZE}" -le 0 ]]; then
    echo "Tmpfs disabled (size <= 0)"
    exit 0
fi

# Check if already mounted (not just if directory exists)
if mountpoint -q "${TMPFS_MOUNT_POINT}"; then
    echo "Tmpfs already mounted at ${TMPFS_MOUNT_POINT}"
    df -h "${TMPFS_MOUNT_POINT}"
    exit 0
fi

# Create mount point if doesn't exist
if [[ ! -d "${TMPFS_MOUNT_POINT}" ]]; then
    echo "Creating directory: ${TMPFS_MOUNT_POINT}"
    sudo mkdir -p "${TMPFS_MOUNT_POINT}"
fi

if ! grep -q "^tmpfs ${TMPFS_MOUNT_POINT}" /etc/fstab; then
    echo "tmpfs ${TMPFS_MOUNT_POINT} tmpfs defaults,noatime,nodiratime,mode=1777,size=${TMPFS_SIZE} 0 0" \
                                                                                                         | sudo tee -a /etc/fstab > /dev/null
    echo "Added to /etc/fstab"
fi

echo "Tmpfs setup completed"
