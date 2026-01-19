#!/bin/sh
set -ex

# Wait until the sidecar produced a non-empty authorized_keys
while [ ! -s /github-keys/authorized_keys ]; do
    echo "Waiting for GitHub keys..."
    sleep 2
done

sed -i 's/^AllowTcpForwarding.*/AllowTcpForwarding yes/' /config/sshd/sshd_config

exec /init
