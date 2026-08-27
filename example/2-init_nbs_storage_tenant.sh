#!/usr/bin/env bash

# Initializes only the NBS tenant on an already-initialized ydbd storage node
# (e.g. the one from cloud/filestore/bin running on the same host). Use this
# instead of 2-init_storage.sh when the storage node is shared with another
# setup: DefineBox / DefineStoragePools / BindRootStorage / Configure-Root are
# deliberately not executed here - they describe this example's own ydbd
# instance (pdisk paths, static group) and have already been applied by the
# owner of the running storage node.
#
# Requirements for the running storage node:
#   - grpc on localhost:$GRPC_PORT, domain Root, ClusterUUID "local"
#   - storage pools of kinds "ssd" and "rot" defined (the filestore setup
#     defines the same pools as this example)

DATA_DIR="data"
source ./prepare_binaries.sh || exit 1

GRPC_PORT=${GRPC_PORT:-9001}

set -e

# CreateTenant is asynchronous: the console immediately replies with an
# unfinished operation, which the ydbd CLI prints as
# "ERROR: STATUS_CODE_UNSPECIFIED ()" even though the tenant is being
# created. Tolerate that reply (and ALREADY_EXISTS on reruns), then poll
# until the tenant path appears.
echo "CreateTenant"
ydbd -s grpc://localhost:9001 admin database /Root/NBS create --no-tx ssd:8 rot:2 || true

echo "Waiting for /Root/NBS"
created=0
for _ in $(seq 1 30); do
    if ydbd -s grpc://localhost:$GRPC_PORT db schema ls /Root 2>/dev/null | grep -qw NBS; then
        created=1
        break
    fi
    sleep 1
done
if [[ "$created" != "1" ]]; then
    echo "ERROR: /Root/NBS did not appear in 30s"
    exit 1
fi

ALLOW_NAMED_CONFIGS_REQ="
ConfigsConfig {
    UsageScopeRestrictions {
        AllowedTenantUsageScopeKinds: 100
        AllowedHostUsageScopeKinds:   100
        AllowedNodeTypeUsageScopeKinds: 100
    }
}
"

echo "AllowNamedConfigs"
ydbd -s grpc://localhost:$GRPC_PORT admin console config set --merge "$ALLOW_NAMED_CONFIGS_REQ"
echo "SetUserAttributes(set unlimited for nonrepl disks)"
ydbd -s grpc://localhost:$GRPC_PORT db schema user-attribute set /Root/NBS __volume_space_limit_ssd_nonrepl=$(( 999 * 1024**5 ))
