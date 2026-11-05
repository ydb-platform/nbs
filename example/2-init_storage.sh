#!/usr/bin/env bash
set -e

tenant_only=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tenant)
            tenant_only=true
            ;;
        -h | --help)
            cat <<EOF
Usage: ./2-init_storage.sh [--tenant]

Initialize storage pools and the NBS tenant on the local YDB cluster.
--tenant  Initialize only the NBS tenant on an already-configured YDB.
          Requires domain Root, ClusterUUID local, and ssd/rot storage pools.
EOF
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
    shift
done

source ./prepare_binaries.sh
GRPC_PORT=${GRPC_PORT:-9001}

if [[ "$tenant_only" == false ]]; then
    echo "DefineBox"
    ydbd -s grpc://localhost:$GRPC_PORT admin bs config invoke --proto-file dynamic/DefineBox.txt
    echo "DefineStoragePools"
    ydbd -s grpc://localhost:$GRPC_PORT admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
    echo "BindRootStorageRequest-Root"
    ydbd -s grpc://localhost:$GRPC_PORT db schema execute dynamic/BindRootStorageRequest-Root.txt
fi

# CreateTenant is asynchronous: the console immediately replies with an
# unfinished operation, which the ydbd CLI prints as
# "ERROR: STATUS_CODE_UNSPECIFIED ()" even though the tenant is being
# created. Tolerate that reply (and ALREADY_EXISTS on reruns), then poll
# until the tenant path appears.
echo "CreateTenant"
ydbd -s grpc://localhost:$GRPC_PORT admin database /Root/NBS create --no-tx ssd:8 rot:2 || true

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
