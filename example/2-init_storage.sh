#!/usr/bin/env bash

DATA_DIR="data"
source ./prepare_binaries.sh || exit 1

echo "DefineBox"
ydbd -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineBox.txt
echo "DefineStoragePools"
ydbd -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
echo "BindRootStorageRequest-Root"
ydbd -s grpc://localhost:9001 db schema execute dynamic/BindRootStorageRequest-Root.txt
echo "CreateTenant"
ydbd -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/CreateTenant.txt
echo "Configure-Root"
ydbd -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/Configure-Root.txt

GRPC_PORT=${GRPC_PORT:-9001}

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
echo "Set Console Config"
ydb -e grpc://localhost:$GRPC_PORT -d /Root admin config replace -f ydb-console/config.yaml --allow-unknown-fields
