#!/usr/bin/env bash

DATA_DIR="data"
YDBD="./ydbd"
export LD_LIBRARY_PATH=$(dirname $(readlink ydbd))

echo "DefineBox"
$YDBD -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineBox.txt
echo "DefineStoragePools"
$YDBD -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
echo "BindRootStorageRequest-Root"
$YDBD -s grpc://localhost:9001 db schema execute dynamic/BindRootStorageRequest-Root.txt
echo "CreateTenant"
$YDBD -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/CreateTenant.txt
echo "Configure-Root"
$YDBD -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/Configure-Root.txt

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
$YDBD -s grpc://localhost:$GRPC_PORT admin console config set --merge "$ALLOW_NAMED_CONFIGS_REQ"
echo "SetUserAttributes(set unlimited for nonrepl disks)"
$YDBD -s grpc://localhost:$GRPC_PORT db schema user-attribute set /Root/NBS __volume_space_limit_ssd_nonrepl=$(( 999 * 1024**5 ))
