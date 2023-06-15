#!/usr/bin/env bash

DATA_DIR="data"
YDBD="./ydbd"

$YDBD -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineBox.txt
$YDBD -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
$YDBD -s grpc://localhost:9001 db schema execute dynamic/BindRootStorageRequest-Root.txt
$YDBD -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/CreateTenant-1.txt
$YDBD -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/CreateTenant-2.txt
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

$YDBD -s grpc://localhost:$GRPC_PORT admin console config set --merge "$ALLOW_NAMED_CONFIGS_REQ"
$YDBD -s grpc://localhost:$GRPC_PORT db schema user-attribute set /Root/NBS __volume_space_limit_ssd_nonrepl=1
