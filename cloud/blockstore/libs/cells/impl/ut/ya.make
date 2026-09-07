UNITTEST_FOR(cloud/blockstore/libs/cells/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    cell_manager_ut.cpp
    connection_ut.cpp
    describe_volume_ut.cpp
    endpoint_router_ut.cpp
    host_pool_ut.cpp
    transport_switcher_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/cells/impl
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
    cloud/blockstore/libs/server

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/certs/server_fallback.crt
)

END()
