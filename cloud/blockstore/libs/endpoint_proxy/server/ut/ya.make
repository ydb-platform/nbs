UNITTEST_FOR(cloud/blockstore/libs/endpoint_proxy/server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    proxy_storage_ut.cpp
    server_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/endpoint_proxy/client
)

END()
