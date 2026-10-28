UNITTEST_FOR(cloud/blockstore/libs/icv2/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    host_endpoint_ut.cpp
    peer_connections_ut.cpp
    ping_client_ut.cpp
    service_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/tcp/testlib
)

END()
