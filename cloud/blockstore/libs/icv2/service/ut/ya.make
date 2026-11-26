UNITTEST_FOR(cloud/blockstore/libs/icv2/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    host_endpoint_network_ut.cpp
    host_endpoint_ut.cpp
    service_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/diagnostics
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/transport/testlib
)

END()
