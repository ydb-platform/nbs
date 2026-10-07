UNITTEST_FOR(cloud/blockstore/libs/vhost)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    server_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/service_local
)

END()
