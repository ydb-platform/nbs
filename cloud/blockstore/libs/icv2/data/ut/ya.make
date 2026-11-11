UNITTEST_FOR(cloud/blockstore/libs/icv2/data)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    endpoint_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/tcp
    cloud/storage/core/libs/icv2/transport/testlib
)

END()
