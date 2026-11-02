UNITTEST_FOR(cloud/blockstore/libs/icv2/transport)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    handshake_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/icv2/transport/protos
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/tcp
)

END()
