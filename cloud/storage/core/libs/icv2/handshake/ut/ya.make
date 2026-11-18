UNITTEST_FOR(cloud/storage/core/libs/icv2/handshake)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    handshake_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/handshake/protos
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/tcp
)

END()
