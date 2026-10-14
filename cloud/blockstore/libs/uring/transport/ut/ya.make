UNITTEST_FOR(cloud/blockstore/libs/uring/transport)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    config_ut.cpp
    protocol_ut.cpp
    socket_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
