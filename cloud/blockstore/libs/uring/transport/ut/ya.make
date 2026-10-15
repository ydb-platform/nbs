UNITTEST_FOR(cloud/blockstore/libs/uring/transport)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    client_ut.cpp
    config_ut.cpp
    connection_ut.cpp
    protocol_ut.cpp
    ring_ut.cpp
    server_ut.cpp
    socket_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
