UNITTEST_FOR(cloud/blockstore/libs/nbd)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    client_handler_ut.cpp
    server_handler_ut.cpp
    server_ut.cpp
)

PEERDIR(
)

END()
