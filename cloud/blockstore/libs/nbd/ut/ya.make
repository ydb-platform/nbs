UNITTEST_FOR(cloud/blockstore/libs/nbd)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    client_handler_ut.cpp
    server_handler_ut.cpp
    server_ut.cpp
    utils_ut.cpp
)

PEERDIR(
)

END()
