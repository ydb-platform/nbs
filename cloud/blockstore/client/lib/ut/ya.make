UNITTEST_FOR(cloud/blockstore/client/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    command_ut.cpp
)

PEERDIR(
    cloud/blockstore/client/lib
)

END()
