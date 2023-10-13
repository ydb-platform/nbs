UNITTEST_FOR(cloud/blockstore/apps/client/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    command_ut.cpp
)

PEERDIR(
    cloud/blockstore/apps/client/lib
)

END()
