UNITTEST_FOR(cloud/blockstore/apps/client/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    command_ut.cpp
)

PEERDIR(
    cloud/blockstore/apps/client/lib
    cloud/storage/core/libs/iam/iface
)

END()
