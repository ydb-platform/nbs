UNITTEST_FOR(cloud/blockstore/libs/service_throttling)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    throttler_policy_ut.cpp
)

PEERDIR(
)

END()
