UNITTEST_FOR(cloud/blockstore/libs/throttling)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    throttler_metrics_ut.cpp
    throttler_ut.cpp
)

PEERDIR(
    library/cpp/threading/future/subscription
)

END()
