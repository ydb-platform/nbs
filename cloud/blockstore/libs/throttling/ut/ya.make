UNITTEST_FOR(cloud/blockstore/libs/throttling)

SRCS(
    throttler_metrics_ut.cpp
    throttler_ut.cpp
)

PEERDIR(
    library/cpp/threading/future/subscription
)

END()
