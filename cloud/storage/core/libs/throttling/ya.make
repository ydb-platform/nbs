LIBRARY()

SRCS(
    helpers.cpp
    leaky_bucket.cpp
    tablet_throttler_logger.cpp
    tablet_throttler_policy.cpp
    tablet_throttler.cpp
    unspent_cost_bucket.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    bench
    ut
)
