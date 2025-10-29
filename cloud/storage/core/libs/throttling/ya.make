LIBRARY()

SRCS(
    helpers.cpp
    leaky_bucket.cpp
    tablet_throttler.cpp
    tablet_throttler_logger.cpp
    tablet_throttler_policy.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
