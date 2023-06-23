LIBRARY()

SRCS(
    throttler.cpp
    throttler_logger.cpp
    throttler_logger_test.cpp
    throttler_metrics.cpp
    throttler_metrics_test.cpp
    throttler_policy.cpp
    throttler_tracker.cpp
    throttler_tracker_test.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/throttling
    cloud/storage/core/protos

    library/cpp/deprecated/atomic
    library/cpp/monlib/dynamic_counters
)

END()

RECURSE_FOR_TESTS(ut)
