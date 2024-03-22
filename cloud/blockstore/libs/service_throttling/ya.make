LIBRARY()

SRCS(
    throttler_logger.cpp
    throttler_policy.cpp
    throttler_tracker.cpp
    throttling.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/libs/throttling

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/throttling

    library/cpp/monlib/dynamic_counters
)

END()

# Until DEVTOOLSSUPPORT-25698 is not solved.
IF (SANITIZER_TYPE == "address" OR SANITIZER_TYPE == "memory")
    RECURSE_FOR_TESTS(
        ut
        ut_metrics
        ut_policy
    )
ELSE()
    RECURSE_FOR_TESTS(
        ut
        ut_logger
        ut_metrics
        ut_policy
    )
ENDIF()
