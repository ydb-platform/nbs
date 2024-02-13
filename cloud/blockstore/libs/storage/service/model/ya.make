LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    ping_metrics.cpp
)

PEERDIR(
)

END()

RECURSE_FOR_TESTS(ut)
