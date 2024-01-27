LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/deny_ydb_dependency.inc)

SRCS(
    ping_metrics.cpp
)

PEERDIR(
)

END()

RECURSE_FOR_TESTS(ut)
