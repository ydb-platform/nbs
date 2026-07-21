GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    metric_buffer_ut.cpp
    metrics_ut.cpp
    spans_ut.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/testing/gtest
    contrib/ydb/public/sdk/cpp/src/client/impl/observability
    contrib/ydb/public/sdk/cpp/src/client/impl/stats
    contrib/ydb/public/sdk/cpp/src/client/metrics
    contrib/ydb/public/sdk/cpp/src/client/query/impl
    contrib/ydb/public/sdk/cpp/src/client/table/impl
)

END()
