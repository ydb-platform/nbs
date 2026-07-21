UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_analyze_ut.cpp
    kqp_explain_ut.cpp
    kqp_limits_ut.cpp
    kqp_params_ut.cpp
    kqp_query_ut.cpp
    kqp_query_event_log_ut.cpp
    kqp_stats_ut.cpp
    kqp_types_ut.cpp
)

PEERDIR(
    contrib/ydb/core/statistics/ut_common
    contrib/ydb/library/yql/udfs/statistics_internal
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/hyperloglog
)

YQL_LAST_ABI_VERSION()

END()
