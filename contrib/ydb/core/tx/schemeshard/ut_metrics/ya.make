UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

SIZE(SMALL)

SRCS(
    ut_table_metrics_settings.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
