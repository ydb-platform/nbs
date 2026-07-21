UNITTEST_FOR(contrib/ydb/library/yql/dq/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    dq_channel_service_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
