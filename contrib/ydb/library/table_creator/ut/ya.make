UNITTEST_FOR(contrib/ydb/library/table_creator)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    table_creator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/src/client/driver
)

YQL_LAST_ABI_VERSION()

END()
