UNITTEST_FOR(contrib/ydb/library/table_creator)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    table_creator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

YQL_LAST_ABI_VERSION()

END()
