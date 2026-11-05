UNITTEST_FOR(contrib/ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    contrib/ydb/core/testlib/pg
    yql/essentials/public/udf/service/exception_policy
    library/cpp/testing/unittest
    yql/essentials/sql/v1_dummy
)

SRCS(
    json_change_record_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
