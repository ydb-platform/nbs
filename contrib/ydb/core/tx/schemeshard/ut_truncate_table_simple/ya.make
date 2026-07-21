UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_truncate_table_simple.cpp
)

YQL_LAST_ABI_VERSION()

END()
