UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/basics/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/v1_dummy
)

SRCS(
    ut_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
