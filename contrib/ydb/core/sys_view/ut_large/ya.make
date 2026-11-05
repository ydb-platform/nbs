UNITTEST_FOR(contrib/ydb/core/sys_view)

FORK_SUBTESTS()

SIZE(LARGE)
TAG(ya:fat)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    library/cpp/testing/unittest
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/pg
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_large.cpp
    ut_common.cpp
)

END()
