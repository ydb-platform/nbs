UNITTEST_FOR(contrib/ydb/core/statistics/database)

FORK_SUBTESTS()

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/statistics/ut_common
)

SRCS(
    ut_database.cpp
)

END()
