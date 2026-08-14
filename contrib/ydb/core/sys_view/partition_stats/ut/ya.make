UNITTEST_FOR(contrib/ydb/core/sys_view/partition_stats)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    partition_stats_ut.cpp
)

END()
