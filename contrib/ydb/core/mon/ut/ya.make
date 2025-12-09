UNITTEST_FOR(contrib/ydb/core/mon)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/mon
    contrib/ydb/core/mon/ut_utils
    contrib/ydb/core/testlib/default
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
)

SRCS(
    mon_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
