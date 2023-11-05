UNITTEST_FOR(contrib/ydb/public/lib/deprecated/kicli)

TIMEOUT(600)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/core/client
    contrib/ydb/core/testlib/default
    contrib/ydb/public/lib/deprecated/kicli
)

YQL_LAST_ABI_VERSION()

SRCS(
    cpp_ut.cpp
)

END()
