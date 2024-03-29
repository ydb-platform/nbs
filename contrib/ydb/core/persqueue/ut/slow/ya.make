UNITTEST_FOR(contrib/ydb/core/persqueue)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)
TIMEOUT(600)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    pq_ut.cpp
)

END()
