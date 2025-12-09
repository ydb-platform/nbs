UNITTEST_FOR(contrib/ydb/core/persqueue)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autopartitioning_ut.cpp
    pq_ut.cpp
)

END()
