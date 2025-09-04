UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

TIMEOUT(900)

SIZE(LARGE)
TAG(ya:fat)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    contrib/ydb/core/cms
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/wrappers/ut_helpers
)

SRCS(
    ut_shred_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
