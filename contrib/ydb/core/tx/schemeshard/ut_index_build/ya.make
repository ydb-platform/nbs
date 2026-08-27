UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/metering
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_schemeshard_build_index_helpers.cpp
    ut_index_build.cpp
    ut_vector_index_build.cpp
)

END()
