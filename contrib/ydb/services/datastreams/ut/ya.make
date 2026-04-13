UNITTEST_FOR(contrib/ydb/services/datastreams)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/services/datastreams

    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
