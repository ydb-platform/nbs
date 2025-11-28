UNITTEST_FOR(contrib/ydb/services/datastreams)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/library/grpc/client
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/services/datastreams

    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:11)

END()
