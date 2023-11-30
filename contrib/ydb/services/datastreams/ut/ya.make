UNITTEST_FOR(contrib/ydb/services/datastreams)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/grpc/client
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/services/datastreams
    contrib/ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:11)

END()
