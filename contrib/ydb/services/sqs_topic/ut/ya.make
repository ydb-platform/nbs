UNITTEST()

SRCS(
    utils_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/services/sqs_topic
)

END()
