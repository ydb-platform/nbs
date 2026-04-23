UNITTEST_FOR(contrib/ydb/core/blobstorage/backpressure)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/dsproxy/mock
)

SRCS(
    queue_backpressure_client_ut.cpp
    queue_backpressure_server_ut.cpp
)

END()
