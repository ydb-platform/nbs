UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/blobstorage/storagepoolmon
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
)

SRCS(
    storagepoolmon_ut.cpp
)

END()
