UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/synclog)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
)

SRCS(
    blobstorage_synclogdata_ut.cpp
    blobstorage_synclogdsk_ut.cpp
    blobstorage_synclogkeeper_ut.cpp
    blobstorage_synclogmem_ut.cpp
    blobstorage_synclogmsgimpl_ut.cpp
    blobstorage_synclogmsgwriter_ut.cpp
    codecs_ut.cpp
)

END()
