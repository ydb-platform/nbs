UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/syncer)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/apps/version
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
)

SRCS(
    blobstorage_syncer_data_ut.cpp
    blobstorage_syncer_localwriter_ut.cpp
    blobstorage_syncquorum_ut.cpp
)

END()
