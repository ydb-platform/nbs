UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/repl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/erasure
)

SRCS(
    blobstorage_hullreplwritesst_ut.cpp
    blobstorage_replrecoverymachine_ut.cpp
)

END()
