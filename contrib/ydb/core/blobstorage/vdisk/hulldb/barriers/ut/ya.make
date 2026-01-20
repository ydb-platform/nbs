UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/barriers)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/vdisk/common
)

SRCS(
    barriers_tree_ut.cpp
)

END()
