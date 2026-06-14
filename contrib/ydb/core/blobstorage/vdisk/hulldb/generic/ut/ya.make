UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/generic)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    hullds_sst_it_all_ut.cpp
    blobstorage_hullwritesst_ut.cpp
)

END()
