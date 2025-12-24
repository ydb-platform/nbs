UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/fresh)

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
    fresh_appendix_ut.cpp
    fresh_data_ut.cpp
    fresh_segment_ut.cpp
    snap_vec_ut.cpp
)

END()
