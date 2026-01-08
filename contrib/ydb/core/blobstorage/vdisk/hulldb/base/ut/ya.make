UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/base)

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
    blobstorage_blob_ut.cpp
    blobstorage_hullsatisfactionrank_ut.cpp
    blobstorage_hullstorageratio_ut.cpp
    hullbase_barrier_ut.cpp
    hullds_generic_it_ut.cpp
    hullds_heap_it_ut.cpp
)

END()
