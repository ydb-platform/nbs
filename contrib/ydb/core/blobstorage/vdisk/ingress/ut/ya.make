UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/ingress)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/erasure
)

SRCS(
    blobstorage_ingress_matrix_ut.cpp
    blobstorage_ingress_ut.cpp
)

END()
