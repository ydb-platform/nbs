GTEST()

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/blobstorage/base
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/erasure
    contrib/ydb/core/protos
)

SRCS(
    contrib/ydb/core/blobstorage/base/batched_vec_ut.cpp
    contrib/ydb/core/blobstorage/base/bufferwithgaps_ut.cpp
    contrib/ydb/core/blobstorage/base/ptr_ut.cpp
)

END()
