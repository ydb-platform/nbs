UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/cache_block)

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
    cache_block_ut.cpp
)

END()
