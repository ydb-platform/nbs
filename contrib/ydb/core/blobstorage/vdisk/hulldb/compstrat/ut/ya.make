UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/hulldb/compstrat)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/blobstorage/vdisk/hulldb/test
)

SRCS(
    hulldb_compstrat_ut.cpp
)

END()
