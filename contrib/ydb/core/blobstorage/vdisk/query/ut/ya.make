UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/query)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/huge
    contrib/ydb/core/protos
)

SRCS(
    query_spacetracker_ut.cpp
)

END()
