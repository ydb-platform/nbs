UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    phantom_blobs.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
)

END()
