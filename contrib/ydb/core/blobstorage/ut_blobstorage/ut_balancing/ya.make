UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    TIMEOUT(600)

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
