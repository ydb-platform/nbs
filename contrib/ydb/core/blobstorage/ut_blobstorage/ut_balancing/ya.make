UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
