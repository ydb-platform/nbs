UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    SRCS(
        vdisk_restart.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
