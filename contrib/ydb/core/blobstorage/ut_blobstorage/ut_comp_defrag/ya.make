UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        FORK_SUBTESTS()
    ENDIF()

    SIZE(MEDIUM)
    TIMEOUT(600)

    SRCS(
        comp_defrag.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
