UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    IF (SANITIZER_TYPE)
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    SRCS(
        comp_defrag.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
