IF (OS_LINUX AND NOT WITH_VALGRIND)
    UNITTEST()

    SIZE(LARGE)

    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

    IF (BUILD_TYPE != "DEBUG")
        SRCS(
            main.cpp
        )
    ELSE ()
        MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
    ENDIF ()

    PEERDIR(
        contrib/ydb/apps/version
        contrib/ydb/core/blobstorage
        contrib/ydb/core/blobstorage/ut_pdiskfit/lib
    )

    END()
ENDIF()
