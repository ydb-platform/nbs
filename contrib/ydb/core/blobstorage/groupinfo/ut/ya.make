UNITTEST_FOR(contrib/ydb/core/blobstorage/groupinfo)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/erasure
)

SRCS(
    blobstorage_groupinfo_iter_ut.cpp
    blobstorage_groupinfo_ut.cpp
)

IF (BUILD_TYPE != "DEBUG")
    SRCS(
        blobstorage_groupinfo_blobmap_ut.cpp
        blobstorage_groupinfo_partlayout_ut.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
