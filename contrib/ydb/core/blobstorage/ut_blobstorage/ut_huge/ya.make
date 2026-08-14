UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SRCS(
    huge.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    REQUIREMENTS(ram:32)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
