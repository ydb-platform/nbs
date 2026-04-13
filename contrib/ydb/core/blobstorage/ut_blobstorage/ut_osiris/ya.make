UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

SRCS(
    osiris.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

END()
