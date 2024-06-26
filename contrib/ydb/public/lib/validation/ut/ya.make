UNITTEST_FOR(contrib/ydb/public/lib/validation)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/public/lib/validation/ut/protos
)

SRCS(
    ut.cpp
)

END()
