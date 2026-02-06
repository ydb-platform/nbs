UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    operation_id_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

END()
