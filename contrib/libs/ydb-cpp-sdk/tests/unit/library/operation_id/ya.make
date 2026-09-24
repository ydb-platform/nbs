UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    operation_id_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
)

END()
