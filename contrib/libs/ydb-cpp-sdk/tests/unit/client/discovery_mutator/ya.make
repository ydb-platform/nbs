UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/extension_common
    contrib/libs/ydb-cpp-sdk/src/client/table
)

SRCS(
    discovery_mutator_ut.cpp
)

END()
