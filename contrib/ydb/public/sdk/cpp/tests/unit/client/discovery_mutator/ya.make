UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/extension_common
    contrib/ydb/public/sdk/cpp/src/client/table
)

SRCS(
    discovery_mutator_ut.cpp
)

END()
