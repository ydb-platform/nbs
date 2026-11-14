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
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/coordination
)

SRCS(
    coordination_ut.cpp
)

END()
