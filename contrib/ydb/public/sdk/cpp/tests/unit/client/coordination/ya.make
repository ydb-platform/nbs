UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    util
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/coordination
)

SRCS(
    coordination_ut.cpp
    distributed_lock_ut.cpp
)

END()
