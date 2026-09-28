UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    endpoints_ut.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_endpoints
)

END()
