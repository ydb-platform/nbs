GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    grpc_iam_ut.cpp
    http_iam_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/common
    contrib/ydb/public/api/client/yc_public/iam
    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/sdk/cpp/src/client/types/core_facility
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/public/sdk/cpp/src/library/issue
    contrib/ydb/public/sdk/cpp/src/library/time
    contrib/ydb/public/sdk/cpp/tests/common/iam_mocks
)

END()
