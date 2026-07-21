GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    grpc_iam_service_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/testing/common
    contrib/ydb/public/api/client/yc_private/iam
    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/sdk/cpp/src/client/iam_private
    contrib/ydb/public/sdk/cpp/src/client/types/core_facility
    contrib/ydb/public/sdk/cpp/src/library/issue
    contrib/ydb/public/sdk/cpp/src/library/time
    contrib/ydb/public/sdk/cpp/tests/common/iam_mocks
)

END()
