LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    iam.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/iam_private/common
    contrib/libs/ydb-cpp-sdk/src/client/iam/common
)

END()
