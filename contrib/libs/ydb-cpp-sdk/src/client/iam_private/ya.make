LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/iam
    contrib/libs/ydb-cpp-sdk/src/client/iam_private/common
)

END()
