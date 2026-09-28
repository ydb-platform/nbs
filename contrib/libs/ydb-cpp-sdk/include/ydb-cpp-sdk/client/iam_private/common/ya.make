LIBRARY(client-iam-private-common-include)

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    types.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/iam/common
)

END()
