LIBRARY(client-iam-common-include)

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    types.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/jwt
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials
)

END()
