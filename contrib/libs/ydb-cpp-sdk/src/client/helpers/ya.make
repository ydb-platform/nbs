LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/iam
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials/oauth2_token_exchange
)

END()
