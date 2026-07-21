LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
    contrib/ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
)

END()
