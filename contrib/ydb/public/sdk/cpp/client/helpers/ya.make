LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/iam/common
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange
    contrib/ydb/library/yql/public/issue/protos
)

END()
