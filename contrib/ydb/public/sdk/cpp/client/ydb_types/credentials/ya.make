LIBRARY()

SRCS(
    credentials.cpp
)

PEERDIR(
    contrib/ydb/library/login
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
    yql/essentials/public/issue
)

END()

RECURSE(
    oauth2_token_exchange
)
