LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    library/cpp/http/simple
    library/cpp/json
    library/cpp/threading/atomic
    contrib/ydb/public/api/client/yc_public/iam
    contrib/ydb/public/lib/jwt
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()

