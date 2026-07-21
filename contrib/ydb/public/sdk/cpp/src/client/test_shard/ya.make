LIBRARY()

SRCS(
    test_shard.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/api/grpc
)

END()
