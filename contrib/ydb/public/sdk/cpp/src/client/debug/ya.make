LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
)

END()
