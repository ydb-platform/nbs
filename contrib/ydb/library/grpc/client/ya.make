LIBRARY()

SRCS(
    grpc_client_low.h
    grpc_common.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
)

END()
