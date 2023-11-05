LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/core/grpc_services
    contrib/ydb/public/api/grpc
    library/cpp/actors/core
    library/cpp/grpc/server
)

END()
