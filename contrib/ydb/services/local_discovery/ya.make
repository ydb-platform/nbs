LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind
    contrib/ydb/public/api/grpc
    contrib/ydb/public/lib/operation_id
)

END()
