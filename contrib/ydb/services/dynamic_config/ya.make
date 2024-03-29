LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind
    contrib/ydb/library/aclib
    contrib/ydb/public/api/grpc
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/sdk/cpp/client/resources
)

END()

RECURSE_FOR_TESTS(
    ut
)
