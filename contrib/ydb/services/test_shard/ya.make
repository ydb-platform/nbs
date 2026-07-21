LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/core/grpc_services
    contrib/ydb/core/test_tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
