LIBRARY()

SRCS(
    grpc_request.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/grpc_services/base
    contrib/ydb/library/ydb_issue
    contrib/ydb/public/api/protos
)

END()
