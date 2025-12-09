LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_value
    yql/essentials/public/issue/protos
)

END()
