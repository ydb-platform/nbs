LIBRARY()

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/impl/ydb_endpoints
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
    yql/essentials/public/issue/protos
)

END()
