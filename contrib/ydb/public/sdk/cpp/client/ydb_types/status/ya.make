LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    yql/essentials/public/issue
)

END()
