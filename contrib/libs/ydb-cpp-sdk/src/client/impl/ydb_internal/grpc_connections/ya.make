LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/db_driver_state
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/plain_status
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/thread_pool
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_stats
    contrib/libs/ydb-cpp-sdk/src/client/resources
    contrib/libs/ydb-cpp-sdk/src/client/types/exceptions
)

END()
