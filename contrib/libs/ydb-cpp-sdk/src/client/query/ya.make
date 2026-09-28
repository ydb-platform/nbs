LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client.cpp
    query.cpp
    stats.cpp
    tx.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/kqp_session_common
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/session_pool
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/retry
    contrib/libs/ydb-cpp-sdk/src/client/common_client
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/query/impl
    contrib/libs/ydb-cpp-sdk/src/client/result
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
)

END()
