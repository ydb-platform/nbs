LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/grpc_connections
)

END()
