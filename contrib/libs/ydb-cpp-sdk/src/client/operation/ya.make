LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    operation.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
    contrib/libs/ydb-cpp-sdk/src/client/query
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/export
    contrib/libs/ydb-cpp-sdk/src/client/import
    contrib/libs/ydb-cpp-sdk/src/client/ss_tasks
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
)

END()
