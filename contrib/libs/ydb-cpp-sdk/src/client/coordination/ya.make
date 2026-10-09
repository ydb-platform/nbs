LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    coordination.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/coordination/coordination.h)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/common_client
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/proto
    contrib/libs/ydb-cpp-sdk/src/client/types
    contrib/libs/ydb-cpp-sdk/src/client/types/status
)

END()
