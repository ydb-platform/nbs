LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    task.cpp
    out.cpp
)

GENERATE_ENUM_SERIALIZATION(task.h)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/proto
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
)

END()
