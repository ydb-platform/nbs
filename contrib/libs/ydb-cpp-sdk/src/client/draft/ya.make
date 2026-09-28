LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    ydb_dynamic_config.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
    ydb_view.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/draft/ydb_replication.h)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/issue
    contrib/ydb/public/api/grpc/draft
    contrib/libs/ydb-cpp-sdk/src/client/table
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
    contrib/libs/ydb-cpp-sdk/src/client/value
)

END()
