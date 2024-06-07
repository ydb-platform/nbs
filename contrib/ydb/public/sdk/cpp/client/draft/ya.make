LIBRARY()

SRCS(
    ydb_dynamic_config.cpp
    ydb_scripting.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
