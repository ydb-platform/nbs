LIBRARY()

SRCS(
    util.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
)

END()
