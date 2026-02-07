LIBRARY()

SRCS(
    workload_factory.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/accessor
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_query
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()
