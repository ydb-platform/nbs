LIBRARY()

SRCS(
    stock_workload.cpp
    kv_workload.cpp
    workload_factory.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

END()
