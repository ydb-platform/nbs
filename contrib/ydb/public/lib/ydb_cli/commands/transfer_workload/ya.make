LIBRARY(transfer_workload)

SRCS(
    transfer_workload.cpp
    transfer_workload_topic_to_table.cpp
    transfer_workload_topic_to_table_init.cpp
    transfer_workload_topic_to_table_clean.cpp
    transfer_workload_topic_to_table_run.cpp
    transfer_workload_defines.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/protos/annotations
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
    contrib/ydb/public/sdk/cpp/client/ydb_types/status    
)

END()
