LIBRARY(transfer_workload)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    transfer_workload.cpp
    transfer_workload_topic_to_table.cpp
    transfer_workload_topic_to_table_init.cpp
    transfer_workload_topic_to_table_clean.cpp
    transfer_workload_topic_to_table_run.cpp
    transfer_workload_defines.cpp
)

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/protos/annotations
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    contrib/ydb/public/sdk/cpp/src/client/types/status    
)

END()
