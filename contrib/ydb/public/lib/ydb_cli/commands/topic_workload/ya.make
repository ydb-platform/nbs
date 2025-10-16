LIBRARY(topic_workload)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    topic_workload_clean.cpp
    topic_workload_describe.cpp
    topic_workload_init.cpp
    topic_workload_params.cpp
    topic_workload_run_read.cpp
    topic_workload_run_write.cpp
    topic_workload_run_full.cpp
    topic_workload_stats.cpp
    topic_workload_stats_collector.cpp
    topic_workload_writer.cpp
    topic_workload_reader.cpp
    topic_workload_reader_transaction_support.cpp
    topic_workload.cpp
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

RECURSE_FOR_TESTS(
    ut
)
