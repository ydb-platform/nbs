LIBRARY(clicommands)

SRCS(
    interactive/interactive_cli.cpp
    interactive/line_reader.cpp
    benchmark_utils.cpp
    click_bench.cpp
    kv_workload.cpp
    stock_workload.cpp
    topic_operations_scenario.cpp
    topic_read_scenario.cpp
    topic_write_scenario.cpp
    topic_readwrite_scenario.cpp
    tpch.cpp
    tpcc_workload.cpp
    query_workload.cpp
    ydb_sdk_core_access.cpp
    ydb_command.cpp
    ydb_profile.cpp
    ydb_root_common.cpp
    ydb_service_auth.cpp
    ydb_service_discovery.cpp
    ydb_service_export.cpp
    ydb_service_import.cpp
    ydb_service_monitoring.cpp
    ydb_service_operation.cpp
    ydb_service_scheme.cpp
    ydb_service_scripting.cpp
    ydb_service_topic.cpp
    ydb_service_table.cpp
    ydb_tools.cpp
    ydb_workload.cpp
    ydb_yql.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    library/cpp/histogram/hdr
    library/cpp/protobuf/json
    library/cpp/regex/pcre
    library/cpp/threading/local_executor
    contrib/ydb/library/backup
    contrib/ydb/library/workload
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/lib/stat_visualization
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/commands/topic_workload
    contrib/ydb/public/lib/ydb_cli/commands/transfer_workload
    contrib/ydb/public/lib/ydb_cli/dump
    contrib/ydb/public/lib/ydb_cli/import
    contrib/ydb/public/lib/ydb_cli/topic
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/public/sdk/cpp/client/ydb_coordination
    contrib/ydb/public/sdk/cpp/client/ydb_discovery
    contrib/ydb/public/sdk/cpp/client/ydb_export
    contrib/ydb/public/sdk/cpp/client/ydb_import
    contrib/ydb/public/sdk/cpp/client/ydb_monitoring
    contrib/ydb/public/sdk/cpp/client/ydb_operation
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials/login
)

RESOURCE(
    click_bench_queries.sql click_bench_queries.sql
    click_bench_schema.sql click_bench_schema.sql
    tpch_schema.sql tpch_schema.sql
)

END()

RECURSE_FOR_TESTS(
    topic_workload/ut
)
