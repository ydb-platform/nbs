IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
        library/cpp/getopt
        yt/cpp/mapreduce/client
        contrib/ydb/library/yql/dq/actors/compute
        contrib/ydb/library/yql/dq/actors/spilling
        contrib/ydb/library/yql/dq/comp_nodes
        contrib/ydb/library/yql/dq/integration/transform
        contrib/ydb/library/yql/dq/transform
        contrib/ydb/library/yql/minikql/comp_nodes/llvm14
        contrib/ydb/library/yql/providers/clickhouse/actors
        contrib/ydb/library/yql/providers/common/comp_nodes
        contrib/ydb/library/yql/providers/dq/runtime
        contrib/ydb/library/yql/providers/dq/service
        contrib/ydb/library/yql/providers/dq/metrics
        contrib/ydb/library/yql/providers/dq/stats_collector
        contrib/ydb/library/yql/providers/dq/task_runner
        contrib/ydb/library/yql/providers/pq/async_io
        contrib/ydb/library/yql/providers/pq/proto
        contrib/ydb/library/yql/providers/s3/actors
        contrib/ydb/library/yql/providers/ydb/actors
        contrib/ydb/library/yql/providers/ydb/comp_nodes
        contrib/ydb/library/yql/public/udf/service/exception_policy
        contrib/ydb/library/yql/utils
        contrib/ydb/library/yql/utils/log
        contrib/ydb/library/yql/utils/log/proto
        contrib/ydb/library/yql/utils/failure_injector
        contrib/ydb/library/yql/utils/backtrace
        contrib/ydb/library/yql/providers/yt/comp_nodes/dq
        contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
        contrib/ydb/library/yql/providers/yt/codec/codegen
        contrib/ydb/library/yql/providers/yt/mkql_dq
        contrib/ydb/library/yql/providers/dq/actors/yt
        contrib/ydb/library/yql/providers/dq/global_worker_manager
        contrib/ydb/library/yql/sql/pg
        contrib/ydb/library/yql/parser/pg_wrapper
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
