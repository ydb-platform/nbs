IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        contrib/ydb/public/sdk/cpp/src/client/persqueue_public/codecs
        library/cpp/getopt
        yt/cpp/mapreduce/client
        contrib/ydb/library/yql/dq/actors/compute
        contrib/ydb/library/yql/dq/actors/spilling
        contrib/ydb/library/yql/dq/comp_nodes
        yql/essentials/core/dq_integration/transform
        contrib/ydb/library/yql/dq/transform
        yql/essentials/minikql/comp_nodes/llvm16
        contrib/ydb/library/yql/providers/clickhouse/actors
        yql/essentials/providers/common/comp_nodes
        contrib/ydb/library/yql/providers/dq/runtime
        contrib/ydb/library/yql/providers/dq/service
        contrib/ydb/library/yql/providers/dq/metrics
        contrib/ydb/library/yql/providers/dq/stats_collector
        contrib/ydb/library/yql/providers/dq/task_runner
        contrib/ydb/library/yql/providers/pq/async_io
        contrib/ydb/library/yql/providers/pq/gateway/native
        contrib/ydb/library/yql/providers/pq/proto
        contrib/ydb/library/yql/providers/s3/actors
        contrib/ydb/library/yql/providers/ydb/actors
        contrib/ydb/library/yql/providers/ydb/comp_nodes
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/utils
        yql/essentials/utils/log
        yql/essentials/utils/log/proto
        yql/essentials/utils/failure_injector
        yql/essentials/utils/backtrace
        yql/essentials/utils/network
        yt/yql/providers/yt/comp_nodes/dq/llvm16
        yt/yql/providers/yt/comp_nodes/llvm16
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/mkql_dq
        contrib/ydb/library/yql/providers/dq/actors/yt
        contrib/ydb/library/yql/providers/dq/global_worker_manager
        yql/essentials/sql/pg
        yql/essentials/parser/pg_wrapper
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
