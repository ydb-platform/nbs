IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        library/cpp/getopt
        yt/cpp/mapreduce/client
        contrib/ydb/library/yql/sql/pg
        contrib/ydb/library/yql/parser/pg_wrapper
        contrib/ydb/library/yql/public/udf/service/exception_policy
        contrib/ydb/library/yql/utils/failure_injector
        contrib/ydb/library/yql/utils/log
        contrib/ydb/library/yql/utils/log/proto
        contrib/ydb/library/yql/providers/dq/provider
        contrib/ydb/library/yql/providers/dq/worker_manager/interface
        contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
        contrib/ydb/library/yql/utils/backtrace
        contrib/ydb/library/yql/providers/dq/service
        contrib/ydb/library/yql/providers/dq/metrics
        contrib/ydb/library/yql/providers/dq/stats_collector
        contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
        contrib/ydb/library/yql/providers/dq/global_worker_manager
        contrib/ydb/library/yql/providers/dq/actors/yt
        contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
        contrib/ydb/library/yql/providers/yt/codec/codegen
        yt/yt/client
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
