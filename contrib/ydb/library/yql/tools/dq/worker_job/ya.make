LIBRARY()

SRCS(
    dq_worker.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/yt/core
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/service
    contrib/ydb/library/yql/providers/dq/stats_collector
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils/log/proto
    contrib/ydb/library/yql/providers/dq/actors/yt
    contrib/ydb/library/yql/providers/dq/global_worker_manager
    contrib/ydb/library/yql/utils/signals
)

YQL_LAST_ABI_VERSION()

END()
