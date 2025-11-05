LIBRARY()

SRC(yql_yt_dq_task_preprocessor.cpp)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils/failure_injector
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/gateway/lib
    contrib/ydb/library/yql/providers/yt/lib/yson_helpers
)

YQL_LAST_ABI_VERSION()

END()
