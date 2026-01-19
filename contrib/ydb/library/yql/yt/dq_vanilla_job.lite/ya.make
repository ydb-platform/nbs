PROGRAM()

PEERDIR(
    library/cpp/yt/mlock
    yt/cpp/mapreduce/client
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq
    contrib/ydb/library/yql/providers/yt/mkql_dq
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
