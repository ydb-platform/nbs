PROGRAM()

PEERDIR(
    library/cpp/svnversion
    library/cpp/yt/mlock
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq
    contrib/ydb/library/yql/providers/yt/mkql_dq
    contrib/ydb/library/yql/tools/dq/worker_job
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
