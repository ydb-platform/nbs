UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/provider)

TAG(ya:manual)

SIZE(SMALL)

SRCS(
    yql_yt_dq_integration_ut.cpp
    yql_yt_epoch_ut.cpp
    yql_yt_cbo_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/schema
    contrib/ydb/library/yql/providers/yt/provider
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
    contrib/ydb/library/yql/core/ut_common
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
