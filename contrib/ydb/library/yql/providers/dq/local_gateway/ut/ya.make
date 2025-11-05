UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/local_gateway)

TAG(ya:manual)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins/no_llvm
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/computation/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
