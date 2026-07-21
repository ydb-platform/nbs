G_BENCHMARK()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/codegen/no_llvm
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/library/yql/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

SRCS(
    ../../ut/mkql_test_factory.cpp
    bench.cpp
)

END()
