LIBRARY()

SRCS(
    yql_pure_provider.cpp
    yql_pure_provider.h
)

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/mkql_simple_file
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/minikql/runtime_settings
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
