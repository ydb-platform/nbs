LIBRARY()

SRCS(
    kqp_mkql_compiler.cpp
    kqp_olap_compiler.cpp
    kqp_query_compiler.cpp
)

PEERDIR(
    contrib/ydb/core/formats
    contrib/ydb/core/kqp/common
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/library/yql/core/dq_integration
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/s3/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
