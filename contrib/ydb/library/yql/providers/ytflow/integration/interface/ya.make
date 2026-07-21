LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql
)

SRCS(
    yql_ytflow_integration.cpp
    yql_ytflow_optimization.cpp
)

END()
