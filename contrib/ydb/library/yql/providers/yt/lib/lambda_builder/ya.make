LIBRARY()

SRCS(
    lambda_builder.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/public/langver
)

YQL_LAST_ABI_VERSION()

END()
