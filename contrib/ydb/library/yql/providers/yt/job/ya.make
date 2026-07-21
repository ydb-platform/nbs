LIBRARY()

SRCS(
    yql_job_base.cpp
    yql_job_calc.cpp
    yql_job_factory.cpp
    yql_job_infer_schema.cpp
    yql_job_registry.h
    yql_job_stats_writer.cpp
    yql_job_user.cpp
    yql_job_user_base.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/streams/brotli
    library/cpp/time_provider
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/user_job_statistics
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/comp_nodes
    contrib/ydb/library/yql/providers/yt/lib/infer_schema
    contrib/ydb/library/yql/providers/yt/lib/lambda_builder
    contrib/ydb/library/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
