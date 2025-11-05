LIBRARY()

SRCS(
    yql_mock.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/json/yson
    library/cpp/monlib/dynamic_counters
    library/cpp/random_provider
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/shared_resources/interface
    contrib/ydb/core/protos
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/providers/clickhouse/provider
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    contrib/ydb/library/yql/providers/ydb/provider
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
