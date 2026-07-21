LIBRARY()

SRCS(
    kqp_federated_query_helpers.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/local_proxy/local_pq_client
    contrib/ydb/core/protos
    contrib/ydb/library/logger
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/transform
    contrib/ydb/library/yql/providers/pq/gateway/native
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/adapters/executor
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/extensions/discovery_mutator
    contrib/ydb/library/yql/core/dq_integration/transform
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/lib/yt_download
    contrib/ydb/library/yql/providers/yt/mkql_dq
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
)

RECURSE_FOR_TESTS(
    ut
)
