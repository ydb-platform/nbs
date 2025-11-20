LIBRARY()

SRCS(
    yql_yt_lookup_actor.cpp
    yql_yt_provider_factories.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/yt/proto
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/public/types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)