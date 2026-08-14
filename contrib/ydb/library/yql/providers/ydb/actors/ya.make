LIBRARY()

SRCS(
    yql_ydb_read_actor.cpp
    yql_ydb_source_factory.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    yql/essentials/minikql/computation
    contrib/ydb/library/yql/providers/common/token_accessor/client
    yql/essentials/public/types
    yql/essentials/utils/log
    contrib/ydb/public/lib/experimental
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/ydb/proto
)

YQL_LAST_ABI_VERSION()

END()
