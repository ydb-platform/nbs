LIBRARY()

SRCS(
    yql_ydb_read_actor.cpp
    yql_ydb_source_factory.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/utils/log
    contrib/ydb/public/lib/experimental
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/ydb/proto
)

YQL_LAST_ABI_VERSION()

END()
