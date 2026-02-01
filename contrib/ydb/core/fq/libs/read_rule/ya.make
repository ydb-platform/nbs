LIBRARY()

SRCS(
    read_rule_creator.cpp
    read_rule_deleter.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

END()
