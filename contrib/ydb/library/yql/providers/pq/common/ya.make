LIBRARY()

SRCS(
    pq_meta_fields.cpp
    pq_partitions.cpp
    yql_names.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/library/yql/public/types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
