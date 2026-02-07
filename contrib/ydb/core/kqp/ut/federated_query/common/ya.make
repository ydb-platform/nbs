LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/public/sdk/cpp/client/ydb_operation
    contrib/ydb/public/sdk/cpp/client/ydb_query
)

YQL_LAST_ABI_VERSION()

END()
