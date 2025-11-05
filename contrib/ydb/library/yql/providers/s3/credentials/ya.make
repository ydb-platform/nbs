LIBRARY()

SRCS(
    credentials.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()

RECURSE_FOR_TESTS(
    ut
)
