LIBRARY()

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_stats
)

END()

RECURSE_FOR_TESTS(
    ut
)
