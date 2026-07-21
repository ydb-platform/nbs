LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    contrib/ydb/core/keyvalue
    contrib/ydb/core/persqueue/pqtablet/blob
    contrib/ydb/core/persqueue/events
    contrib/ydb/public/api/grpc/draft
)

END()

RECURSE_FOR_TESTS(
    ut
)
