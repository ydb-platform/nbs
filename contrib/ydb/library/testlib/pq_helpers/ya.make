LIBRARY()

SRCS(
    mock_pq_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/library/yql/providers/pq/provider
)

YQL_LAST_ABI_VERSION()

END()
