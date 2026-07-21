LIBRARY()

SRCS(
    mock_pq_gateway.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()
