LIBRARY()

SRCS(
    mock_pq_gateway.cpp
    ut_helpers.cpp
)

PEERDIR(
    yql/essentials/minikql/computation/llvm16
    contrib/ydb/library/yql/providers/common/ut_helpers
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
