LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/ut_helpers
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/public/sdk/cpp/src/client/topic
    yql/essentials/minikql/computation/llvm16
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
