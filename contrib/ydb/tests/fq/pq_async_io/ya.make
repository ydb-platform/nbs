LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/common/ut_helpers
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/library/yql/minikql/computation/llvm16
    contrib/ydb/library/yql/providers/common/proto
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
