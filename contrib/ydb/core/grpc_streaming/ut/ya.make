UNITTEST_FOR(contrib/ydb/core/grpc_streaming)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    grpc_streaming_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/core/grpc_streaming/ut/grpc
    contrib/ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
