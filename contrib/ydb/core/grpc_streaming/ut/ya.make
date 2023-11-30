UNITTEST_FOR(contrib/ydb/core/grpc_streaming)

FORK_SUBTESTS()

TIMEOUT(300)

SIZE(MEDIUM)

SRCS(
    grpc_streaming_ut.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    contrib/ydb/core/grpc_streaming/ut/grpc
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
