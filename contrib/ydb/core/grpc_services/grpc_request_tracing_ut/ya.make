UNITTEST_FOR(contrib/ydb/core/grpc_services)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    grpc_request_tracing_ut.cpp
)

END()
