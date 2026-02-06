UNITTEST_FOR(contrib/ydb/core/grpc_services)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/library/testlib/service_mocks
)

YQL_LAST_ABI_VERSION()

SRCS(
    grpc_request_check_actor_ut.cpp
)

END()
