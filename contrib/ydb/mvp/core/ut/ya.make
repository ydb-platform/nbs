UNITTEST_FOR(contrib/ydb/mvp/core)

SIZE(SMALL)

SRCS(
    mvp_ut.cpp
    mvp_tokens.cpp
    mvp_test_runtime.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/actors
    contrib/libs/jwt-cpp
    contrib/ydb/library/testlib/service_mocks
)

END()
