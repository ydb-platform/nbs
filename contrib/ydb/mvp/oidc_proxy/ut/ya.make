UNITTEST_FOR(contrib/ydb/mvp/oidc_proxy)

SIZE(SMALL)

SRCS(
    oidc_proxy_ut.cpp
    openid_connect.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/testlib/service_mocks
)

END()
