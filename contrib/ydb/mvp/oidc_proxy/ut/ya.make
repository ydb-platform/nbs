UNITTEST_FOR(contrib/ydb/mvp/oidc_proxy)

SIZE(SMALL)

SRCS(
    mvp_config_validation_ut.cpp
    oidc_proxy_ut.cpp
    openid_connect.cpp
    tokenator_integration_ut.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/testlib/service_mocks
)

DATA(
    arcadia/contrib/ydb/mvp/oidc_proxy/examples
)

END()
