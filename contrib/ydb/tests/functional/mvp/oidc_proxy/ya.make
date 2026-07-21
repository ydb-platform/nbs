PY3TEST()

FORK_TEST_FILES()
SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

ENV(OIDC_PROXY_BINARY="contrib/ydb/mvp/oidc_proxy/bin/mvp_oidc_proxy")

TEST_SRCS(
    conftest.py
    oidc_proxy_env.py
    oidc_proxy_testlib.py
    test_auth_failures.py
    test_basic_scenarios.py
    test_streaming.py
)

DEPENDS(
    contrib/ydb/mvp/oidc_proxy/bin
)

PEERDIR(
    contrib/python/requests
    contrib/ydb/tests/functional/mvp/common
    contrib/ydb/tests/library
)

END()
