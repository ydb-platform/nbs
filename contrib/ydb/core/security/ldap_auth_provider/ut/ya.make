UNITTEST_FOR(contrib/ydb/core/security/ldap_auth_provider)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/security/ldap_auth_provider/test_utils
    contrib/ydb/library/testlib/service_mocks/ldap_mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    ldap_auth_provider_ut.cpp
    ldap_utils_ut.cpp
)

END()
