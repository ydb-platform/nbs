UNITTEST_FOR(contrib/ydb/core/security)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    SUPPRESSIONS(tsan.supp)
ENDIF()

PEERDIR(
    contrib/ydb/core/security/certificate_check/test_utils
    contrib/ydb/core/security/external_idp/test_utils
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/audit_helpers
    contrib/ydb/library/testlib/service_mocks
    contrib/ydb/library/testlib/service_mocks/ldap_mock
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/libs/jwt-cpp
    contrib/libs/openssl
    library/cpp/json
    library/cpp/string_utils/base64
)

YQL_LAST_ABI_VERSION()

SRCS(
    audit_ut.cpp
    secure_request_ut.cpp
    ticket_parser_ut.cpp
    ut_common.cpp
)

END()
