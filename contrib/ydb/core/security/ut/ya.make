UNITTEST_FOR(contrib/ydb/core/security)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/library/testlib/service_mocks
    contrib/ydb/library/testlib/service_mocks/ldap_mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    ticket_parser_ut.cpp
    ldap_utils_ut.cpp
)

END()
