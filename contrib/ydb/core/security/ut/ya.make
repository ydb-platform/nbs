UNITTEST_FOR(contrib/ydb/core/security)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/library/testlib/service_mocks
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ticket_parser_ut.cpp
)

END()
