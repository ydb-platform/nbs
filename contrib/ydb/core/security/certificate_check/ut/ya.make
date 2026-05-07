UNITTEST_FOR(contrib/ydb/core/security/certificate_check)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    cert_check_ut.cpp
    cert_utils_ut.cpp
)

END()
