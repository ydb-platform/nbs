UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/library/login
    contrib/ydb/library/testlib/service_mocks/ldap_mock
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/core/testlib/audit_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_login.cpp
)

END()
