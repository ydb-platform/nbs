UNITTEST_FOR(contrib/ydb/core/tx/tx_proxy)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    proxy_ext_tenant_ut.cpp
    proxy_ut_helpers.cpp
)

END()
