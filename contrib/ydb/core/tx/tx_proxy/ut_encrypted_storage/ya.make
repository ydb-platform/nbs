UNITTEST_FOR(contrib/ydb/core/tx/tx_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

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
    encrypted_storage_ut.cpp
    proxy_ut_helpers.h
    proxy_ut_helpers.cpp
)

END()
