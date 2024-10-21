UNITTEST_FOR(cotnrib/ydb/library/ncloud/impl)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

REQUIREMENTS(ram:10)

END()
