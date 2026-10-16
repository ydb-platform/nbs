UNITTEST_FOR(contrib/ydb/library/ncloud/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    contrib/ydb/core/base
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

END()
