UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/gateway)

SIZE(MEDIUM)

SRCS(
    composite_client_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/basics
    contrib/ydb/library/testlib/common
    contrib/ydb/library/testlib/pq_helpers
    contrib/ydb/library/yql/providers/pq/async_io
    contrib/ydb/library/yql/providers/pq/gateway/clients/composite
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
