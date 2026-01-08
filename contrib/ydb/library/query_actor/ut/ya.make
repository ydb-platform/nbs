UNITTEST_FOR(contrib/ydb/library/query_actor)

PEERDIR(
    contrib/ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

SRCS(
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
