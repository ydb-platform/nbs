UNITTEST_FOR(contrib/ydb/library/yql/providers/generic/actors)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    yql_generic_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
