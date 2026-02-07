UNITTEST_FOR(contrib/ydb/library/yql/providers/generic/actors)

TAG(ya:manual)

PEERDIR(
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    contrib/ydb/library/actors/testlib
    library/cpp/testing/unittest
)

SRCS(
    yql_generic_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
