UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/provider)

TAG(ya:manual)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/stub
)

SRCS(
    yql_dq_provider_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
