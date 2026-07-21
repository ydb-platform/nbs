UNITTEST_FOR(contrib/ydb/library/json_index)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/library/yql/minikql/jsonpath/parser
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    json_index_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
