UNITTEST_FOR(contrib/ydb/library/yql/providers/s3/provider)

TAG(ya:manual)

SRCS(
    yql_s3_listing_strategy_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
