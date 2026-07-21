UNITTEST_FOR(contrib/ydb/library/yql/providers/config)

SRCS(
    yql_config_provider_statistics_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
