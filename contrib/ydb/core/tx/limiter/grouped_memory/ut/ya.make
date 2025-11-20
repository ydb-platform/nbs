UNITTEST_FOR(contrib/ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/tx/limiter/grouped_memory/usage
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/parser/pg_wrapper
)

SRCS(
    ut_manager.cpp
)

YQL_LAST_ABI_VERSION()

END()
