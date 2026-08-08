UNITTEST_FOR(contrib/ydb/core/formats/arrow/accessor/sparsed)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/sparsed
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    contrib/ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_composite.cpp
)

END()
