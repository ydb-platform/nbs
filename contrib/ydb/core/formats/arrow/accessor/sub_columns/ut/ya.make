UNITTEST_FOR(contrib/ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/sub_columns
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    contrib/ydb/core/formats/arrow
)

SRCS(
    ut_sub_columns.cpp
)

YQL_LAST_ABI_VERSION()

END()
