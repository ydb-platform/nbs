UNITTEST_FOR(contrib/ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/sub_columns
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/formats/arrow
)

SRCS(
    ut_sub_columns.cpp
    ut_native_scalars.cpp
    ut_dictionary.cpp
)

YQL_LAST_ABI_VERSION()

END()
