UNITTEST_FOR(contrib/ydb/core/formats/arrow/accessor/dictionary)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/dictionary
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_dictionary.cpp
)

END()
