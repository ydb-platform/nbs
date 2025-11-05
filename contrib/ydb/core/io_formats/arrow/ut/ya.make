UNITTEST_FOR(contrib/ydb/core/io_formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/io_formats/arrow

    # for NYql::NUdf alloc stuff used in binary_json
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    csv_arrow_ut.cpp
)

END()
