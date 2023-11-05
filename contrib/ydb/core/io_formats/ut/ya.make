UNITTEST_FOR(contrib/ydb/core/io_formats)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/io_formats

    # for NYql::NUdf alloc stuff used in binary_json
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_csv.cpp
)

END()
