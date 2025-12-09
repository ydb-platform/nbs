UNITTEST_FOR(contrib/ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/arrow_kernels
    contrib/ydb/library/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/program
    contrib/ydb/core/base
    contrib/ydb/library/formats/arrow

    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper

)

ADDINCL(
    contrib/ydb/library/arrow_clickhouse
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_arrow.cpp
    ut_program_step.cpp
    ut_dictionary.cpp
    ut_column_filter.cpp
    ut_hash.cpp
    ut_reader.cpp
)

END()
