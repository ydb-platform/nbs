UNITTEST_FOR(contrib/ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/arrow_kernels
    contrib/ydb/library/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/core/formats/arrow/hash
    contrib/ydb/core/formats/arrow/printer
    contrib/ydb/core/formats/arrow/program
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/core/base
    contrib/ydb/library/formats/arrow

    # for NYql::NUdf alloc stuff used in binary_json
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper

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
    ut_column_filter.cpp
    ut_dictionary.cpp
    ut_hash.cpp
    ut_printer.cpp
    ut_program_step.cpp
    ut_reader.cpp
    ut_slicer.cpp
)

END()
