PROGRAM(memory_tests)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/core/formats/arrow/rows
)

SRCDIR(
    contrib/ydb/core/tx/columnshard/tools/memory_tests
)

SRCS(
    main.cpp
)

YQL_LAST_ABI_VERSION()

END()
