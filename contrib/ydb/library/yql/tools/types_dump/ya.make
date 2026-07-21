PROGRAM()

SRCS(
    types_dump.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/utils/backtrace
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
