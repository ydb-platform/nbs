PROGRAM()

SRCS(
    sql_functions_dump.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/stub
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
