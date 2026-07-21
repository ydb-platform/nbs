LIBRARY()

SRCS(
    data_language_json.cpp
    sql_highlight_json.cpp
    sql_highlight.cpp
    sql_highlighter.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer/regex
    contrib/ydb/library/yql/sql/v1/reflect
    library/cpp/resource
    library/cpp/json
)

RESOURCE(
    contrib/ydb/library/yql/sql/v1/highlight/ut/suite.json suite.json
    contrib/ydb/library/yql/data/language/types.json types.json
    contrib/ydb/library/yql/data/language/statements_opensource.json statements_opensource.json
)

END()

RECURSE_FOR_TESTS(
    ut
)
