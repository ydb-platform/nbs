LIBRARY()

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/value
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
)

SRCS(
    yql_parser.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
