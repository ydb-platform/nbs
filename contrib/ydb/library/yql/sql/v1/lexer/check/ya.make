LIBRARY()

SRCS(
    check_lexers.h
    check_lexers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/sql/v1/lexer/regex
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

END()

RECURSE_FOR_TESTS(ut)
