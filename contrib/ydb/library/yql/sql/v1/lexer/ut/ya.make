UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/lexer)

PEERDIR(
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/sql/v1/lexer/regex
)

SRCS(
    lexer_ut.cpp
)

END()
