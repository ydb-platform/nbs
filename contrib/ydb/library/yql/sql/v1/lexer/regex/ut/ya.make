UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/lexer/regex)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
)

SRCS(
    lexer_ut.cpp
    regex_ut.cpp
)

END()