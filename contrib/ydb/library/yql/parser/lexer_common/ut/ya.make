UNITTEST_FOR(contrib/ydb/library/yql/parser/lexer_common)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
)


SRCS(
    hints_ut.cpp
    parse_hints_ut.cpp
)

END()
