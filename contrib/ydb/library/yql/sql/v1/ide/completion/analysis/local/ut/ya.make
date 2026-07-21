UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/ide/completion/analysis/local)

SRCS(
    cursor_token_context_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
)

END()
