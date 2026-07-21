LIBRARY()

SRCS(
    ansi.cpp
    format.cpp
    grammar.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_ansi_antlr4
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_antlr4
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/regex
    contrib/ydb/library/yql/sql/v1/reflect
    contrib/ydb/library/yql/sql/v1/ide/completion/antlr4
    contrib/ydb/library/yql/sql/v1/ide/completion/core
    contrib/ydb/library/yql/sql/v1/ide/completion/text
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
