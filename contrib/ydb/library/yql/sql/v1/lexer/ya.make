LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/sql/settings
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()

RECURSE(
    antlr4
    antlr4_ansi
    antlr4_pure
    antlr4_pure_ansi
    check
    regex
)

RECURSE_FOR_TESTS(
    ut
)

