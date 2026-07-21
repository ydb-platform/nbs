LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/parser/common/antlr4
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_antlr4
)

SRCS(
    lexer.cpp
)

END()
