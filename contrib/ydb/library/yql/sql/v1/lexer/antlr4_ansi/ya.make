LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/parser/proto_ast/collect_issues
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    lexer.cpp
)

END()
