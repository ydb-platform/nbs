LIBRARY()

SRCS(
    column.cpp
    evaluate.cpp
    function.cpp
    global.cpp
    input.cpp
    named_node_resolution.cpp
    named_node_visibility.cpp
    parse_tree.cpp
    parser.cpp
    use.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/core
    contrib/ydb/library/yql/sql/v1/ide/completion/syntax
    contrib/ydb/library/yql/sql/v1/ide/completion/text
    contrib/ydb/library/yql/sql/v1/ide/pure_ast
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_antlr4
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_ansi_antlr4
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
