LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_ansi_antlr4
    contrib/ydb/library/yql/parser/antlr_ast/gen/v1_antlr4
)

SRCS(
    base_visitor.cpp
    cursor_text.cpp
    narrowing_visitor.cpp
    parse_tree.cpp
    parser.cpp
)

END()

RECURSE_FOR_TESTS(

)
