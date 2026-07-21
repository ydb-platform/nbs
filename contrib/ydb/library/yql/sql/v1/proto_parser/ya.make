LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/parser/proto_ast/collect_issues
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4
)

SRCS(
    parse_tree.cpp
    proto_parser.cpp
    reflection.cpp
    statement.cpp
    token.cpp
)

END()

RECURSE(
    antlr4
    antlr4_ansi
)
