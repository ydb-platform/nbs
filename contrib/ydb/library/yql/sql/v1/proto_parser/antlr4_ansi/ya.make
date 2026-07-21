LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/parser/proto_ast/antlr4
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    proto_parser.cpp
)

END()
