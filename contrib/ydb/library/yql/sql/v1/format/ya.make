LIBRARY()

SRCS(
    sql_format.cpp
)

RESOURCE(DONT_PARSE contrib/ydb/library/yql/sql/v1/SQLv1Antlr4.g.in SQLv1Antlr4.g.in)

PEERDIR(
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/proto_parser
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4
    contrib/ydb/library/yql/core/sql_types
    library/cpp/protobuf/util
    library/cpp/resource
)

END()

RECURSE(
    check
)

RECURSE_FOR_TESTS(
    ut_antlr4
)
