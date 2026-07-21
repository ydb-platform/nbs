LIBRARY()

SRCS(
    sql_format.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

END()
