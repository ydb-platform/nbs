UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/format)

SRCS(
    sql_format_ut_antlr4.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/utils/string
)


END()
