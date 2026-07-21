IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    contrib/libs/protobuf
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/core/langver
)

SRCS(
    sql_formatter.cpp
)

END()

ENDIF()
