IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM(sql2yql)

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    library/cpp/getopt
    library/cpp/testing/unittest
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/sql/v1/ide/completion/check
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/format/check
    contrib/ydb/library/yql/sql/v1/lexer/check
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/core/pg_ext
    contrib/ydb/library/yql/providers/common/gateways_utils
)

ALLOCATOR(J)

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    sql2yql.cpp
)

END()

ENDIF()
