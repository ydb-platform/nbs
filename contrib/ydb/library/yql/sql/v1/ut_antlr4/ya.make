UNITTEST_FOR(contrib/ydb/library/yql/sql/v1)

SRCS(
    sql_ut_antlr4.cpp
    sql_match_recognize_ut.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    contrib/ydb/library/yql/utils/string
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/core/langver
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
