UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/ide/completion/analysis/yql)

SRCS(
    yql_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

END()
