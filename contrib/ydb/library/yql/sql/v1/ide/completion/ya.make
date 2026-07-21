LIBRARY()

SRCS(
    configuration.cpp
    name_mapping.cpp
    sql_complete.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/ide/completion/antlr4
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
    contrib/ydb/library/yql/sql/v1/ide/completion/syntax
    contrib/ydb/library/yql/sql/v1/ide/completion/analysis/global
    contrib/ydb/library/yql/sql/v1/ide/completion/analysis/local
    contrib/ydb/library/yql/sql/v1/ide/completion/text
    # TODO(YQL-19747): split /name/service/ranking interface and implementation
    # TODO(YQL-19747): extract NameIndex
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/binding
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/column
)

END()

RECURSE(
    analysis
    antlr4
    bench
    check
    core
    name
    syntax
    text
)

RECURSE_FOR_TESTS(
    ut
)
