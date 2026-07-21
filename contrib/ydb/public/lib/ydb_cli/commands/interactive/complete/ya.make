LIBRARY()

SRCS(
    ydb_schema.cpp
    yql_completer.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    contrib/ydb/library/yql/sql/v1/ide/completion
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cache/local
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/cached
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/impatient
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/schema
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/union
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    contrib/ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
