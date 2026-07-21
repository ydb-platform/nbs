UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/ide/completion)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cache/local
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/cached
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/cluster
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/impatient
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/schema
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/union
)

END()
