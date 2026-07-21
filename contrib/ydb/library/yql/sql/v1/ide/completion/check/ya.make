LIBRARY()

SRCS(
    check_complete.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion
    contrib/ydb/library/yql/sql/v1/ide/completion/analysis/yql
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/cluster
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/schema
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/union
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
