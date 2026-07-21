G_BENCHMARK()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/sql/v1/ide/completion
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/static
)

END()
