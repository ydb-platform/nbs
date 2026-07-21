IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/iterator
    contrib/ydb/library/yql/sql/v1/ide/completion
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/cluster
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/schema
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/union
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_pure_ansi
    contrib/ydb/library/yql/utils
)

SRCS(
    yql_complete.cpp
)

END()

ENDIF()
