IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

YQL_UDF_CONTRIB(yql_language_udf)

YQL_ABI_VERSION(
    2
    39
    0
)

SUBSCRIBER(g:yql)

SRCS(
    sql2yql.cpp
    yql_language_udf.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/reflect
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/gateways_utils
    library/cpp/protobuf/util
)

END()

ENDIF()

RECURSE_FOR_TESTS(
    test
)
