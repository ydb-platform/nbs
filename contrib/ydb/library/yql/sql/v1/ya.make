LIBRARY()

PEERDIR(
    library/cpp/charset/lite
    library/cpp/enumbitset
    library/cpp/json
    library/cpp/yson/node
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/core/langver
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/parser/proto_ast/collect_issues
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/proto_parser
    # for lexer tokens
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_antlr4
)

SRCS(
    aggregation.cpp
    builtin.cpp
    context.cpp
    join.cpp
    insert.cpp
    list_builtin.cpp
    match_recognize.cpp
    namespace.cpp
    node.cpp
    result.cpp
    secret_settings.cpp
    select_yql.cpp
    select_yql_aggregation.cpp
    select_yql_window.cpp
    select.cpp
    source.cpp
    sql.cpp
    sql_call_expr.cpp
    sql_expression.cpp
    sql_group_by.cpp
    sql_match_recognize.cpp
    sql_into_tables.cpp
    sql_query.cpp
    sql_select_window.cpp
    sql_select_yql.cpp
    sql_select.cpp
    sql_translation.cpp
    sql_values.cpp
    query.cpp
    object_processing.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(match_recognize.h)
GENERATE_ENUM_SERIALIZATION(node.h)
GENERATE_ENUM_SERIALIZATION(sql_call_param.h)

END()

RECURSE(
    format
    highlight
    ide
    lexer
    perf
    proto_parser
    reflect
)

RECURSE_FOR_TESTS(
    ut_antlr4
)
