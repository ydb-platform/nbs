LIBRARY()

ENABLE(SKIP_YQL_STYLE_CPP)
NO_CLANG_TIDY()

PEERDIR(
    library/cpp/charset/lite
    library/cpp/enumbitset
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/parser/proto_ast/collect_issues
    contrib/ydb/library/yql/parser/proto_ast/gen/v0
    contrib/ydb/library/yql/sql/v0/lexer
)

SRCS(
    aggregation.cpp
    builtin.cpp
    context.cpp
    join.cpp
    insert.cpp
    list_builtin.cpp
    node.cpp
    select.cpp
    sql.cpp
    query.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(node.h)

END()

RECURSE(
    lexer
)

RECURSE_FOR_TESTS(
    ut
)
