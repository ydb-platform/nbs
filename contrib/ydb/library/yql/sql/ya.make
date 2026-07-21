LIBRARY()

PEERDIR(
    library/cpp/deprecated/split
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/parser/proto_ast
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/utils
)

SRCS(
    cluster_mapping.cpp
    sql.cpp
)

END()

RECURSE(
    pg
    pg_dummy
    settings
    v0
    v1
)
