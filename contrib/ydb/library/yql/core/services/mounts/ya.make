LIBRARY()

SRCS(
    yql_mounts.h
    yql_mounts.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    contrib/ydb/library/yql/mount/lib/yql/aggregate.yqls /lib/yql/aggregate.yqls
    contrib/ydb/library/yql/mount/lib/yql/window.yqls /lib/yql/window.yqls
    contrib/ydb/library/yql/mount/lib/yql/id.yqls /lib/yql/id.yqls
    contrib/ydb/library/yql/mount/lib/yql/sqr.yqls /lib/yql/sqr.yqls
    contrib/ydb/library/yql/mount/lib/yql/core.yqls /lib/yql/core.yqls
    contrib/ydb/library/yql/mount/lib/yql/walk_folders.yqls /lib/yql/walk_folders.yqls
)

END()
