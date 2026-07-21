LIBRARY()

SRCS(
    yql_highlighter.cpp
)

PEERDIR(
    contrib/libs/ftxui
    contrib/restricted/patched/replxx
    contrib/ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/library/yql/sql/v1/highlight
)

END()

RECURSE(
    color
)

RECURSE_FOR_TESTS(
    ut
)
