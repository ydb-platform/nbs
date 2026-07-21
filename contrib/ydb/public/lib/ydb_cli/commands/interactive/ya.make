LIBRARY()

SRCS(
    interactive_cli.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/lib/ydb_cli/commands/interactive/highlight
    contrib/ydb/public/lib/ydb_cli/commands/interactive/complete
    contrib/ydb/public/lib/ydb_cli/commands/interactive/session
)

END()

RECURSE(
    ai
    complete
    highlight
    session
)
