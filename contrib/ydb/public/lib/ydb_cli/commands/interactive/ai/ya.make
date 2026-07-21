LIBRARY()

SRCS(
    ai_model_handler.cpp
)

PEERDIR(
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/models
    contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/tools
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/lib/ydb_cli/common
)

END()

RECURSE(
    models
    tools
)
