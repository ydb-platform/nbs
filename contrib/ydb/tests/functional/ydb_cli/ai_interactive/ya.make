PROGRAM(ydb)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/apps/ydb/commands
    contrib/ydb/public/lib/ydb_cli/commands
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/ya.make.inc)

END()
