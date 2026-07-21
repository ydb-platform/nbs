LIBRARY()

SRCS(
    ai_session_runner.cpp
    session_runner_common.cpp
    sql_session_runner.cpp
)

PEERDIR(
    contrib/libs/ftxui
    library/cpp/colorizer
    library/cpp/yaml/as
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/lib/ydb_cli/commands/interactive/ai
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/src/client/table/query_stats
)

END()
