LIBRARY()

SRCS(
    describe_tool.cpp
    docs_search_tool.cpp
    exec_query_tool.cpp
    exec_shell_tool.cpp
    explain_query_tool.cpp
    list_directory_tool.cpp
    tool_base.cpp
    tool_interface.cpp
    ydb_help_tool.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/archive
    library/cpp/colorizer
    library/cpp/json/writer
    library/cpp/resource
    library/cpp/yaml/as
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/lib/ydb_cli/commands/interactive/highlight
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/dump
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/scheme
)

END()
