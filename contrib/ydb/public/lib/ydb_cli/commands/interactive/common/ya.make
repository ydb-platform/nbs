LIBRARY()

SRCS(
    api_utils.cpp
    config_ui.cpp
    interactive_config.cpp
    interactive_settings.cpp
    json_utils.cpp
    line_reader.cpp
)

PEERDIR(
    contrib/libs/ftxui
    contrib/restricted/patched/replxx
    library/cpp/http/simple
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/url
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/lib/ydb_cli/commands/interactive/complete
    contrib/ydb/public/lib/ydb_cli/commands/interactive/highlight
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/library/yql/sql/v1/ide/completion
)

GENERATE_ENUM_SERIALIZATION(interactive_config.h)
GENERATE_ENUM_SERIALIZATION(json_utils.h)

END()

RECURSE_FOR_TESTS(
    ut
)
