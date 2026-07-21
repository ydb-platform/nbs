LIBRARY()

SRCS(
    model_anthropic.cpp
    model_base.cpp
    model_openai.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/url
    library/cpp/threading/future
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/lib/ydb_cli/commands/interactive/common
    contrib/ydb/public/lib/ydb_cli/common
)

END()
