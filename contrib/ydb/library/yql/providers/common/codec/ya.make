LIBRARY()

SRCS(
    yql_codec.cpp
    yql_codec.h
    yql_codec_buf.cpp
    yql_codec_buf.h
    yql_codec_type_flags.cpp
    yql_codec_type_flags.h
    yql_json_codec.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/public/result_format
    library/cpp/yson/node
    library/cpp/yson
    library/cpp/json
    library/cpp/enumbitset
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_codec_type_flags.h)

END()

RECURSE(
    arrow
    yt_arrow_converter_interface
)

RECURSE_FOR_TESTS(
    ut
)
