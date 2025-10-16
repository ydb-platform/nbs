LIBRARY(common)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    aws.cpp
    command.cpp
    common.cpp
    csv_parser.cpp
    examples.cpp
    format.cpp
    interactive.cpp
    interruptible.cpp
    normalize_path.cpp
    parameter_stream.cpp
    parameters.cpp
    pg_dump_parser.cpp
    pretty_table.cpp
    print_operation.cpp
    print_utils.cpp
    profile_manager.cpp
    progress_bar.cpp
    query_stats.cpp
    recursive_list.cpp
    recursive_remove.cpp
    retry_func.cpp
    root.cpp
    scheme_printers.cpp
    sys.cpp
    tabbed_table.cpp
    ydb_updater.cpp
    yt.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/config
    library/cpp/getopt
    library/cpp/json/writer
    library/cpp/yaml/as
    library/cpp/string_utils/csv
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/library/arrow_parquet
)

GENERATE_ENUM_SERIALIZATION(formats.h)
GENERATE_ENUM_SERIALIZATION(parameters.h)

END()

RECURSE_FOR_TESTS(
    ut
)
