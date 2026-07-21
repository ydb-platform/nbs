LIBRARY()

SRCS(
    import.cpp
    cli_arrow_helpers.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/lib/json_value
    contrib/libs/apache/arrow
    library/cpp/string_utils/csv
)

GENERATE_ENUM_SERIALIZATION(import.h)

END()
