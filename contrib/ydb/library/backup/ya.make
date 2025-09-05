LIBRARY(kikimr_backup)

CFLAGS(
    -Wno-deprecated-declarations
)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    util
    contrib/ydb/library/dynumber
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

SRCS(
    backup.cpp
    query_builder.cpp
    query_uploader.cpp
    util.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
