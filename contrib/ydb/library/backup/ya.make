LIBRARY(kikimr_backup)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/logger
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    yql/essentials/types/dynumber
    yql/essentials/sql/v1/format
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/lib/ydb_cli/dump/files
    contrib/ydb/public/sdk/cpp/src/client/cms
    contrib/ydb/public/sdk/cpp/src/client/coordination
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/rate_limiter
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/value
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
