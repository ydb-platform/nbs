LIBRARY()

SRCS(
    dump.cpp
    dump_impl.cpp
    restore_impl.cpp
    restore_import_data.cpp
    restore_compat.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/bucket_quoter
    library/cpp/logger
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    contrib/ydb/library/backup
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/dump/files
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/topic
)

GENERATE_ENUM_SERIALIZATION(dump.h)

END()
