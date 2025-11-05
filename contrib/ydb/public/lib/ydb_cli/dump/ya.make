LIBRARY()

SRCS(
    dump.cpp
    dump_impl.cpp
    restore_impl.cpp
    restore_import_data.cpp
    restore_compat.cpp
)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/string_utils/quote
    contrib/ydb/library/backup
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/sdk/cpp/client/ydb_proto
)

END()
