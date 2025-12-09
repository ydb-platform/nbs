UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    backup_path_ut.cpp
    encrypted_backup_ut.cpp
    list_objects_in_s3_export_ut.cpp
    ydb_backup_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/streams/zstd
    contrib/ydb/core/testlib/default
    contrib/ydb/core/util
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/public/lib/ydb_cli/dump
    contrib/ydb/public/sdk/cpp/src/client/coordination
    contrib/ydb/public/sdk/cpp/src/client/export
    contrib/ydb/public/sdk/cpp/src/client/import
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/public/sdk/cpp/src/client/rate_limiter
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/value
    contrib/ydb/library/backup
)

YQL_LAST_ABI_VERSION()

END()
