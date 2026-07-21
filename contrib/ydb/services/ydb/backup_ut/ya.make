UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    backup_path_ut.cpp
    encrypted_backup_ut.cpp
    fs_backup_validation_ut.cpp
    list_objects_in_s3_export_ut.cpp
    ydb_backup_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    library/cpp/streams/zstd
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/util
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/library/aws_init
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
    contrib/ydb/library/testlib/parquet_helpers
)

YQL_LAST_ABI_VERSION()

IF (OS_LINUX)
    LDFLAGS(-Wl,--wrap=statfs)
ENDIF()

END()
