UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_backup_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/public/lib/ydb_cli/dump
    contrib/ydb/public/sdk/cpp/client/ydb_export
    contrib/ydb/public/sdk/cpp/client/ydb_import
    contrib/ydb/public/sdk/cpp/client/ydb_operation
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_value
    contrib/ydb/library/backup
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
)

YQL_LAST_ABI_VERSION()

END()
