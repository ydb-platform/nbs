UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

SRCS(
    ut_full_backup.cpp
    ut_full_backup_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
