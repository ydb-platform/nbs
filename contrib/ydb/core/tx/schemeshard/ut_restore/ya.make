UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(120)

REQUIREMENTS(cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/streams/zstd
    library/cpp/string_utils/quote
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/util
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/aws_init
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/core/testlib/audit_helpers
)

SRCS(
    ut_restore.cpp
    ut_restore_fs.cpp
)

YQL_LAST_ABI_VERSION()

END()
