UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(11)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

IF (NOT OS_WINDOWS)
    PEERDIR(
        contrib/libs/apache/arrow
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        contrib/ydb/core/testlib/default
        contrib/ydb/library/testlib/parquet_helpers
        contrib/ydb/core/tx
        contrib/ydb/core/tx/columnshard
        contrib/ydb/core/tx/columnshard/hooks/testing
        contrib/ydb/core/tx/columnshard/test_helper
        contrib/ydb/core/tx/schemeshard/ut_helpers
        contrib/ydb/core/util
        contrib/ydb/core/wrappers/ut_helpers
        contrib/ydb/library/aws_init
        contrib/ydb/library/yql/public/udf/service/exception_policy
        contrib/ydb/core/testlib/audit_helpers
    )
    SRCS(
        ut_export.cpp
        ut_export_fs.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
