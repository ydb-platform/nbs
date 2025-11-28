UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
    contrib/libs/double-conversion
    library/cpp/string_utils/quote
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

SRCS(
    ut_restore.cpp
)

YQL_LAST_ABI_VERSION()

END()
