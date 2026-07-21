UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(150)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    bool_ut.cpp
    datatime64_ut.cpp
    decimal_ut.cpp
    dictionary_ut.cpp
    dynumber_ut.cpp
    interval_ut.cpp
    json_ut.cpp
    uuid_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/kqp/ut/olap/helpers
    contrib/ydb/core/kqp/ut/olap/combinatory
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/library/yql/types/dynumber
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(bool_test_enums.h)

END()
