UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_index_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/public/lib/idx_test
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
