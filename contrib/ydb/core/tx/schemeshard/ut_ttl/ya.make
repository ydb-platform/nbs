UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

SRCS(
    ut_ttl.cpp
    ut_ttl_utility.cpp
)

YQL_LAST_ABI_VERSION()

END()
