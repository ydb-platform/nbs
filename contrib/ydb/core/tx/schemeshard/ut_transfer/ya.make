UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_transfer.cpp
)

YQL_LAST_ABI_VERSION()

END()
