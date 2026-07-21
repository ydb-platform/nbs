UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

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
)

SRCS(
    ut_replication.cpp
)

YQL_LAST_ABI_VERSION()

END()
