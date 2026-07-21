UNITTEST_FOR(contrib/ydb/core/kqp/compile_service)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_compile_fallback_ut.cpp
    kqp_replay_log_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/compile_service
    contrib/ydb/core/kqp/common
    contrib/ydb/public/sdk/cpp/src/client/proto
    library/cpp/testing/unittest
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
