UNITTEST_FOR(contrib/ydb/core/kqp)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_sys_col_ut.cpp
    kqp_sys_view_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    library/cpp/json
)

YQL_LAST_ABI_VERSION()

END()
