PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(STRESS_TEST_UTILITY="contrib/ydb/tests/stress/scheme_board/pile_promotion/pile_promotion_workload")

TEST_SRCS(
    pile_promotion_test.py
    test_scheme_board_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/scheme_board/pile_promotion
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/scheme_board/pile_promotion/workload
)


END()
