PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_TEST_PATH="contrib/ydb/tests/stress/viewer/viewer")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:2)
ENDIF()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

DEPENDS(
    contrib/ydb/tests/stress/viewer
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/viewer/workload
)

END()
