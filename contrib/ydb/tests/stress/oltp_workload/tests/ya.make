PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/stress/oltp_workload/workload
    contrib/ydb/tests/stress/common
)


END()
