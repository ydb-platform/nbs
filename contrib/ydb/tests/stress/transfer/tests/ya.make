IF (NOT WITH_VALGRIND)
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="contrib/ydb/tests/stress/transfer/transfer")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/transfer
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/stress/transfer/workload
)


END()

ENDIF()
