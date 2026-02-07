IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_kv_workload.py
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydbd
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
)


END()

ENDIF()
