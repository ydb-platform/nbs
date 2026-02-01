IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_serializable.py
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/tools/ydb_serializable
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
)


END()

ENDIF()
