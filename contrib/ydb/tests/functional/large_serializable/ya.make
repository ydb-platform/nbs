IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_serializable.py
)

REQUIREMENTS(
    ram:32
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/tools/ydb_serializable
)

PEERDIR(
    contrib/ydb/tests/library
)


END()

ENDIF()
