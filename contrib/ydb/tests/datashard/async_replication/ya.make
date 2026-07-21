PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(20)
SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

TEST_SRCS(
    test_async_replication.py
)

PEERDIR(
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/sql/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
