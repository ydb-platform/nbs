PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(39)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_vector_index.py
    test_vector_index_negative.py
)

PEERDIR(
    contrib/ydb/tests/datashard/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
