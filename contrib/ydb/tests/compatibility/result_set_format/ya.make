PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(32)

TEST_SRCS(
    test_result_set_value.py
    test_result_set_arrow.py
)

SIZE(LARGE)
IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/compatibility
    contrib/ydb/tests/datashard/lib
    contrib/python/pyarrow
)

END()
