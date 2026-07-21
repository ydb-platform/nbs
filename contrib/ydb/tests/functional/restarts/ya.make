PY3TEST()

TEST_SRCS(
    test_restarts.py
)

SPLIT_FACTOR(10)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
    IF (SANITIZER_TYPE == "thread")
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    ENDIF()
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
