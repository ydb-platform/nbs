PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_example.py  # TODO: change file name to yours
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()


PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
)

END()
