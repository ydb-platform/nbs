PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
PY_SRCS (
    conftest.py
    common.py
)

TEST_SRCS(
    test_rename.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
    contrib/python/tornado/tornado-4
)

END()
