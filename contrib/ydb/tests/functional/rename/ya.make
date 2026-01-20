PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
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
    REQUIREMENTS(ram:32 cpu:2)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
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
