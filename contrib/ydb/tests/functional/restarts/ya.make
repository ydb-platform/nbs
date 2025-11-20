PY3TEST()

TEST_SRCS(
    test_restarts.py
)

SPLIT_FACTOR(10)
IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:32
)

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
