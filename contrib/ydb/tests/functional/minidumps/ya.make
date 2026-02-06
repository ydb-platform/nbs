IF (OS_LINUX AND NOT SANITIZER_TYPE)

PY3TEST()

TEST_SRCS(
    test_break.py
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

PEERDIR(
    contrib/ydb/tests/library
)

DEPENDS(
)


END()

ENDIF()
