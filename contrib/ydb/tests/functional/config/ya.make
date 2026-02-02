PY3TEST()

TEST_SRCS(
    test_config_with_metadata.py
    test_generate_dynamic_config.py
    test_distconf.py
)

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    contrib/ydb/apps/ydb
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
