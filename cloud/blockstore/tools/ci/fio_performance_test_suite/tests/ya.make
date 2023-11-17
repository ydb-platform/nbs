PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/ci/fio_performance_test_suite
)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

END()
