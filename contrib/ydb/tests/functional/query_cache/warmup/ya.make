PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_warmup.py
)

SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

END()
