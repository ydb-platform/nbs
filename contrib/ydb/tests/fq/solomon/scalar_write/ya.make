PY3TEST()

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_scalar_solomon_write.py
)

PY_SRCS(
    conftest.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2 ram:16)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/library/yql/tools/solomon_emulator/client
    contrib/ydb/tests/fq/streaming_common
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    contrib/ydb/tests/tools/datastreams_helpers
    contrib/ydb/tests/tools/fq_runner
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

END()
