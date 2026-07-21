PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

FORK_TEST_FILES()

TEST_SRCS(
    backpressure_test.py
    base.py
    basic_reading.py
    data_paging.py
    listing_batching.py
    listing_paging.py
    settings_validation.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/library/yql/tools/solomon_emulator/client
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
)

END()
