PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
    ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        order_by_with_limit.py
        test_quota_exhaustion.py
        tablets_movement.py
        test_cs_many_updates.py
        data_read_correctness.py
        zip_bomb.py
    )

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        TAG(ya:fat)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    DEPENDS(
        contrib/ydb/apps/ydb
        )

    PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    )
END()

RECURSE(
    lib
    scenario
    docs
    load
    ttl_tiering
)
