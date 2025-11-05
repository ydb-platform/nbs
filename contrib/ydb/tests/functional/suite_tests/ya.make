IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
    PY3TEST()
    ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
    ENV(USE_IN_MEMORY_PDISKS=true)
    TEST_SRCS(
        test_base.py
        test_postgres.py
        test_sql_logic.py
        test_stream_query.py
    )

    TIMEOUT(600)
    SIZE(MEDIUM)

    DEPENDS(
        contrib/ydb/apps/ydbd
    )

    DATA (
        arcadia/contrib/ydb/tests/functional/suite_tests/postgres
        arcadia/contrib/ydb/tests/functional/suite_tests/sqllogictest
    )

    PEERDIR(
        contrib/ydb/tests/library
        contrib/ydb/tests/oss/canonical
        contrib/ydb/tests/oss/ydb_sdk_import
    )

    FORK_SUBTESTS()
    FORK_TEST_FILES()

    REQUIREMENTS(ram:12)

    END()
ENDIF()
