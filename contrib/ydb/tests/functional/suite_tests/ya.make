IF (NOT SANITIZER_TYPE)
    PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
    ENV(YDB_ENABLE_COLUMN_TABLES="true")
    ENV(USE_IN_MEMORY_PDISKS=true)
    TEST_SRCS(
        test_base.py
        test_postgres.py
        test_sql_logic.py
        test_stream_query.py
    )

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)

    DEPENDS(
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

    END()
ENDIF()
