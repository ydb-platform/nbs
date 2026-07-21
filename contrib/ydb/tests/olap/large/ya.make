PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
    ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        test_log_scenario.py
    )
    FORK_SUBTESTS()
    SPLIT_FACTOR(100)

    SIZE(LARGE)

    REQUIREMENTS(cpu:2)

    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    DEPENDS(
        contrib/ydb/apps/ydb
        )

    PEERDIR(
        contrib/ydb/tests/library
        contrib/ydb/tests/library/test_meta
        contrib/ydb/tests/olap/common
        contrib/ydb/tests/olap/lib
    )
END()
