PY3TEST()

TEST_SRCS(
    conftest.py
    test_ydb_backup.py
    test_ydb_common.py
    test_ydb_flame_graph.py
    test_ydb_impex.py
    test_ydb_interactive_ai.py
    test_ydb_interactive_sql.py
    test_ydb_profile.py
    test_ydb_recursive_remove.py
    test_ydb_scheme.py
    test_ydb_scripting.py
    test_ydb_sql.py
    test_ydb_table.py
    test_ydb_tools.py
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_CLI_EXPERIMENTAL_BINARY="contrib/ydb/apps/ydb/experimental/ydb/ydb")
ENV(YDB_CLI_WITH_ENABLED_AI_BINARY="contrib/ydb/tests/functional/ydb_cli/ai_interactive/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/apps/ydb/experimental/ydb
    contrib/ydb/tests/functional/ydb_cli/ai_interactive
)

PEERDIR(
    contrib/python/pexpect
    contrib/python/pyarrow
    contrib/python/PyYAML
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/canonical
    contrib/ydb/tests/oss/ydb_sdk_import
)

PY_SRCS(
    ydb_cli_helpers.py
    ydb_cli_interactive_helpers.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(30)

END()

RECURSE(
    ai_interactive
    benchmarks
)
