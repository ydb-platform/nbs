PY3TEST()

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

TEST_SRCS(
    test_simple_table.py
    test_tpch_import.py
    test_types_and_formats.py
)

FORK_SUBTESTS()
SPLIT_FACTOR(100)

PY_SRCS(
    base.py
)

SIZE(LARGE)

REQUIREMENTS(cpu:2)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
TIMEOUT(900)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/boto3
    contrib/python/pyarrow
    library/recipes/common
    contrib/ydb/tests/olap/common
)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/python/moto/bin
)

END()

RECURSE(
    large
)
