PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(300)

TEST_SRCS(
    alter_compression.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/olap/scenario/helpers
    contrib/ydb/tests/olap/common
    contrib/ydb/tests/olap/column_compression/common
)

DEPENDS(
)

END()
