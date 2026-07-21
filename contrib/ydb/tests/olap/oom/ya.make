PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()

TEST_SRCS(
    overlapping_portions.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    contrib/ydb/tests/olap/common
)

DEPENDS(
)

END()

