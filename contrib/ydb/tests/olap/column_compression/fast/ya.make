PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()

SIZE(MEDIUM)

TEST_SRCS(
    test_column_family_negative.py
    test_disabled.py
    test_incorrect.py
    test_mixed.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/olap/scenario/helpers
    contrib/ydb/tests/olap/common
)

DEPENDS(
)

END()
