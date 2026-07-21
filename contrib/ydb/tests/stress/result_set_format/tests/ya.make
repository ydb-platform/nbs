PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/result_set_format/result_set_format")

TEST_SRCS(
    test_arrow_workload.py
    test_value_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/stress/result_set_format
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/result_set_format/workload
    contrib/ydb/tests/stress/common
)

END()
