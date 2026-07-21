PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_VECTOR_WORKLOAD_PATH="contrib/ydb/tests/stress/vector_workload/workload_vector")
ENV(YDB_FULLTEXT_WORKLOAD_PATH="contrib/ydb/tests/stress/fulltext_workload/workload_fulltext")

TEST_SRCS(
    test_compare.py
)

REQUIREMENTS(ram:32 cpu:4)
TAG(ya:external)

SIZE(LARGE)
TAG(ya:fat)
TIMEOUT(3600)

DEPENDS(
    contrib/ydb/tests/stress/vector_workload
    contrib/ydb/tests/stress/fulltext_workload
)

# Flame-graph scripts (stackcollapse-perf.pl, flamegraph.pl) used by the
# optional compare_flamegraph=1 mode; resolved via yatest.common.source_path.
DATA(
    arcadia/contrib/tools/flame-graph
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
)

END()
