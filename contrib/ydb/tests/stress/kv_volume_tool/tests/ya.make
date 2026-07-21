PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_DSTOOL_BINARY="contrib/ydb/apps/dstool/ydb-dstool")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_KV_VOLUME_TOOL_PATH="contrib/ydb/tests/stress/kv_volume_tool/kv_volume_tool")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/dstool
    contrib/ydb/tests/stress/kv_volume_tool
)

PEERDIR(
    contrib/python/boto3
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
)

END()
