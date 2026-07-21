PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

ENV(YDB_KV_VOLUME_TOOL_PATH="contrib/ydb/tests/stress/kv_volume_tool/kv_volume_tool")
ENV(YDB_DICTS_UDF_PATH="contrib/ydb/library/yql/udfs/examples/dicts/libdicts_udf.so")
ENV(YDB_UPLOAD_UDF_PATH="contrib/ydb/tests/functional/udf_store/upload_udf/upload_udf")

TEST_SRCS(
    test_udf_store.py
)

SPLIT_FACTOR(10)

DEPENDS(
    contrib/ydb/tests/stress/kv_volume_tool
    contrib/ydb/library/yql/udfs/examples/dicts
    contrib/ydb/tests/functional/udf_store/upload_udf
)

PEERDIR(
    contrib/ydb/tests/functional/udf_store/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
