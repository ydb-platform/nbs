PY3_PROGRAM(upload_udf)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/functional/udf_store/lib
    contrib/ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    contrib/ydb/tests/stress/kv_volume_tool
)

END()
