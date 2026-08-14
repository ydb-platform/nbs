PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    test_query_cache.py
)

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

END()
