PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

TEST_SRCS(test.py)

SIZE(MEDIUM)
DEPENDS(
)

PEERDIR(
    contrib/python/requests
    contrib/python/urllib3
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

END()
