PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_liveness_wardens.py
)

DEPENDS(
)

PEERDIR(
    contrib/ydb/public/sdk/python
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
)

SIZE(MEDIUM)

END()
