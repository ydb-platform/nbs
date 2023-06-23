PY2TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test.py
)

TIMEOUT(360)

DEPENDS(
    cloud/blockstore/tools/testing/eternal-tests/eternal-load/bin
)

PEERDIR(
    contrib/deprecated/python/futures
)

END()
